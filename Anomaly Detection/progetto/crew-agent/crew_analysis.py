"""
crew_analysis.py — CrewAI con tool MCP-backed, guardrail e ribilanciamento produzione

Architettura a 3 agenti:
  1. Analyst (telemetry read-only): diagnostica il guasto
  2. Production Optimizer (telemetry read-only): calcola il piano di ribilanciamento
     per mantenere la produzione totale invariata distribuendo il carico sulle macchine sane
  3. Specialist (remediation bounded): esegue le azioni — rallenta la macchina guasta,
     accelera le macchine sane, apre ticket

Modello produttivo:
  - 4 macchine parallele (producono la stessa cosa, intercambiabili)
  - Ogni macchina gira normalmente al 75% della capacita massima (RPM_NOMINAL)
  - RPM_MAX e il 100%: le macchine sane possono salire fino a RPM_MAX per compensare
  - RPM_MIN_ALLOWED = 300 (limite sicurezza, sotto serve approvazione umana)
  - Se la produzione non puo essere mantenuta al 100%, l'agente scala all'operatore

Policy di sicurezza (SRS §5.1):
  - Azioni vietate bloccate da _policy_check()
  - RPM bounds: RPM_MIN_ALLOWED <= rpm <= max_rpm per macchina
  - Ogni tool call e auditata con timestamp
"""

import json
import logging
import os
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from typing import Any, Type

import numpy as np
from crewai import Agent, Task, Crew, Process
from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from pymongo import MongoClient

log = logging.getLogger("crew-analysis")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongos1:27017,mongos2:27017")
MONGO_DB  = os.getenv("MONGO_DB", "factory")

# Guardrail economici (SRS §4.3)
CREW_TIMEOUT_SECONDS = int(os.getenv("CREW_TIMEOUT_SECONDS", "120"))
AGENT_MAX_ITER       = int(os.getenv("AGENT_MAX_ITER", "5"))

# Policy di sicurezza (SRS §5.1)
RPM_MIN_ALLOWED  = 300
FORBIDDEN_ACTIONS = {
    "shutdown", "emergency_stop", "power_off", "kill", "halt",
    "disable_safety", "override_limit", "bypass_policy",
}

# Modello produttivo — ogni macchina gira al 75% del max (headroom del 25%)
MACHINE_PROFILES = {
    "machine_a": {"nominal_rpm": 1800, "max_rpm": 3000},
    "machine_b": {"nominal_rpm": 1750, "max_rpm": 3000},
    "machine_c": {"nominal_rpm": 1800, "max_rpm": 3000},
    "machine_d": {"nominal_rpm": 1850, "max_rpm": 3000},
}
TOTAL_NOMINAL_PRODUCTION = sum(p["nominal_rpm"] for p in MACHINE_PROFILES.values())


# ── MongoDB helper ─────────────────────────────────────────────────────────────

_mongo_client = None

def _get_db():
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return _mongo_client[MONGO_DB]


# ── Audit log ──────────────────────────────────────────────────────────────────

def _audit(server, tool, args, outcome):
    log.info("AUDIT %s", json.dumps({
        "ts":      datetime.now(timezone.utc).isoformat(),
        "server":  server, "tool": tool, "args": args, "outcome": outcome,
    }))


# ── Policy check ───────────────────────────────────────────────────────────────

def _policy_check(text):
    for forbidden in FORBIDDEN_ACTIONS:
        if forbidden in text.lower():
            return (
                f"POLICY VIOLATION: '{forbidden}' non consentito via MCP. "
                f"Usa request_human_escalation."
            )
    return None


# ══════════════════════════════════════════════════════════════════════════════
# TELEMETRY TOOLS (read-only — server: telemetry-mcp)
# ══════════════════════════════════════════════════════════════════════════════

class GetRecentReadingsInput(BaseModel):
    machine_id: str = Field(description="ID macchina (es. machine_a)")
    limit: int = Field(default=20, description="Numero campioni (max 200)")

class GetRecentReadingsTool(BaseTool):
    name: str = "get_recent_readings"
    description: str = (
        "Recupera le ultime N letture sensore per una macchina dal database storico. "
        "Usa per capire il trend recente prima di formulare una diagnosi."
    )
    args_schema: Type[BaseModel] = GetRecentReadingsInput

    def _run(self, machine_id: str, limit: int = 20) -> str:
        limit = min(limit, 200)
        docs = list(_get_db().sensor_readings.find(
            {"machine_id": machine_id}, {"_id": 0}
        ).sort("timestamp", -1).limit(limit))
        _audit("telemetry-mcp", "get_recent_readings",
               {"machine_id": machine_id, "limit": limit}, f"{len(docs)} docs")
        return json.dumps(docs, default=str)


class GetActiveAlertsInput(BaseModel):
    machine_id: str = Field(default="", description="ID macchina (opzionale, vuoto = tutta la flotta)")

class GetActiveAlertsTool(BaseTool):
    name: str = "get_active_alerts"
    description: str = (
        "Recupera gli alert attivi nelle ultime 2 ore. "
        "Se machine_id e vuoto, restituisce alert di tutta la flotta."
    )
    args_schema: Type[BaseModel] = GetActiveAlertsInput

    def _run(self, machine_id: str = "") -> str:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        query = {"timestamp": {"$gte": cutoff}}
        if machine_id:
            query["machine_id"] = machine_id
        docs = list(_get_db().alerts.find(query, {"_id": 0}).sort("timestamp", -1).limit(50))
        _audit("telemetry-mcp", "get_active_alerts", {"machine_id": machine_id}, f"{len(docs)} docs")
        return json.dumps(docs, default=str)


class GetMachineBaselineInput(BaseModel):
    machine_id: str = Field(description="ID macchina")

class GetMachineBaselineTool(BaseTool):
    name: str = "get_machine_baseline"
    description: str = (
        "Calcola baseline statistica (media, stddev, min, max) degli ultimi 500 campioni. "
        "Usa per stabilire se il dato anomalo e davvero fuori norma."
    )
    args_schema: Type[BaseModel] = GetMachineBaselineInput

    def _run(self, machine_id: str) -> str:
        docs = list(_get_db().sensor_readings.find(
            {"machine_id": machine_id},
            {"_id": 0, "vibration_x": 1, "vibration_y": 1, "temperature": 1, "rpm": 1}
        ).sort("timestamp", -1).limit(500))
        if not docs:
            return json.dumps({"error": f"Nessun dato per {machine_id}"})
        result = {"machine_id": machine_id, "sample_count": len(docs)}
        for field in ["vibration_x", "vibration_y", "temperature", "rpm"]:
            vals = [d[field] for d in docs if field in d]
            if vals:
                result[field] = {
                    "mean": round(float(np.mean(vals)), 4),
                    "std":  round(float(np.std(vals)), 4),
                    "min":  round(float(np.min(vals)), 4),
                    "max":  round(float(np.max(vals)), 4),
                }
        _audit("telemetry-mcp", "get_machine_baseline", {"machine_id": machine_id}, "ok")
        return json.dumps(result, default=str)


class _EmptyInput(BaseModel):
    pass

class GetFleetProductionStatusTool(BaseTool):
    """
    Tool chiave per il ribilanciamento: stato produttivo completo della flotta.
    Calcola RPM attuali, headroom disponibile per ogni macchina, e gap vs target.
    """
    name: str = "get_fleet_production_status"
    description: str = (
        "Restituisce lo stato produttivo completo della flotta: RPM attuale, nominale "
        "e massimo per ogni macchina, headroom disponibile (RPM extra utilizzabili), "
        "e produzione totale attuale vs target. "
        "USA QUESTO TOOL come primo passo per calcolare il piano di ribilanciamento."
    )
    args_schema: Type[BaseModel] = _EmptyInput

    def _run(self) -> str:
        db = _get_db()

        # Macchine con fault attivo nelle ultime 2 ore
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        active_faults = set(
            doc["machine_id"] for doc in
            db.alerts.find(
                {"timestamp": {"$gte": cutoff}, "severity": "fault"},
                {"machine_id": 1}
            )
        )

        # Comandi RPM pendenti (azioni gia emesse dall'agente in precedenza)
        pending_cmds = {}
        for cmd in db.machine_commands.find({"status": "pending"}):
            pending_cmds[cmd["machine_id"]] = cmd.get("rpm_limit") or cmd.get("target_rpm")

        fleet = []
        total_current = 0.0

        for machine_id, profile in MACHINE_PROFILES.items():
            # RPM corrente: usa comando pendente se esiste, altrimenti ultimo campione DB
            if machine_id in pending_cmds and pending_cmds[machine_id]:
                current_rpm = float(pending_cmds[machine_id])
            else:
                last = db.sensor_readings.find_one(
                    {"machine_id": machine_id}, {"rpm": 1}, sort=[("timestamp", -1)]
                )
                current_rpm = float(last["rpm"]) if last else float(profile["nominal_rpm"])

            nominal  = profile["nominal_rpm"]
            max_rpm  = profile["max_rpm"]
            headroom = max(0.0, max_rpm - current_rpm)

            fleet.append({
                "machine_id":      machine_id,
                "current_rpm":     round(current_rpm, 1),
                "nominal_rpm":     nominal,
                "max_rpm":         max_rpm,
                "headroom_rpm":    round(headroom, 1),
                "utilization_pct": round(current_rpm / max_rpm * 100, 1),
                "is_faulted":      machine_id in active_faults,
            })
            total_current += current_rpm

        result = {
            "fleet":                     fleet,
            "total_current_rpm":         round(total_current, 1),
            "total_nominal_rpm":         TOTAL_NOMINAL_PRODUCTION,
            "production_gap_rpm":        round(TOTAL_NOMINAL_PRODUCTION - total_current, 1),
            "production_pct_of_target":  round(total_current / TOTAL_NOMINAL_PRODUCTION * 100, 1),
            "machine_profiles_reference": MACHINE_PROFILES,
        }
        _audit("telemetry-mcp", "get_fleet_production_status", {}, "ok")
        log.info(
            f"Fleet status: {result['production_pct_of_target']}% of target "
            f"({result['total_current_rpm']}/{result['total_nominal_rpm']} RPM)"
        )
        return json.dumps(result, default=str)


# ══════════════════════════════════════════════════════════════════════════════
# REMEDIATION TOOLS (bounded actions — server: remediation-mcp)
# ══════════════════════════════════════════════════════════════════════════════

class OpenTicketInput(BaseModel):
    machine_id:  str = Field(description="ID macchina")
    urgency:     str = Field(description="Urgenza: low | medium | high | critical")
    description: str = Field(description="Descrizione tecnica del problema e azioni intraprese")

class OpenMaintenanceTicketTool(BaseTool):
    name: str = "open_maintenance_ticket"
    description: str = (
        "Apre un ticket di manutenzione con descrizione del guasto e delle azioni di "
        "ribilanciamento intraprese. NON usare per richiedere shutdown."
    )
    args_schema: Type[BaseModel] = OpenTicketInput

    def _run(self, machine_id: str, urgency: str, description: str) -> str:
        violation = _policy_check(description)
        if violation:
            _audit("remediation-mcp", "open_maintenance_ticket",
                   {"machine_id": machine_id}, f"REJECTED: policy")
            return json.dumps({"status": "rejected", "reason": violation})
        if urgency not in {"low", "medium", "high", "critical"}:
            return json.dumps({"status": "rejected",
                               "reason": f"urgency '{urgency}' non valida"})
        ticket = {
            "ticket_id":   str(uuid.uuid4()),
            "machine_id":  machine_id,
            "urgency":     urgency,
            "description": description,
            "created_at":  datetime.now(timezone.utc).isoformat(),
            "status":      "open",
            "created_by":  "remediation-mcp/agent",
        }
        _get_db().maintenance_tickets.insert_one(dict(ticket))
        _audit("remediation-mcp", "open_maintenance_ticket",
               {"machine_id": machine_id, "urgency": urgency}, "ok")
        log.warning(f"TICKET [{urgency.upper()}] {machine_id}: {description[:80]}")
        return json.dumps({"status": "ok", "ticket_id": ticket["ticket_id"],
                           "urgency": urgency})


class SpeedLimitInput(BaseModel):
    machine_id: str   = Field(description="ID macchina da rallentare (quella guasta)")
    rpm_limit:  float = Field(description=f"RPM target ridotto (min {RPM_MIN_ALLOWED})")
    reason:     str   = Field(description="Motivazione: es. 'limite ridotto per [motivo] (-300 RPM)'")

class SetMachineSpeedLimitTool(BaseTool):
    name: str = "set_machine_speed_limit"
    description: str = (
        f"Rallenta una macchina guasta impostando un limite RPM inferiore al nominale. "
        f"Minimo consentito: {RPM_MIN_ALLOWED} RPM (sotto serve escalation umana). "
        f"Dopo aver usato questo tool, usa set_machine_production_target sulle macchine "
        f"sane per compensare la perdita di produzione."
    )
    args_schema: Type[BaseModel] = SpeedLimitInput

    def _run(self, machine_id: str, rpm_limit: float, reason: str) -> str:
        if rpm_limit < RPM_MIN_ALLOWED:
            _audit("remediation-mcp", "set_machine_speed_limit",
                   {"machine_id": machine_id, "rpm_limit": rpm_limit}, "REJECTED: below min")
            return json.dumps({"status": "rejected",
                               "reason": f"POLICY VIOLATION: rpm_limit={rpm_limit} < "
                                         f"{RPM_MIN_ALLOWED}. Richiede autorizzazione umana."})
        profile = MACHINE_PROFILES.get(machine_id, {})
        max_rpm = profile.get("max_rpm", 9999)
        nominal_rpm = profile["nominal_rpm"]
        if rpm_limit > max_rpm:
            return json.dumps({"status": "rejected",
                               "reason": f"rpm_limit={rpm_limit} supera il massimo "
                                         f"consentito per {machine_id} ({max_rpm} RPM)."})
        cmd = {
            "command_id": str(uuid.uuid4()),
            "machine_id": machine_id,
            "type":       "speed_limit",
            "rpm_limit":  rpm_limit,
            "nominal_rpm": nominal_rpm,
            "max_rpm":     max_rpm,
            "reason":      reason,
            "issued_at":  datetime.now(timezone.utc).isoformat(),
            "issued_by":  "remediation-mcp/agent",
            "status":     "pending",
        }
        _get_db().machine_commands.insert_one(dict(cmd))
        _audit("remediation-mcp", "set_machine_speed_limit",
               {"machine_id": machine_id, "rpm_limit": rpm_limit}, "ok")
        nominal = profile.get("nominal_rpm", "?")
        log.warning(f"SPEED LIMIT: {machine_id} → {rpm_limit} RPM "
                    f"(nominal: {nominal}, max: {max_rpm})")
        return json.dumps({"status": "ok", "command_id": cmd["command_id"],
                           "rpm_limit": rpm_limit, "machine_max_rpm": max_rpm,
                           "machine_nominal_rpm": nominal})


class ProductionTargetInput(BaseModel):
    machine_id:  str   = Field(description="ID macchina SANA da accelerare")
    target_rpm:  float = Field(description="RPM target aumentato (non puo superare max della macchina)")
    reason:      str   = Field(description="Motivazione: es. 'compensazione perdita machine_a (+300 RPM)'")

class SetMachineProductionTargetTool(BaseTool):
    name: str = "set_machine_production_target"
    description: str = (
        "Aumenta il target RPM di una macchina SANA per compensare la perdita di produzione "
        "di una macchina guasta. Il target non puo superare il RPM massimo della macchina. "
        "Usa get_fleet_production_status prima per sapere quanto headroom ha ogni macchina."
    )
    args_schema: Type[BaseModel] = ProductionTargetInput

    def _run(self, machine_id: str, target_rpm: float, reason: str) -> str:
        profile = MACHINE_PROFILES.get(machine_id)
        if not profile:
            return json.dumps({"status": "rejected",
                               "reason": f"Macchina {machine_id} non trovata nei profili."})
        max_rpm     = profile["max_rpm"]
        nominal_rpm = profile["nominal_rpm"]

        # Cap automatico al massimo — non rifiuta, aggiusta e avvisa
        capped = False
        if target_rpm > max_rpm:
            target_rpm = max_rpm
            capped = True

        if target_rpm < RPM_MIN_ALLOWED:
            return json.dumps({"status": "rejected",
                               "reason": f"target_rpm={target_rpm} e inferiore al nominale "
                                         f"({nominal_rpm}). Usa set_machine_speed_limit "
                                         f"per rallentare, non questo tool."})
        cmd = {
            "command_id":  str(uuid.uuid4()),
            "machine_id":  machine_id,
            "type":        "production_target",
            "target_rpm":  target_rpm,
            "nominal_rpm": nominal_rpm,
            "max_rpm":     max_rpm,
            "reason":      reason,
            "issued_at":   datetime.now(timezone.utc).isoformat(),
            "issued_by":   "remediation-mcp/agent",
            "status":      "pending",
        }
        _get_db().machine_commands.insert_one(dict(cmd))
        _audit("remediation-mcp", "set_machine_production_target",
               {"machine_id": machine_id, "target_rpm": target_rpm}, "ok")
        increase_pct = round((target_rpm - nominal_rpm) / nominal_rpm * 100, 1)
        log.warning(f"PRODUCTION TARGET: {machine_id} → {target_rpm} RPM "
                    f"(+{increase_pct}% vs nominal {nominal_rpm}"
                    f"{', CAPPED AL MAX' if capped else ''})")
        return json.dumps({
            "status":        "ok",
            "command_id":    cmd["command_id"],
            "machine_id":    machine_id,
            "target_rpm":    target_rpm,
            "nominal_rpm":   nominal_rpm,
            "max_rpm":       max_rpm,
            "increase_pct":  increase_pct,
            "capped_to_max": capped,
            "reason":        reason,
        })


class EscalationInput(BaseModel):
    machine_id: str = Field(description="ID macchina principale coinvolta")
    reason:     str = Field(description="Motivazione tecnica dettagliata per l'escalation")

class RequestHumanEscalationTool(BaseTool):
    name: str = "request_human_escalation"
    description: str = (
        "Richiede intervento operatore umano. Usare quando: "
        "(1) serve uno shutdown, "
        "(2) il deficit di produzione supera il 20% del target e non recuperabile, "
        "(3) diagnosi incerta su guasto critico, "
        "(4) l'azione supera i limiti consentiti agli agenti automatici."
    )
    args_schema: Type[BaseModel] = EscalationInput

    def _run(self, machine_id: str, reason: str) -> str:
        esc = {
            "escalation_id": str(uuid.uuid4()),
            "machine_id":    machine_id,
            "reason":        reason,
            "created_at":    datetime.now(timezone.utc).isoformat(),
            "status":        "pending_human_review",
            "created_by":    "remediation-mcp/agent",
        }
        _get_db().escalations.insert_one(dict(esc))
        _audit("remediation-mcp", "request_human_escalation",
               {"machine_id": machine_id}, "ok")
        log.critical(f"ESCALATION UMANA: {machine_id} — {reason[:120]}")
        return json.dumps({"status": "ok", "escalation_id": esc["escalation_id"]})


class AcknowledgeAlertInput(BaseModel):
    alert_id:      str = Field(description="ID dell'alert")
    operator_note: str = Field(description="Nota operatore")

class AcknowledgeAlertTool(BaseTool):
    name: str = "acknowledge_alert"
    description: str = "Segna un alert come riconosciuto con una nota operatore."
    args_schema: Type[BaseModel] = AcknowledgeAlertInput

    def _run(self, alert_id: str, operator_note: str) -> str:
        result = _get_db().alerts.update_one(
            {"alert_id": alert_id},
            {"$set": {"acknowledged": True,
                      "acknowledged_at": datetime.now(timezone.utc).isoformat(),
                      "operator_note":   operator_note}}
        )
        _audit("remediation-mcp", "acknowledge_alert", {"alert_id": alert_id}, "ok")
        return json.dumps({"status": "ok" if result.matched_count else "not_found",
                           "alert_id": alert_id})


# ── Tool instances ─────────────────────────────────────────────────────────────

# Analyst: solo lettura diagnostica
ANALYST_TOOLS = [
    GetRecentReadingsTool(),
    GetActiveAlertsTool(),
    GetMachineBaselineTool(),
]

# Optimizer: lettura + stato flotta (nessuna scrittura)
OPTIMIZER_TOOLS = [
    GetFleetProductionStatusTool(),
    GetActiveAlertsTool(),
]

# Specialist: tutte le azioni di remediation
SPECIALIST_TOOLS = [
    OpenMaintenanceTicketTool(),
    SetMachineSpeedLimitTool(),
    SetMachineProductionTargetTool(),
    RequestHumanEscalationTool(),
    AcknowledgeAlertTool(),
]


# ── Timeout guardrail ──────────────────────────────────────────────────────────

class CrewTimeoutError(Exception):
    pass

@contextmanager
def crew_timeout(seconds):
    """Timeout compatibile con thread secondari (usa threading.Timer + ctypes)."""
    current_thread = threading.current_thread()

    def _fire():
        import ctypes
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(current_thread.ident),
            ctypes.py_object(CrewTimeoutError)
        )

    timer = threading.Timer(seconds, _fire)
    timer.daemon = True
    timer.start()
    try:
        yield
    finally:
        timer.cancel()


# ── Crew factory ───────────────────────────────────────────────────────────────

def get_anomaly_crew(machine_id: str, sensor_data: Any,
                     history_context: str, llm_name: str) -> Crew:
    """
    Crew a 3 agenti sequenziali:
      1. Analyst        — diagnostica (telemetry read-only)
      2. Prod Optimizer — piano ribilanciamento (fleet status read-only)
      3. Specialist     — esegue le azioni (remediation bounded)
    """

    # ── Agente 1: Analista ────────────────────────────────────────────────────
    analyst = Agent(
        role="Senior Data Reliability Engineer",
        goal=(
            f"Analizzare i segnali di {machine_id} e determinare con precisione "
            f"se l'anomalia e un guasto reale, un warning o un falso positivo. "
            f"Per i guasti reali, stimare anche la riduzione RPM sicura."
        ),
        backstory=(
            "Sei un esperto di analisi spettrale e termografia industriale. "
            "Confronti sempre i dati attuali con la baseline storica prima di concludere. "
            "La tua diagnosi deve includere: classificazione (GUASTO_REALE | WARNING | "
            "FALSO_POSITIVO), tipo di guasto, confidence level, se e progressivo o "
            "improvviso, e per i guasti reali la riduzione RPM consigliata "
            "(es. 'ridurre a 900 RPM per limitare l'attrito sul cuscinetto')."
            "Tuttavia, se l'anomalia e esclusivamente statistica (ML) e i sensori fisici sono vicini alla baseline, preferisci una classificazione di WARNING con riduzioni moderate invece di diagnosi drastiche."
        ),
        llm=llm_name,
        tools=ANALYST_TOOLS,
        allow_delegation=False,
        verbose=True,
        max_iter=AGENT_MAX_ITER,
    )

    # ── Agente 2: Production Optimizer ────────────────────────────────────────
    optimizer = Agent(
        role="Production Continuity Manager",
        goal=(
            "Garantire che la produzione totale rimanga al 100% del target "
            "distribuendo il carico sulle macchine sane entro i loro limiti operativi. "
            "Calcolare RPM esatti per ogni macchina, non percentuali generiche."
        ),
        backstory=(
            "Sei un esperto di ottimizzazione della produzione industriale. "
            "La fabbrica ha 4 macchine parallele che producono la stessa cosa. "
            "Ogni macchina normalmente gira al 75% della sua capacita massima, "
            "quindi ha un margine del 25% per compensare i guasti delle altre. "
            f"Produzione totale nominale: {TOTAL_NOMINAL_PRODUCTION} RPM. "
            "Il tuo algoritmo: "
            "1. Chiama get_fleet_production_status per vedere RPM attuali e headroom. "
            "2. Calcola il delta da recuperare = RPM nominale macchina guasta - RPM ridotto. "
            "3. Distribuisci il delta sulle macchine sane proporzionalmente al loro headroom. "
            "4. Verifica che nessuna macchina superi il suo max_rpm. "
            "5. Se il delta e maggiore della somma degli headroom disponibili, "
            "   calcola il massimo raggiungibile e documenta il deficit residuo. "
            "6. Se deficit > 20% del target: raccomanda escalation umana."
        ),
        llm=llm_name,
        tools=OPTIMIZER_TOOLS,
        allow_delegation=False,
        verbose=True,
        max_iter=AGENT_MAX_ITER,
    )

    # ── Agente 3: Specialista ─────────────────────────────────────────────────
    specialist = Agent(
        role="Predictive Maintenance Expert",
        goal=(
            "Eseguire esattamente il piano dell'analista e dell'optimizer: "
            "rallentare la macchina guasta, accelerare le macchine sane, "
            "aprire i ticket appropriati."
        ),
        backstory=(
            "Hai 20 anni di esperienza su cuscinetti CWRU e motori industriali. "
            "Esegui il piano fornito rispettando sempre i limiti di policy:\n"
            "- set_machine_speed_limit: rallenta la macchina guasta\n"
            "- set_machine_production_target: accelera le macchine sane (una chiamata per macchina)\n"
            "- open_maintenance_ticket: documenta il guasto E il piano di ribilanciamento\n"
            "- request_human_escalation: se serve shutdown o deficit > 20%\n"
            "Non puoi ordinare shutdown o emergency stop direttamente."
        ),
        llm=llm_name,
        tools=SPECIALIST_TOOLS,
        allow_delegation=False,
        verbose=True,
        max_iter=AGENT_MAX_ITER,
    )

    # ── Task 1: Diagnosi ──────────────────────────────────────────────────────
    task_analysis = Task(
        description=(
            f"Dati sensore attuali per {machine_id}:\n"
            f"{json.dumps(sensor_data, default=str, indent=2)}\n\n"
            f"Contesto storico (ultimi campioni da DB):\n{history_context}\n\n"
            f"Passi obbligatori:\n"
            f"1. Usa get_machine_baseline per confrontare con la baseline storica.\n"
            f"2. Usa get_recent_readings per vedere il trend degli ultimi 20 campioni.\n"
            f"3. Usa get_active_alerts per vedere alert correlati aperti.\n"
            f"4. Concludi con classificazione, tipo guasto, confidence level, "
            f"   progressivo/improvviso, e RPM di sicurezza consigliato."
            "(Nota: Per sole anomalie ML prediligi la categoria WARNING con riduzione RPM < 20%)."
        ),
        expected_output=(
            "Report diagnostico strutturato:\n"
            "- Classificazione: GUASTO_REALE | WARNING | FALSO_POSITIVO\n"
            "- Tipo guasto e confidence level (es. bearing_inner, 85%)\n"
            "- Progressivo o improvviso\n"
            "- RPM sicuro consigliato per la macchina guasta (numero esatto)\n"
            "- Motivazione tecnica sintetica"
        ),
        agent=analyst,
    )

    # ── Task 2: Piano Ribilanciamento ─────────────────────────────────────────
    task_rebalance = Task(
        description=(
            f"Basandoti sulla diagnosi dell'analista per {machine_id}:\n\n"
            f"Se GUASTO_REALE o WARNING:\n"
            f"1. Chiama get_fleet_production_status per vedere RPM e headroom di ogni macchina.\n"
            f"2. Calcola il delta produttivo = RPM nominale {machine_id} - RPM sicuro consigliato.\n"
            f"3. Distribuisci il delta sulle macchine SANE proporzionalmente al loro headroom_rpm.\n"
            f"4. Assicurati che nessuna macchina superi il suo max_rpm.\n"
            f"5. Calcola produzione totale risultante e % del target.\n"
            f"6. Se deficit residuo > 20%: raccomanda escalation umana con motivazione.\n\n"
            f"Se FALSO_POSITIVO: piano = nessuna azione di ribilanciamento necessaria.\n\n"
            f"Il piano deve contenere RPM ESATTI per ogni macchina (non percentuali)."
        ),
        expected_output=(
            "Piano di ribilanciamento:\n"
            f"- RPM target per {machine_id} (macchina guasta)\n"
            "- RPM target per ogni macchina sana (machine_a/b/c/d esclusa quella guasta)\n"
            "- Produzione totale risultante in RPM e % del target nominale\n"
            "- Deficit residuo (se presente) con motivazione\n"
            "- Raccomandazione escalation se deficit > 20%"
        ),
        agent=optimizer,
        context=[task_analysis],
    )

    # ── Task 3: Esecuzione ────────────────────────────────────────────────────
    task_action = Task(
        description=(
            "Esegui ESATTAMENTE il piano dell'analista e dell'optimizer.\n\n"
            "Per GUASTO_REALE confidence > 70%:\n"
            "  1. set_machine_speed_limit sulla macchina guasta (RPM dal piano)\n"
            "  2. set_machine_production_target su OGNI macchina sana (RPM dal piano, una chiamata per macchina)\n"
            "  3. open_maintenance_ticket CRITICAL — descrivi il guasto E il ribilanciamento attuato\n"
            "  4. Se deficit > 20%: request_human_escalation\n\n"
            "Per GUASTO_REALE confidence < 70% o WARNING:\n"
            "  1. set_machine_speed_limit leggero (riduzione 10-20% dal nominale)\n"
            "  2. set_machine_production_target sulle macchine sane\n"
            "  3. open_maintenance_ticket HIGH\n\n"
            "Per FALSO_POSITIVO:\n"
            "  1. open_maintenance_ticket LOW per tracciabilita\n"
            "  (nessun ribilanciamento)\n\n"
            "IMPORTANTE: non puoi ordinare shutdown o emergency stop."
        ),
        expected_output=(
            "Riepilogo azioni eseguite:\n"
            "- command_id per ogni set_machine_speed_limit\n"
            "- command_id per ogni set_machine_production_target (una per macchina sana)\n"
            "- ticket_id per il ticket di manutenzione\n"
            "- escalation_id (se chiamata)\n"
            "- Produzione totale risultante dopo le azioni"
        ),
        agent=specialist,
        context=[task_analysis, task_rebalance],
    )

    return Crew(
        agents=[analyst, optimizer, specialist],
        tasks=[task_analysis, task_rebalance, task_action],
        process=Process.sequential,
        verbose=True,
    )


# ── Entry point con guardrail ──────────────────────────────────────────────────

def run_crew_with_guardrails(machine_id: str, sensor_data: Any,
                              history_context: str, llm_name: str) -> dict:
    """
    Esegue la crew con timeout threading e gestione escalation automatica.
    """
    crew = get_anomaly_crew(machine_id, sensor_data, history_context, llm_name)
    try:
        with crew_timeout(CREW_TIMEOUT_SECONDS):
            result = crew.kickoff()
        return {"status": "ok", "result": str(result)}

    except CrewTimeoutError:
        log.error(f"CREW TIMEOUT per {machine_id} dopo {CREW_TIMEOUT_SECONDS}s")
        try:
            _get_db().escalations.insert_one({
                "escalation_id": str(uuid.uuid4()),
                "machine_id":    machine_id,
                "reason":        f"Crew AI timeout dopo {CREW_TIMEOUT_SECONDS}s — analisi non completata",
                "created_at":    datetime.now(timezone.utc).isoformat(),
                "status":        "pending_human_review",
                "created_by":    "crew-guardrail/timeout",
            })
        except Exception as me:
            log.error(f"Errore escalation MongoDB: {me}")
        return {"status": "timeout_escalated",
                "escalation": f"Timeout dopo {CREW_TIMEOUT_SECONDS}s"}

    except Exception as e:
        log.error(f"Errore crew per {machine_id}: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}
