"""
Remediation MCP Server
Espone tool di azione per gli agenti CrewAI — con policy di sicurezza integrata.

Tool disponibili:
  - open_maintenance_ticket(machine_id, urgency, description)   crea ticket su MongoDB
  - set_machine_speed_limit(machine_id, rpm_limit)              registra limite RPM (bounded)
  - request_human_escalation(machine_id, reason)                escalation operatore umano
  - acknowledge_alert(alert_id, operator_note)                  chiude un alert

POLICY DI SICUREZZA (SRS §5.1):
  - Lo shutdown immediato NON è eseguibile via MCP. Richiede operatore umano.
  - set_machine_speed_limit: rpm_limit non può essere < RPM_MIN_ALLOWED (300).
  - Ogni azione è loggata con timestamp, agente e parametri (auditability SRS §4.2).
  - Azioni non in whitelist vengono rifiutate con messaggio esplicito.
"""

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [remediation-mcp] %(levelname)s — %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("remediation-mcp")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongos:27017")
MONGO_DB  = os.getenv("MONGO_DB", "factory")

# ── Policy di sicurezza ───────────────────────────────────────────────────────

# RPM minimo consentito via MCP. Sotto questa soglia serve escalation umana.
RPM_MIN_ALLOWED = 300

# Urgenze permesse per i ticket
ALLOWED_URGENCIES = {"low", "medium", "high", "critical"}

# Azioni vietate agli agenti: se l'LLM le propone, vengono rifiutate
FORBIDDEN_ACTIONS = {
    "shutdown", "emergency_stop", "power_off", "kill", "halt",
    "disable_safety", "override_limit", "bypass_policy",
}

def policy_check_description(text: str) -> str | None:
    """Controlla se la descrizione tenta di aggirare la policy. Ritorna None se OK, str se violazione."""
    text_lower = text.lower()
    for forbidden in FORBIDDEN_ACTIONS:
        if forbidden in text_lower:
            return (
                f"POLICY VIOLATION: l'azione '{forbidden}' non è consentita via MCP. "
                f"Per uno shutdown è richiesta autorizzazione operatore umano. "
                f"Usa request_human_escalation per escalare la decisione."
            )
    return None


# ── Connessione MongoDB ───────────────────────────────────────────────────────

_mongo_client: MongoClient | None = None

def get_db():
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return _mongo_client[MONGO_DB]


# ── Audit Log ─────────────────────────────────────────────────────────────────

def audit(tool_name: str, args: dict, outcome: str):
    """Logga ogni azione di remediation per auditability"""
    entry = {
        "ts":      datetime.now(timezone.utc).isoformat(),
        "server":  "remediation-mcp",
        "tool":    tool_name,
        "args":    args,
        "outcome": outcome,
    }
    log.info("AUDIT %s", json.dumps(entry))


# ── Implementazione tool ──────────────────────────────────────────────────────

def open_maintenance_ticket(machine_id: str, urgency: str, description: str) -> dict:
    # Policy check sul testo libero
    violation = policy_check_description(description)
    if violation:
        audit("open_maintenance_ticket", {"machine_id": machine_id, "urgency": urgency}, f"REJECTED: {violation}")
        return {"status": "rejected", "reason": violation}

    if urgency not in ALLOWED_URGENCIES:
        reason = f"urgency '{urgency}' non valida. Valori ammessi: {ALLOWED_URGENCIES}"
        audit("open_maintenance_ticket", {"machine_id": machine_id}, f"REJECTED: {reason}")
        return {"status": "rejected", "reason": reason}

    ticket = {
        "ticket_id":   str(uuid.uuid4()),
        "machine_id":  machine_id,
        "urgency":     urgency,
        "description": description,
        "created_at":  datetime.now(timezone.utc).isoformat(),
        "status":      "open",
        "created_by":  "remediation-mcp/agent",
    }
    db = get_db()
    db.maintenance_tickets.insert_one(ticket)
    ticket.pop("_id", None)

    audit("open_maintenance_ticket", {"machine_id": machine_id, "urgency": urgency}, "OK")
    log.warning(f"TICKET APERTO [{urgency.upper()}] {machine_id}: {description[:80]}")
    return {"status": "ok", "ticket": ticket}


def set_machine_speed_limit(machine_id: str, rpm_limit: float) -> dict:
    """Registra un limite RPM per la macchina. Non può scendere sotto RPM_MIN_ALLOWED."""
    if rpm_limit < RPM_MIN_ALLOWED:
        reason = (
            f"POLICY VIOLATION: rpm_limit={rpm_limit} è inferiore al minimo consentito "
            f"({RPM_MIN_ALLOWED} RPM). Per ridurre ulteriormente la velocità o fermare "
            f"la macchina è richiesta autorizzazione operatore umano."
        )
        audit("set_machine_speed_limit", {"machine_id": machine_id, "rpm_limit": rpm_limit}, f"REJECTED: {reason}")
        return {"status": "rejected", "reason": reason}

    command = {
        "command_id":  str(uuid.uuid4()),
        "machine_id":  machine_id,
        "type":        "speed_limit",
        "rpm_limit":   rpm_limit,
        "issued_at":   datetime.now(timezone.utc).isoformat(),
        "issued_by":   "remediation-mcp/agent",
        "status":      "pending",   # un operatore o sistema esterno la eseguirà
    }
    db = get_db()
    db.machine_commands.insert_one(command)
    command.pop("_id", None)

    audit("set_machine_speed_limit", {"machine_id": machine_id, "rpm_limit": rpm_limit}, "OK")
    log.warning(f"COMANDO RPM REGISTRATO: {machine_id} → {rpm_limit} RPM")
    return {"status": "ok", "command": command}


def request_human_escalation(machine_id: str, reason: str) -> dict:
    """Escalation obbligatoria verso operatore umano. Usata quando l'agente non può agire."""
    escalation = {
        "escalation_id": str(uuid.uuid4()),
        "machine_id":    machine_id,
        "reason":        reason,
        "created_at":    datetime.now(timezone.utc).isoformat(),
        "status":        "pending_human_review",
        "created_by":    "remediation-mcp/agent",
    }
    db = get_db()
    db.escalations.insert_one(escalation)
    escalation.pop("_id", None)

    audit("request_human_escalation", {"machine_id": machine_id}, "OK")
    log.critical(f"ESCALATION UMANA RICHIESTA: {machine_id} — {reason[:100]}")
    return {"status": "ok", "escalation": escalation}


def acknowledge_alert(alert_id: str, operator_note: str) -> dict:
    """Marca un alert come riconosciuto con una nota."""
    db = get_db()
    result = db.alerts.update_one(
        {"alert_id": alert_id},
        {"$set": {
            "acknowledged": True,
            "acknowledged_at": datetime.now(timezone.utc).isoformat(),
            "operator_note": operator_note,
        }}
    )
    if result.matched_count == 0:
        return {"status": "not_found", "alert_id": alert_id}

    audit("acknowledge_alert", {"alert_id": alert_id}, "OK")
    return {"status": "ok", "alert_id": alert_id, "acknowledged": True}


# ── MCP Server ────────────────────────────────────────────────────────────────

app = Server("remediation-mcp")

TOOLS = [
    Tool(
        name="open_maintenance_ticket",
        description=(
            "Apre un ticket di manutenzione per una macchina. "
            "Usa questo tool quando l'analisi conferma un guasto che richiede intervento tecnico. "
            "NOTA: non usare questo tool per richiedere uno shutdown — usa request_human_escalation."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "machine_id":   {"type": "string"},
                "urgency":      {"type": "string", "enum": ["low", "medium", "high", "critical"]},
                "description":  {"type": "string", "description": "Descrizione tecnica del problema (max 500 chars)"},
            },
            "required": ["machine_id", "urgency", "description"],
        },
    ),
    Tool(
        name="set_machine_speed_limit",
        description=(
            "Registra un limite di velocità (RPM) per la macchina nel sistema di controllo. "
            "Il limite minimo consentito via questo tool è 300 RPM. "
            "Per valori inferiori o per lo stop completo, usa request_human_escalation."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "machine_id": {"type": "string"},
                "rpm_limit":  {"type": "number", "description": "Limite RPM target (min 300)"},
            },
            "required": ["machine_id", "rpm_limit"],
        },
    ),
    Tool(
        name="request_human_escalation",
        description=(
            "Richiede l'intervento di un operatore umano. "
            "USA QUESTO TOOL quando: (1) l'analisi indica uno shutdown necessario, "
            "(2) non sei sicuro della diagnosi, (3) il rischio è critico e l'azione supera "
            "i limiti consentiti agli agenti automatici."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "machine_id": {"type": "string"},
                "reason":     {"type": "string", "description": "Motivazione tecnica dettagliata per l'escalation"},
            },
            "required": ["machine_id", "reason"],
        },
    ),
    Tool(
        name="acknowledge_alert",
        description="Segna un alert come riconosciuto con una nota operatore. Non chiude il ticket di manutenzione.",
        inputSchema={
            "type": "object",
            "properties": {
                "alert_id":      {"type": "string"},
                "operator_note": {"type": "string"},
            },
            "required": ["alert_id", "operator_note"],
        },
    ),
]


@app.list_tools()
async def list_tools() -> list[Tool]:
    return TOOLS


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    log.info(f"Tool chiamato: {name} args={arguments}")

    try:
        if name == "open_maintenance_ticket":
            result = open_maintenance_ticket(
                arguments["machine_id"],
                arguments["urgency"],
                arguments["description"],
            )
        elif name == "set_machine_speed_limit":
            result = set_machine_speed_limit(
                arguments["machine_id"],
                float(arguments["rpm_limit"]),
            )
        elif name == "request_human_escalation":
            result = request_human_escalation(
                arguments["machine_id"],
                arguments["reason"],
            )
        elif name == "acknowledge_alert":
            result = acknowledge_alert(
                arguments["alert_id"],
                arguments.get("operator_note", ""),
            )
        else:
            result = {"status": "error", "reason": f"Tool sconosciuto: {name}"}

        return [TextContent(type="text", text=json.dumps(result, default=str))]

    except Exception as e:
        log.error(f"Errore in {name}: {e}", exc_info=True)
        return [TextContent(type="text", text=json.dumps({"status": "error", "reason": str(e)}))]


async def main():
    log.info(f"Remediation MCP server avviato. MongoDB: {MONGO_URI}")
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
