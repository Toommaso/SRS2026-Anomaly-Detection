"""
Telemetry MCP Server
Espone tool read-only su MongoDB per gli agenti CrewAI.

Tool disponibili:
  - get_recent_readings(machine_id, limit)   ultimi N campioni sensore
  - get_active_alerts(machine_id)            alert aperti nelle ultime 2 ore
  - get_machine_baseline(machine_id)         media e stddev degli ultimi 500 campioni
  - get_fleet_status()                       stato sintetico di tutte le macchine

Ogni chiamata è loggata con timestamp e caller_id per auditability (SRS §4.2).
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [telemetry-mcp] %(levelname)s — %(message)s",
    stream=sys.stderr,   # MCP usa stdout per il protocollo, log su stderr
)
log = logging.getLogger("telemetry-mcp")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongos:27017")
MONGO_DB  = os.getenv("MONGO_DB", "factory")

# Limite massimo di documenti restituibili per chiamata (rate-limiting soft)
MAX_LIMIT = 200

# ── Connessione MongoDB ───────────────────────────────────────────────────────

_mongo_client: MongoClient | None = None

def get_db():
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return _mongo_client[MONGO_DB]


# ── Audit Log ─────────────────────────────────────────────────────────────────

def audit(tool_name: str, args: dict, result_size: int):
    """Logga ogni tool call per auditability (SRS §4.2)."""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "server": "telemetry-mcp",
        "tool": tool_name,
        "args": args,
        "result_docs": result_size,
    }
    log.info("AUDIT %s", json.dumps(entry))


# ── Implementazione tool ──────────────────────────────────────────────────────

def get_recent_readings(machine_id: str, limit: int = 20) -> list[dict]:
    limit = min(limit, MAX_LIMIT)
    db = get_db()
    cursor = (
        db.sensor_readings
        .find({"machine_id": machine_id}, {"_id": 0})
        .sort("timestamp", -1)
        .limit(limit)
    )
    return list(cursor)


def get_active_alerts(machine_id: str | None = None) -> list[dict]:
    db = get_db()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    query: dict[str, Any] = {"timestamp": {"$gte": cutoff}}
    if machine_id:
        query["machine_id"] = machine_id
    cursor = (
        db.alerts
        .find(query, {"_id": 0})
        .sort("timestamp", -1)
        .limit(50)
    )
    return list(cursor)


def get_machine_baseline(machine_id: str) -> dict:
    """Calcola media e stddev degli ultimi 500 campioni per la macchina."""
    import numpy as np
    db = get_db()
    docs = list(
        db.sensor_readings
        .find({"machine_id": machine_id}, {"_id": 0, "vibration_x": 1, "vibration_y": 1, "temperature": 1, "rpm": 1})
        .sort("timestamp", -1)
        .limit(500)
    )
    if not docs:
        return {"error": f"Nessun dato per {machine_id}"}

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
    return result


def get_fleet_status() -> list[dict]:
    """Stato sintetico di tutte le macchine: ultima lettura + alert attivi."""
    db = get_db()
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id": "$machine_id",
            "last_ts":     {"$first": "$timestamp"},
            "last_status": {"$first": "$status"},
            "last_vib_x":  {"$first": "$vibration_x"},
            "last_temp":   {"$first": "$temperature"},
        }},
    ]
    machines = list(db.sensor_readings.aggregate(pipeline))
    # Conta alert attivi per macchina nelle ultime 2 ore
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    for m in machines:
        m["machine_id"] = m.pop("_id")
        m["active_alerts"] = db.alerts.count_documents({
            "machine_id": m["machine_id"],
            "timestamp": {"$gte": cutoff},
        })
    return machines


# ── MCP Server ────────────────────────────────────────────────────────────────

app = Server("telemetry-mcp")

TOOLS = [
    Tool(
        name="get_recent_readings",
        description=(
            "Recupera le ultime N letture sensore per una macchina dal database storico. "
            "Usa questo tool per capire il trend recente prima di formulare una diagnosi."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "machine_id": {"type": "string", "description": "ID macchina (es. machine_a)"},
                "limit":      {"type": "integer", "description": "Numero campioni da restituire (max 200)", "default": 20},
            },
            "required": ["machine_id"],
        },
    ),
    Tool(
        name="get_active_alerts",
        description=(
            "Recupera gli alert attivi nelle ultime 2 ore. "
            "Se machine_id è omesso, restituisce alert di tutta la flotta."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "machine_id": {"type": "string", "description": "ID macchina (opzionale, ometti per flotta completa)"},
            },
        },
    ),
    Tool(
        name="get_machine_baseline",
        description=(
            "Calcola la baseline statistica (media, stddev, min, max) degli ultimi 500 campioni. "
            "Usa questo tool per stabilire se il dato anomalo è davvero fuori norma rispetto alla storia della macchina."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "machine_id": {"type": "string", "description": "ID macchina"},
            },
            "required": ["machine_id"],
        },
    ),
    Tool(
        name="get_fleet_status",
        description="Panoramica rapida di tutte le macchine: ultima lettura, stato e numero alert attivi.",
        inputSchema={"type": "object", "properties": {}},
    ),
]


@app.list_tools()
async def list_tools() -> list[Tool]:
    return TOOLS


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    log.info(f"Tool chiamato: {name} args={arguments}")

    try:
        if name == "get_recent_readings":
            machine_id = arguments["machine_id"]
            limit = int(arguments.get("limit", 20))
            result = get_recent_readings(machine_id, limit)
            audit(name, arguments, len(result))
            return [TextContent(type="text", text=json.dumps(result, default=str))]

        elif name == "get_active_alerts":
            machine_id = arguments.get("machine_id")
            result = get_active_alerts(machine_id)
            audit(name, arguments, len(result))
            return [TextContent(type="text", text=json.dumps(result, default=str))]

        elif name == "get_machine_baseline":
            machine_id = arguments["machine_id"]
            result = get_machine_baseline(machine_id)
            audit(name, arguments, 1)
            return [TextContent(type="text", text=json.dumps(result, default=str))]

        elif name == "get_fleet_status":
            result = get_fleet_status()
            audit(name, arguments, len(result))
            return [TextContent(type="text", text=json.dumps(result, default=str))]

        else:
            return [TextContent(type="text", text=f"Tool sconosciuto: {name}")]

    except Exception as e:
        log.error(f"Errore in {name}: {e}", exc_info=True)
        return [TextContent(type="text", text=f"Errore interno: {e}")]


async def main():
    log.info(f"Telemetry MCP server avviato. MongoDB: {MONGO_URI}")
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
