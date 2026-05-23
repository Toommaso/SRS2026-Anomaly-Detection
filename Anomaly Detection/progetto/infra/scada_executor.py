"""
SCADA Executor Service
Simula il layer di esecuzione fisica dei comandi emessi dagli agenti AI.

Comportamento:
  - Poll MongoDB ogni POLL_INTERVAL_SECONDS cercando comandi con status='pending'
  - Attende EXECUTION_DELAY_SECONDS (simula latenza SCADA/PLC)
  - Aggiorna lo status a 'executed' e logga l'azione
  - Pubblica un evento su Kafka topic 'scada-events' per tracciabilità

In un sistema reale questo servizio sarebbe sostituito dall'integrazione
con il sistema SCADA/PLC reale via OPC-UA o Modbus.

Collection MongoDB monitorata: machine_commands
  - type: 'speed_limit'        → riduzione RPM macchina guasta
  - type: 'production_target'  → aumento RPM macchine sane
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [scada-executor] %(levelname)s — %(message)s"
)
log = logging.getLogger("scada-executor")

# ── Configurazione ────────────────────────────────────────────────────────────

MONGO_URI             = os.getenv("MONGO_URI", "mongodb://mongos1:27017,mongos2:27017")
MONGO_DB              = os.getenv("MONGO_DB", "factory")
KAFKA_BOOTSTRAP       = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SCADA_TOPIC           = os.getenv("SCADA_TOPIC", "scada-events")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
EXECUTION_DELAY_SECONDS = int(os.getenv("EXECUTION_DELAY_SECONDS", "30"))

MACHINE_PARTITION = {
    "machine_a": 0,
    "machine_b": 1,
    "machine_c": 2,
    "machine_d": 3,
}


# ── Connessioni ───────────────────────────────────────────────────────────────

def connect_mongo(retries=15) -> MongoClient:
    for attempt in range(retries):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            log.info(f"Connesso a MongoDB: {MONGO_URI}")
            return client
        except ConnectionFailure as e:
            log.warning(f"MongoDB non disponibile, tentativo {attempt+1}/{retries}: {e}")
            time.sleep(4)
    raise RuntimeError("Impossibile connettersi a MongoDB")


def connect_kafka(retries=15) -> KafkaProducer:
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
            )
            log.info(f"Connesso a Kafka: {KAFKA_BOOTSTRAP}")
            return producer
        except NoBrokersAvailable:
            log.warning(f"Kafka non disponibile, tentativo {attempt+1}/{retries}...")
            time.sleep(5)
    raise RuntimeError("Impossibile connettersi a Kafka")


# ── Esecuzione comandi ────────────────────────────────────────────────────────

def execute_command(db, producer, cmd: dict):
    """
    Simula l'esecuzione fisica di un comando su una macchina.
    Aggiorna MongoDB e pubblica evento su Kafka.
    """
    cmd_id     = cmd.get("command_id", str(cmd.get("_id", "?")))
    machine_id = cmd.get("machine_id", "?")
    cmd_type   = cmd.get("type", "?")
    executed_at = datetime.now(timezone.utc).isoformat()

    # Determina il valore target in base al tipo di comando
    if cmd_type == "speed_limit":
        target_rpm = cmd.get("rpm_limit")
        action_desc = f"Cambio velocita a {target_rpm} RPM (guasto rilevato)"
    elif cmd_type == "production_target":
        target_rpm = cmd.get("target_rpm")
        action_desc = f"Aumento velocita a {target_rpm} RPM (obiettivo produzione)"
    else:
        target_rpm = None
        action_desc = f"Comando sconosciuto: {cmd_type}"

    # Aggiorna status su MongoDB
    db.machine_commands.update_one(
        {"command_id": cmd_id},
        {"$set": {
            "status":       "executed",
            "executed_at":  executed_at,
            "executed_by":  "scada-executor",
        }}
    )

    # Pubblica evento su Kafka per tracciabilità
    event = {
        "event_id":    str(uuid.uuid4()),
        "event_type":  "command_executed",
        "command_id":  cmd_id,
        "machine_id":  machine_id,
        "command_type": cmd_type,
        "target_rpm":  target_rpm,
        "action_desc": action_desc,
        "executed_at": executed_at,
        "executor":    "scada-executor",
    }
    producer.send(SCADA_TOPIC, key=machine_id, value=event, partition=MACHINE_PARTITION[machine_id])

    log.warning(
        f"[ESEGUITO] {machine_id} | {cmd_type} | {action_desc} "
        f"(command_id: {cmd_id[:8]}...)"
    )


# ── Main loop ─────────────────────────────────────────────────────────────────

def run():
    mongo_client = connect_mongo()
    db = mongo_client[MONGO_DB]
    producer = connect_kafka()

    log.info(
        f"SCADA Executor avviato. "
        f"Poll: {POLL_INTERVAL_SECONDS}s, "
        f"Execution delay: {EXECUTION_DELAY_SECONDS}s"
    )

    # Dizionario per tracciare i comandi in attesa di esecuzione
    # { command_id: timestamp_quando_eseguire }
    pending_execution = {}

    while True:
        try:
            # Trova tutti i comandi pending
            pending_cmds = list(db.machine_commands.find({"status": "pending"}))

            now = time.time()

            for cmd in pending_cmds:
                cmd_id = cmd.get("command_id", str(cmd.get("_id")))

                if cmd_id not in pending_execution:
                    # Nuovo comando — schedula l'esecuzione dopo il delay
                    execute_at = now + EXECUTION_DELAY_SECONDS
                    pending_execution[cmd_id] = execute_at
                    machine_id = cmd.get("machine_id", "?")
                    cmd_type   = cmd.get("type", "?")
                    log.info(
                        f"[SCHEDULATO] {machine_id} | {cmd_type} | "
                        f"esecuzione tra {EXECUTION_DELAY_SECONDS}s "
                        f"(command_id: {cmd_id[:8]}...)"
                    )

                elif now >= pending_execution[cmd_id]:
                    # Delay scaduto — esegui il comando
                    execute_command(db, producer, cmd)
                    del pending_execution[cmd_id]

            # Pulisci pending_execution da comandi che non sono più nel DB
            current_ids = {
                cmd.get("command_id", str(cmd.get("_id")))
                for cmd in pending_cmds
            }
            stale = [cid for cid in pending_execution if cid not in current_ids]
            for cid in stale:
                del pending_execution[cid]

        except Exception as e:
            log.error(f"Errore nel loop di esecuzione: {e}", exc_info=True)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
