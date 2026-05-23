"""
Crew Agent Service
==================
Consuma il topic Kafka 'anomalies' prodotto da ml-detector,
recupera lo storico recente da MongoDB, lancia la crew AI (3 agenti sequenziali)
e pubblica il risultato sul topic 'alerts'.

Questo container NON fa ML: riceve eventi già classificati e si occupa
esclusivamente dell'analisi approfondita e della remediation.

Scaling note:
  - Il consumer group 'crew-agent-group' garantisce che ogni evento anomalia
    venga processato da una sola replica anche se ci sono più pod in K8s.
  - Il ThreadPoolExecutor (max_workers=5) permette di gestire burst di anomalie
    senza bloccare il consumer loop di Kafka.
  - Il cooldown in-memory è per sicurezza aggiuntiva, ma il vero rate-limiting
    è già applicato a monte da ml-detector.

Topic Kafka:
  INPUT   anomalies  →  eventi anomalia da ml-detector
  OUTPUT  alerts     →  alert arricchiti con analisi AI e azioni eseguite
"""

import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient

from crew_analysis import run_crew_with_guardrails

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [crew-agent] %(levelname)s — %(message)s",
)
log = logging.getLogger("crew-agent")

# ── Configurazione ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
ANOMALY_TOPIC   = os.getenv("ANOMALY_TOPIC", "anomalies")
ALERT_TOPIC     = os.getenv("ALERT_TOPIC", "alerts")
GROUP_ID        = os.getenv("GROUP_ID", "crew-agent-group")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongos1:27017,mongos2:27017")
MONGO_DB  = os.getenv("MONGO_DB", "factory")

# Configurazione LLM — inoltrata a crew_analysis tramite env
os.environ["OPENAI_API_KEY"]  = os.getenv("OPENAI_API_KEY", "any")
os.environ["OPENAI_API_BASE"] = os.getenv(
    "OPENAI_API_BASE",
    "https://litellm-proxy-1013932759942.europe-west8.run.app/v1",
)
LLM_MODEL       = os.getenv("MODEL", "gemini-2.5-pro")
LLM_CONFIG_NAME = f"openai/{LLM_MODEL}"

# Numero massimo di analisi AI in parallelo (ogni thread usa il LLM)
MAX_PARALLEL_CREWS = int(os.getenv("MAX_PARALLEL_CREWS", "5"))

# Cooldown locale: difesa in profondità aggiuntiva rispetto al cooldown del ml-detector.
# In condizioni normali questo non dovrebbe mai scattare, ma protegge in caso di
# replay di messaggi o bug nel producer.
LOCAL_COOLDOWN_SECONDS = int(os.getenv("LOCAL_COOLDOWN_SECONDS", "30"))

# Storico da MongoDB da passare alla crew (campioni)
HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "10"))


# ── Kafka helpers ──────────────────────────────────────────────────────────────

def connect_kafka(retries: int = 15) -> tuple[KafkaConsumer, KafkaProducer]:
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                ANOMALY_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                # Commit manuale: facciamo commit solo dopo aver accodato
                # l'analisi nel ThreadPoolExecutor, non dopo il suo completamento.
                # Questo è intenzionale: se il pod muore dopo il commit ma prima
                # del completamento, l'evento viene perso (at-most-once per la crew).
                # Per at-least-once andrebbe usato un pattern con DLQ o idempotency key.
                enable_auto_commit=True,
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                acks="all",
            )
            log.info(f"Connesso a Kafka: {KAFKA_BOOTSTRAP}")
            return consumer, producer
        except NoBrokersAvailable:
            log.warning(f"Kafka non disponibile, tentativo {attempt + 1}/{retries}...")
            time.sleep(5)
    raise RuntimeError("Impossibile connettersi a Kafka")


# ── MongoDB helper ─────────────────────────────────────────────────────────────

def connect_mongo(retries: int = 15) -> MongoClient:
    for attempt in range(retries):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            log.info(f"Connesso a MongoDB: {MONGO_URI}")
            return client
        except Exception as e:
            log.warning(f"MongoDB non disponibile, tentativo {attempt + 1}/{retries}: {e}")
            time.sleep(4)
    raise RuntimeError("Impossibile connettersi a MongoDB")


def fetch_history(db, machine_id: str) -> list[dict]:
    """Recupera gli ultimi HISTORY_LIMIT campioni per la macchina dal DB."""
    cursor = (
        db.sensor_readings
        .find({"machine_id": machine_id}, {"_id": 0})
        .sort("timestamp", -1)
        .limit(HISTORY_LIMIT)
    )
    return list(cursor)


# ── Thread AI ──────────────────────────────────────────────────────────────────

def process_crew_analysis(
    anomaly_event: dict,
    history_data: list[dict],
    producer: KafkaProducer,
) -> None:
    """
    Eseguita in thread separato per non bloccare il consumer loop Kafka.

    Riceve l'evento anomalia già deserializzato da ml-detector, lancia la crew,
    e pubblica l'alert arricchito sul topic 'alerts'.
    """
    m_id      = anomaly_event["machine_id"]
    detection = anomaly_event["detection"]
    reading   = anomaly_event["reading_snapshot"]

    log.info(
        f"==> [THREAD] Avvio analisi AI per {m_id} "
        f"(anomaly_id={anomaly_event['anomaly_id']}, "
        f"timeout={os.getenv('CREW_TIMEOUT_SECONDS', '120')}s)"
    )

    try:
        crew_result = run_crew_with_guardrails(
            machine_id=m_id,
            sensor_data=reading,
            history_context=json.dumps(history_data, default=str),
            llm_name=LLM_CONFIG_NAME,
        )

        alert = {
            "alert_id":        str(uuid.uuid4()),
            # Traceability: collega l'alert all'evento anomalia che lo ha generato
            "anomaly_id":      anomaly_event["anomaly_id"],
            "machine_id":      m_id,
            "timestamp":       datetime.now(timezone.utc).isoformat(),
            # Metadati dalla detection ML (provengono da ml-detector)
            "severity":        detection["severity"],
            "alert_type":      detection["type"],
            "detection_source": detection["source"],
            # Risultato della crew AI
            "crew_status":     crew_result["status"],
            "ai_reasoning":    crew_result.get("result") or crew_result.get("escalation", ""),
            # Snapshot del campione anomalo per audit trail
            "reading_snapshot": reading,
        }

        producer.send(ALERT_TOPIC, key=m_id.encode(), value=alert)
        log.info(
            f"==> [THREAD] Alert pubblicato per {m_id} "
            f"[crew_status={crew_result['status']}]"
        )

    except Exception as e:
        log.error(
            f"Errore nell'analisi crew per {m_id} "
            f"(anomaly_id={anomaly_event.get('anomaly_id')}): {e}",
            exc_info=True,
        )


# ── Main loop ──────────────────────────────────────────────────────────────────

def run():
    mongo_client = connect_mongo()
    db = mongo_client[MONGO_DB]

    consumer, producer = connect_kafka()

    executor = ThreadPoolExecutor(max_workers=MAX_PARALLEL_CREWS)

    # Cooldown locale in-memory (difesa in profondità)
    last_crew_time: dict[str, float] = {}

    log.info(
        f"Crew Agent avviato. "
        f"Input: '{ANOMALY_TOPIC}' → Output: '{ALERT_TOPIC}' | "
        f"Modello: {LLM_CONFIG_NAME} | "
        f"Max crew parallele: {MAX_PARALLEL_CREWS}"
    )

    for msg in consumer:
        anomaly_event = msg.value
        m_id          = anomaly_event.get("machine_id", "unknown")
        anomaly_id    = anomaly_event.get("anomaly_id", "?")
        detection     = anomaly_event.get("detection", {})

        log.warning(
            f"ANOMALY EVENT ricevuto: {m_id} | "
            f"{detection.get('type')} [{detection.get('severity')}] "
            f"via {detection.get('source')} | anomaly_id={anomaly_id}"
        )

        # Cooldown locale: protezione aggiuntiva contro replay o doppi invii
        now  = time.time()
        last = last_crew_time.get(m_id, 0.0)
        if now - last < LOCAL_COOLDOWN_SECONDS:
            remaining = int(LOCAL_COOLDOWN_SECONDS - (now - last))
            log.info(
                f"[{m_id}] Evento ignorato: cooldown locale attivo "
                f"({remaining}s rimanenti)"
            )
            continue

        last_crew_time[m_id] = now

        # Recupera storico da MongoDB per contestualizzare l'analisi AI
        try:
            history_data = fetch_history(db, m_id)
            log.info(f"[{m_id}] Storico recuperato: {len(history_data)} campioni")
        except Exception as e:
            log.warning(f"[{m_id}] Impossibile recuperare storico MongoDB: {e} — uso lista vuota")
            history_data = []

        # Accoda l'analisi AI nel thread pool (non bloccante)
        executor.submit(process_crew_analysis, anomaly_event, history_data, producer)
        log.info(f"[{m_id}] Analisi AI accodata nel ThreadPoolExecutor")


if __name__ == "__main__":
    run()
