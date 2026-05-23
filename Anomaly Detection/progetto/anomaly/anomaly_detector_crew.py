"""
Anomaly Detector — pipeline IoT→Kafka→CrewAI con MCP e guardrail

Flusso:
  1. Consuma sensor-readings da Kafka
  2. Fast detection (soglie deterministiche + IsolationForest)
  3. Se anomalia → cooldown check (max 1 crew/macchina/minuto)
  4. Lancia run_crew_with_guardrails() in thread separato (ThreadPoolExecutor)
       - Agente 1 usa tool MCP read-only su MongoDB
       - Agente 2 usa tool MCP di remediation con policy di sicurezza
       - Timeout SIGALRM 120s con escalation automatica se superato
  5. Pubblica alert su topic Kafka 'alerts'
"""

import json
import logging
import os
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient

from crew_analysis import run_crew_with_guardrails

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s"
)
log = logging.getLogger("anomaly-detector-crew")

# ── Configurazione ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPIC     = os.getenv("INPUT_TOPIC", "sensor-readings")
ALERT_TOPIC     = os.getenv("ALERT_TOPIC", "alerts")
MONGO_URI       = os.getenv("MONGO_URI", "mongodb://mongos1:27017,mongos2:27017")
MONGO_DB        = "factory"

os.environ["OPENAI_API_KEY"]  = os.getenv("OPENAI_API_KEY", "any")
os.environ["OPENAI_API_BASE"] = os.getenv("OPENAI_API_BASE",
    "https://litellm-proxy-1013932759942.europe-west8.run.app/v1")

LLM_MODEL       = os.getenv("MODEL", "gemini-2.5-pro")
LLM_CONFIG_NAME = f"openai/{LLM_MODEL}"

# Cooldown: analizza la stessa macchina al massimo una volta ogni 60s
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "60"))

# Massimo 5 analisi AI in parallelo
executor = ThreadPoolExecutor(max_workers=5)
last_ai_alert_time = {}

# ── Soglie deterministiche ────────────────────────────────────────────────────

THRESHOLDS = {
    "vibration_x": {"warning": 0.40, "fault": 0.70},
    "vibration_y": {"warning": 0.35, "fault": 0.60},
    "temperature":  {"warning": 85.0,  "fault": 100.0},
    "rpm":          {"warning": 1500.0, "fault": 1200.0},
}

ML_WARMUP_SAMPLES = 100
ML_WINDOW_SIZE    = 500


# ── Detector per macchina ─────────────────────────────────────────────────────

class MachineAnomalyDetector:
    def __init__(self, machine_id: str):
        self.machine_id = machine_id
        self.window = deque(maxlen=ML_WINDOW_SIZE)
        self.model = None
        self.model_trained = False
        self.sample_count = 0

    def _features(self, reading):
        return [
            reading.get("vibration_x", 0.0),
            reading.get("vibration_y", 0.0),
            reading.get("temperature", 0.0),
            reading.get("rpm", 0.0),
        ]

    def _threshold_check(self, reading):
        for field, levels in THRESHOLDS.items():
            val = reading.get(field)
            if val is None:
                continue
            if field == "rpm":
                if val < levels["fault"]:
                    return {"type": "rpm_low", "severity": "fault", "source": "threshold"}
                elif val < levels["warning"]:
                    return {"type": "rpm_low", "severity": "warning", "source": "threshold"}
            else:
                if val >= levels["fault"]:
                    return {"type": f"{field}_high", "severity": "fault", "source": "threshold"}
                elif val >= levels["warning"]:
                    return {"type": f"{field}_high", "severity": "warning", "source": "threshold"}
        return None

    def _ml_check(self, reading):
        from sklearn.ensemble import IsolationForest
        features = self._features(reading)
        self.window.append(features)
        self.sample_count += 1

        if self.sample_count < ML_WARMUP_SAMPLES:
            return None

        if self.sample_count % 200 == 0 or not self.model_trained:
            X = np.array(self.window)
            self.model = IsolationForest(contamination=0.005, n_estimators=100, random_state=42)
            self.model.fit(X)
            self.model_trained = True

        score = self.model.score_samples([features])[0]
        if self.model.predict([features])[0] == -1:
            return {
                "type": "ml_anomaly",
                "severity": "warning",
                "source": "isolation_forest",
                "score": round(float(score), 4),
            }
        return None

    def analyze(self, reading):
        alert = self._threshold_check(reading)
        if not alert:
            alert = self._ml_check(reading)
        return alert


# ── Thread AI ─────────────────────────────────────────────────────────────────

def process_ai_analysis(m_id, reading, history_data, fast_alert, producer):
    """Eseguita in thread separato per non bloccare il consumer Kafka."""
    try:
        log.info(f"==> [THREAD] Avvio analisi AI per {m_id} (timeout={os.getenv('CREW_TIMEOUT_SECONDS','120')}s)...")

        crew_result = run_crew_with_guardrails(
            machine_id=m_id,
            sensor_data=reading,
            history_context=json.dumps(history_data, default=str),
            llm_name=LLM_CONFIG_NAME,
        )

        final_alert = {
            "alert_id":              str(uuid.uuid4()),
            "machine_id":            m_id,
            "timestamp":             datetime.now(timezone.utc).isoformat(),
            "severity":              fast_alert["severity"],
            "type":                  fast_alert["type"],
            "source":                fast_alert["source"],
            "fast_detection_source": fast_alert["source"],
            "crew_status":           crew_result["status"],
            "ai_reasoning":          crew_result.get("result") or crew_result.get("escalation", ""),
            "reading_snapshot":      reading,
        }

        producer.send(ALERT_TOPIC, value=final_alert)
        log.info(f"==> [THREAD] ALERT pubblicato per {m_id} [crew_status={crew_result['status']}]")

    except Exception as e:
        log.error(f"Errore nell'analisi del thread AI per {m_id}: {e}", exc_info=True)


# ── Main loop ─────────────────────────────────────────────────────────────────

def run():
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]

    # Retry connessione Kafka
    for attempt in range(15):
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="crew-detector-group",
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            log.info(f"Connesso a Kafka. Modello AI: {LLM_CONFIG_NAME}")
            break
        except NoBrokersAvailable:
            log.warning(f"Kafka non disponibile, tentativo {attempt+1}/15...")
            time.sleep(5)
    else:
        raise RuntimeError("Impossibile connettersi a Kafka")

    detectors = {}

    for msg in consumer:
        reading = msg.value
        m_id = reading.get("machine_id", "unknown")

        if m_id not in detectors:
            detectors[m_id] = MachineAnomalyDetector(m_id)

        # 1. Fast detection
        fast_alert = detectors[m_id].analyze(reading)

        if not fast_alert:
            continue

        # 2. Cooldown check: max 1 crew/macchina ogni COOLDOWN_SECONDS
        current_time = time.time()
        if m_id in last_ai_alert_time and \
                (current_time - last_ai_alert_time[m_id] < COOLDOWN_SECONDS):
            continue

        log.warning(
            f"FAST ALERT [{fast_alert['source']}] {fast_alert['type']} su {m_id}. "
            f"Accodamento analisi AI..."
        )

        # 3. Recupero storico da MongoDB
        history_cursor = (
            db.sensor_readings
            .find({"machine_id": m_id}, {"_id": 0})
            .sort("timestamp", -1)
            .limit(10)
        )
        history_data = list(history_cursor)

        # 4. Lancia crew in background e torna a leggere Kafka
        last_ai_alert_time[m_id] = current_time
        executor.submit(process_ai_analysis, m_id, reading, history_data, fast_alert, producer)


if __name__ == "__main__":
    run()
