"""
ML Detector Service
===================
Consuma il topic Kafka 'sensor-readings', applica detection a due livelli:

  1. Soglie deterministiche (veloce, nessun warm-up)
  2. IsolationForest per anomalie statistiche (attivo dopo ML_WARMUP_SAMPLES)

Se viene rilevata un'anomalia e il cooldown per quella macchina è scaduto,
pubblica un messaggio sul topic 'anomalies' con tutto il contesto necessario
al crew-agent a valle.

Il container NON conosce CrewAI, LLM o MongoDB: è stateless rispetto alla
persistenza, scala orizzontalmente con le partizioni Kafka.

Topic Kafka:
  INPUT   sensor-readings  →  letture grezze dei sensori
  OUTPUT  anomalies        →  eventi anomalia con snapshot + metadati detection
"""

import json
import logging
import os
import time
import uuid
from collections import deque
from datetime import datetime, timezone

import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ml-detector] %(levelname)s — %(message)s",
)
log = logging.getLogger("ml-detector")

# ── Configurazione ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPIC      = os.getenv("INPUT_TOPIC", "sensor-readings")
ANOMALY_TOPIC    = os.getenv("ANOMALY_TOPIC", "anomalies")
GROUP_ID         = os.getenv("GROUP_ID", "ml-detector-group")

# Cooldown: max 1 evento anomalia per macchina ogni N secondi
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "60"))

# ── Soglie deterministiche ─────────────────────────────────────────────────────
#
# Per rpm il controllo è invertito: valori BASSI sono anomali.

THRESHOLDS = {
    "vibration_x": {"warning": 0.40, "fault": 0.70},
    "vibration_y": {"warning": 0.35, "fault": 0.60},
    "temperature":  {"warning": 85.0,  "fault": 100.0},
    "rpm":          {"warning": 1500.0, "fault": 1200.0},
}

ML_WARMUP_SAMPLES = 100   # campioni minimi prima di addestrare il modello
ML_WINDOW_SIZE    = 500   # finestra rolling per il retraining


# ── Detector per singola macchina ──────────────────────────────────────────────

import pickle

class MachineAnomalyDetector:
    """
    Mantiene lo stato ML per una macchina specifica, ora persistito su disco.
    """

    def __init__(self, machine_id: str):
        self.machine_id      = machine_id
        self.window          = deque(maxlen=ML_WINDOW_SIZE)
        self.model           = None
        self.model_trained   = False
        self.sample_count    = 0
        
        # Configurazione percorso di persistenza
        self.model_dir = os.getenv("MODEL_DIR", "/data/ml_state")
        self.file_path = os.path.join(self.model_dir, f"{self.machine_id}.pkl")
        
        # Tenta il ripristino dello stato al boot
        self._load_state()

    def _load_state(self):
        """Carica lo stato del modello e della finestra da disco se esiste."""
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, "rb") as f:
                    state = pickle.load(f)
                    self.window        = state.get("window", deque(maxlen=ML_WINDOW_SIZE))
                    self.model         = state.get("model", None)
                    self.model_trained = state.get("model_trained", False)
                    self.sample_count  = state.get("sample_count", 0)
                log.info(f"[{self.machine_id}] Stato ML ripristinato con successo (Campioni: {self.sample_count})")
            except Exception as e:
                log.error(f"[{self.machine_id}] Errore nel caricamento dello stato ML: {e}. Riparto da zero.")

    def _save_state(self):
        """Salva lo stato corrente su disco."""
        try:
            os.makedirs(self.model_dir, exist_ok=True)
            state = {
                "window":        self.window,
                "model":         self.model,
                "model_trained": self.model_trained,
                "sample_count":  self.sample_count
            }
            with open(self.file_path, "wb") as f:
                pickle.dump(state, f)
        except Exception as e:
            log.error(f"[{self.machine_id}] Errore durante il salvataggio dello stato: {e}")

    def _features(self, reading: dict) -> list[float]:
        return [
            reading.get("vibration_x", 0.0),
            reading.get("vibration_y", 0.0),
            reading.get("temperature", 0.0),
            reading.get("rpm", 0.0),
        ]

    def _threshold_check(self, reading: dict) -> dict | None:
        for field, levels in THRESHOLDS.items():
            val = reading.get(field)
            if val is None:
                continue
            if field == "rpm":
                if val < levels["fault"]:
                    return {"type": "rpm_low", "severity": "fault",   "source": "threshold"}
                elif val < levels["warning"]:
                    return {"type": "rpm_low", "severity": "warning", "source": "threshold"}
            else:
                if val >= levels["fault"]:
                    return {"type": f"{field}_high", "severity": "fault",   "source": "threshold"}
                elif val >= levels["warning"]:
                    return {"type": f"{field}_high", "severity": "warning", "source": "threshold"}
        return None

    def _ml_check(self, reading: dict) -> dict | None:
        from sklearn.ensemble import IsolationForest

        features = self._features(reading)
        self.window.append(features)
        self.sample_count += 1

        # Retraining periodico ogni 200 campioni o al primo raggiungimento del warm-up
        if self.sample_count >= ML_WARMUP_SAMPLES:
            if self.sample_count % 200 == 0 or not self.model_trained:
                X = np.array(self.window)
                self.model = IsolationForest(
                    contamination=0.005,
                    n_estimators=100,
                    random_state=42,
                )
                self.model.fit(X)
                self.model_trained = True
                log.info(f"[{self.machine_id}] Modello IsolationForest aggiornato ed eseguito il fit.")
                # SALVATAGGIO STATO: Salviamo lo stato aggiornato su file ad ogni retraining
                self._save_state()

        if self.sample_count < ML_WARMUP_SAMPLES:
            return None

        score = self.model.score_samples([features])[0]
        if self.model.predict([features])[0] == -1:
            return {
                "type":     "ml_anomaly",
                "severity": "warning",
                "source":   "isolation_forest",
                "score":    round(float(score), 4),
            }
        return None

    def analyze(self, reading: dict) -> dict | None:
        alert = self._threshold_check(reading)
        if not alert:
            alert = self._ml_check(reading)
        return alert


# ── Kafka helpers ──────────────────────────────────────────────────────────────

def connect_kafka(retries: int = 15) -> tuple[KafkaConsumer, KafkaProducer]:
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
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


# ── Main loop ──────────────────────────────────────────────────────────────────

def run():
    consumer, producer = connect_kafka()

    detectors: dict[str, MachineAnomalyDetector] = {}
    last_anomaly_time: dict[str, float] = {}

    log.info(
        f"ML Detector avviato. "
        f"Input: '{INPUT_TOPIC}' → Output: '{ANOMALY_TOPIC}' | "
        f"Cooldown: {COOLDOWN_SECONDS}s"
    )

    for msg in consumer:
        reading = msg.value
        m_id    = reading.get("machine_id", "unknown")

        # Inizializza detector per nuova macchina
        if m_id not in detectors:
            detectors[m_id] = MachineAnomalyDetector(m_id)
            log.info(f"Nuovo detector inizializzato per '{m_id}'")

        # 1. Fast detection (soglie + ML)
        detection = detectors[m_id].analyze(reading)
        if not detection:
            continue

        # 2. Cooldown: evita di inondare il topic anomalies con la stessa macchina
        now = time.time()
        last = last_anomaly_time.get(m_id, 0.0)
        if now - last < COOLDOWN_SECONDS:
            remaining = int(COOLDOWN_SECONDS - (now - last))
            log.debug(f"[{m_id}] Anomalia rilevata ma in cooldown ({remaining}s rimanenti)")
            continue

        last_anomaly_time[m_id] = now

        # 3. Costruisce il messaggio per il topic 'anomalies'
        #    Contiene tutto il contesto che crew-agent dovrà usare senza
        #    dover ri-leggere il topic sensor-readings.
        anomaly_event = {
            "anomaly_id":       str(uuid.uuid4()),
            "machine_id":       m_id,
            "detected_at":      datetime.now(timezone.utc).isoformat(),
            # Snapshot del campione che ha triggerato l'anomalia
            "reading_snapshot": reading,
            # Metadati della detection
            "detection": {
                "type":     detection["type"],
                "severity": detection["severity"],
                "source":   detection["source"],
                # Solo IsolationForest aggiunge 'score'
                **({ "score": detection["score"] } if "score" in detection else {}),
            },
        }

        producer.send(ANOMALY_TOPIC, key=m_id.encode(), value=anomaly_event)

        log.warning(
            f"ANOMALIA [{detection['severity'].upper()}] {m_id} "
            f"via {detection['source']} — tipo: {detection['type']} "
            f"→ pubblicata su '{ANOMALY_TOPIC}'"
        )


if __name__ == "__main__":
    run()
