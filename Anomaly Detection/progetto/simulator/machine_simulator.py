"""
Machine Simulator — pubblica dati sensori su MQTT
Legge file .mat del dataset CWRU (Case Western Reserve University)
oppure genera dati sintetici se il dataset non è disponibile.

Topic MQTT: factory/{machine_id}/sensors
Payload JSON: { machine_id, timestamp, vibration_x, vibration_y, temperature, rpm, status }
"""

import json
import time
import random
import logging
import os
import threading
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional

import paho.mqtt.client as mqtt
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s"
)
log = logging.getLogger("simulator")

# ── Configurazione ────────────────────────────────────────────────────────────

# Legge quale macchina simulare da env
MACHINE_ID = os.getenv("MACHINE_ID", "machine_a")
MACHINES = [MACHINE_ID]

MQTT_HOST     = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT     = int(os.getenv("MQTT_PORT", "1883"))
PUBLISH_HZ    = float(os.getenv("PUBLISH_HZ", "2"))   # campionamenti/sec per macchina
FAULT_MACHINE = os.getenv("FAULT_MACHINE", "")        # es. "machine_b" per iniettare fault

KAFKA_BOOTSTRAP       = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SCADA_TOPIC           = os.getenv("SCADA_TOPIC", "scada-events")
GROUP_ID = os.getenv("SCADA_CONSUMER_GROUP", "scada-events-inspector")

# Parametri baseline per macchina (normale funzionamento)
# BASELINES = {
#    "machine_a": dict(vib=0.12, temp=65.0, rpm=1800),
#     "machine_b": dict(vib=0.15, temp=70.0, rpm=1750),
#     "machine_c": dict(vib=0.10, temp=60.0, rpm=2000),
#     "machine_d": dict(vib=0.13, temp=68.0, rpm=1850),
# }

# ── Struttura dati sensore ────────────────────────────────────────────────────

@dataclass
class SensorReading:
    machine_id:   str
    timestamp:    str
    vibration_x:  float   # g (accelerazione)
    vibration_y:  float   # g
    temperature:  float   # °C
    rpm:          float
    status:       str     # "normal" | "warning" | "fault"
    fault_type:   Optional[str] = None  # "bearing_inner" | "bearing_outer" | "overtemp"

# ── Logica di generazione dati ────────────────────────────────────────────────

class MachineDataGenerator:
    """Genera letture realistiche per una macchina, con fault injection."""

    def __init__(self, machine_id: str):
        self.machine_id = machine_id

        #lettura da file per i valori di baseline -Lorenzo
        with open("baselines_" + machine_id + ".txt") as f:
            self.base = dict(vib=float(f.readline()), temp=float(f.readline()), rpm=float(f.readline()))


        self._fault_active = False
        self._fault_type: Optional[str] = None
        self._fault_severity = 0.0   # cresce nel tempo (degrado progressivo)
        self._t = 0.0                # tempo interno per pattern ciclici

    def inject_fault(self, fault_type: str):
        """Attiva un fault progressivo sulla macchina."""
        self._fault_active = True
        self._fault_type = fault_type
        self._fault_severity = 0.0
        log.warning(f"[{self.machine_id}] FAULT INJECTED: {fault_type}")

    def clear_fault(self):
        self._fault_active = False
        self._fault_type = None
        self._fault_severity = 0.0

    def _normal_vibration(self) -> tuple[float, float]:
        """Vibrazione normale con rumore gaussiano + componente ciclica."""
        base = self.base["vib"]
        cycle = 0.02 * np.sin(2 * np.pi * self._t / 10)
        noise_x = np.random.normal(0, base * 0.08)
        noise_y = np.random.normal(0, base * 0.06)
        return round(base + cycle + noise_x, 4), round(base * 0.8 + noise_y, 4)

    def _fault_vibration(self) -> tuple[float, float]:
        """Vibrazione anomala: aumenta con la severity del guasto."""
        base = self.base["vib"]
        sev = self._fault_severity

        if self._fault_type == "bearing_inner":
            # Picchi impulsivi periodici (caratteristica guasto inner race)
            impulse = sev * 0.8 * abs(np.sin(2 * np.pi * self._t * 3.5))
            vx = base + impulse + np.random.normal(0, sev * 0.1)
            vy = base * 0.8 + impulse * 0.6 + np.random.normal(0, sev * 0.08)
        elif self._fault_type == "bearing_outer":
            # Vibrazione sostenuta ad alta frequenza
            vx = base + sev * 0.5 + np.random.normal(0, sev * 0.15)
            vy = base * 0.8 + sev * 0.4 + np.random.normal(0, sev * 0.12)
        else:
            vx = base + sev * 0.3 + np.random.normal(0, 0.02)
            vy = base * 0.8 + sev * 0.2 + np.random.normal(0, 0.015)

        return round(max(0, vx), 4), round(max(0, vy), 4)

    def next_reading(self) -> SensorReading:
        self._t += 1.0 / PUBLISH_HZ

        # Rilegge i valori aggiornati dal file a ogni campionamento
        with open("baselines_" + self.machine_id + ".txt") as f:
            self.base = dict(
                vib=float(f.readline()),
                temp=float(f.readline()),
                rpm=float(f.readline())
            )

        # Degrado progressivo del fault (peggiora nel tempo)
        if self._fault_active:
            self._fault_severity = min(2.0, self._fault_severity + 0.005)

        # Vibrazione
        if self._fault_active:
            vx, vy = self._fault_vibration()
        else:
            vx, vy = self._normal_vibration()

        # Temperatura: sale con vibrazione alta (attrito)
        base_temp = self.base["temp"]
        temp_delta = (vx - self.base["vib"]) * 15.0
        if self._fault_type == "overtemp":
            temp_delta += self._fault_severity * 20.0
        temperature = round(base_temp + temp_delta + np.random.normal(0, 0.5), 2)

        # RPM: cala leggermente sotto carico anomalo
        rpm = self.base["rpm"] + np.random.normal(0, 10)
        if self._fault_active:
            rpm -= self._fault_severity * 30
        rpm = round(max(0, rpm), 1)

        # Determina status
        if not self._fault_active:
            status = "normal"
            fault_type = None
        elif self._fault_severity < 0.3:
            status = "warning"
            fault_type = self._fault_type
        else:
            status = "fault"
            fault_type = self._fault_type

        return SensorReading(
            machine_id=self.machine_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            vibration_x=vx,
            vibration_y=vy,
            temperature=temperature,
            rpm=rpm,
            status=status,
            fault_type=fault_type,
        )


# ── MQTT Publisher ────────────────────────────────────────────────────────────

class MachineSimulator:
    def __init__(self):
        self.generators = {m: MachineDataGenerator(m) for m in MACHINES}
        self.client = mqtt.Client(client_id=f"factory-simulator-{MACHINE_ID}")
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self._running = False

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info(f"Connesso al broker MQTT {MQTT_HOST}:{MQTT_PORT}")
        else:
            log.error(f"Connessione fallita, rc={rc}")

    def _on_disconnect(self, client, userdata, rc):
        log.warning(f"Disconnesso dal broker (rc={rc}), riconnessione...")

    def connect(self, retries=10):
        for attempt in range(retries):
            try:
                self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
                self.client.loop_start()
                return
            except Exception as e:
                log.warning(f"Tentativo {attempt+1}/{retries} fallito: {e}")
                time.sleep(3)
        raise RuntimeError("Impossibile connettersi al broker MQTT")

    def _publish_machine(self, machine_id: str):
        """Loop di pubblicazione per una singola macchina."""
        gen = self.generators[machine_id]
        interval = 1.0 / PUBLISH_HZ
        while self._running:
            reading = gen.next_reading()
            topic = f"factory/{machine_id}/sensors"
            payload = json.dumps(asdict(reading))
            result = self.client.publish(topic, payload, qos=0)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                log.error(f"Publish fallito su {topic}")
            else:
                if reading.status != "normal":
                    log.warning(f"[{machine_id}] {reading.status.upper()} "
                                f"vib={reading.vibration_x:.3f}g "
                                f"temp={reading.temperature:.1f}°C "
                                f"fault={reading.fault_type}")
            time.sleep(interval)

    def run(self):
        self._running = True

        # Inject fault sulla macchina configurata via env
        if FAULT_MACHINE and FAULT_MACHINE in self.generators:
            # Aspetta 15 secondi poi inietta il fault
            def delayed_fault():
                time.sleep(15)
                self.generators[FAULT_MACHINE].inject_fault("bearing_inner")
            threading.Thread(target=delayed_fault, daemon=True).start()

        # Avvia un thread per ogni macchina
        threads = []
        for machine_id in MACHINES:
            t = threading.Thread(
                target=self._publish_machine,
                args=(machine_id,),
                daemon=True,
                name=f"sim-{machine_id}"
            )
            t.start()
            threads.append(t)
            log.info(f"Avviato simulatore per {machine_id}")

        log.info(f"Simulatore attivo — {len(MACHINES)} macchine @ {PUBLISH_HZ} Hz")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("Arresto simulatore...")
            self._running = False


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sim = MachineSimulator()
    sim.connect()
    sim.run()
