#!/bin/sh
set -e

# Avvia il simulatore e il consumer in parallelo nello stesso container.
python machine_simulator.py &
SIM_PID=$!
python scada_consumer.py &
CONSUMER_PID=$!

shutdown() {
  echo "Ricevuto segnale di arresto, terminazione dei processi..."
  kill -TERM "$SIM_PID" 2>/dev/null || true
  kill -TERM "$CONSUMER_PID" 2>/dev/null || true
  wait "$SIM_PID" 2>/dev/null || true
  wait "$CONSUMER_PID" 2>/dev/null || true
}

trap shutdown TERM INT
wait "$SIM_PID" "$CONSUMER_PID"
