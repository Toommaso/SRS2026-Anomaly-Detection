#!/bin/bash
set -euo pipefail

log() { echo "[$(date '+%H:%M:%S')] $*"; }

wait_for() {
  local host=$1 port=$2
  log "Attendo $host:$port ..."
  until mongosh --host "$host" --port "$port" --eval "db.adminCommand('ping')" --quiet --norc &>/dev/null; do
    sleep 2
  done
  log "$host:$port raggiungibile."
}

wait_primary() {
  local host=$1 port=$2 name=$3
  log "Attendo PRIMARY in $name ..."
  until mongosh --host "$host" --port "$port" --quiet --norc --eval "
    try {
      const s = rs.status();
      print(s.members.some(m => m.stateStr === 'PRIMARY'));
    } catch(e) {
      print(false);
    }
  " 2>/dev/null | grep -q true; do
    sleep 3
  done
  log "$name pronto."
}

rs_initiate() {
  local host=$1 port=$2 config=$3
  # Usa try/catch: rs.status() lancia eccezione se il replset non esiste ancora
  mongosh --host "$host" --port "$port" --quiet --norc --eval "
    try {
      rs.status();
      print('replica set già inizializzato');
    } catch(e) {
      print('inizializzazione replica set...');
      rs.initiate($config);
    }
  "
}

log "=== MONGO-INIT ==="

# --- Config Servers ---
log "=== configReplSet ==="
wait_for config1 27017
wait_for config2 27017
wait_for config3 27017

rs_initiate config1 27017 '{
  _id: "configReplSet", configsvr: true,
  members: [
    { _id: 0, host: "config1:27017" },
    { _id: 1, host: "config2:27017" },
    { _id: 2, host: "config3:27017" }
  ]
}'
wait_primary config1 27017 configReplSet

# --- Shard 1 ---
log "=== shard1ReplSet ==="
wait_for shard1a 27017
wait_for shard1b 27017
wait_for shard1c 27017

rs_initiate shard1a 27017 '{
  _id: "shard1ReplSet",
  members: [
    { _id: 0, host: "shard1a:27017" },
    { _id: 1, host: "shard1b:27017" },
    { _id: 2, host: "shard1c:27017" }
  ]
}'
wait_primary shard1a 27017 shard1ReplSet

# --- Shard 2 ---
log "=== shard2ReplSet ==="
wait_for shard2a 27017
wait_for shard2b 27017
wait_for shard2c 27017

rs_initiate shard2a 27017 '{
  _id: "shard2ReplSet",
  members: [
    { _id: 0, host: "shard2a:27017" },
    { _id: 1, host: "shard2b:27017" },
    { _id: 2, host: "shard2c:27017" }
  ]
}'
wait_primary shard2a 27017 shard2ReplSet

# --- Mongos temporaneo per registrazione shard ---
log "=== Avvio mongos temporaneo per registrazione shard ==="
mongos --configdb configReplSet/config1:27017,config2:27017,config3:27017 \
       --port 27018 --bind_ip_all --quiet &
MONGOS_PID=$!

log "Attendo mongos temporaneo ..."
until mongosh --port 27018 --eval "db.adminCommand('ping')" --quiet --norc &>/dev/null; do
  sleep 2
done

log "=== Aggiunta shard al cluster ==="
mongosh --port 27018 --quiet --norc --eval "
  try { sh.addShard('shard1ReplSet/shard1a:27017,shard1b:27017,shard1c:27017'); } catch(e) { print('shard1 gia presente'); }
  try { sh.addShard('shard2ReplSet/shard2a:27017,shard2b:27017,shard2c:27017'); } catch(e) { print('shard2 gia presente'); }
  sh.status();
"

kill "$MONGOS_PID" 2>/dev/null || true
log "=== Cluster inizializzato ==="
