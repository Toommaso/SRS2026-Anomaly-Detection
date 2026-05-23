#!/bin/bash
set -e

NAMESPACE=factory
MONGO_POD=mongodb-psmdb-db-rs0-0
MONGO_URI="mongodb://databaseAdmin:fcSldEWtkiteZoEeo@localhost:27017/factory?authSource=admin"
KAFKA_POD=kafka-combined-0

echo "=== 1. Stop consumer ==="
kubectl scale deployment/mongo-writer -n $NAMESPACE --replicas=0
kubectl scale deployment/ml-detector -n $NAMESPACE --replicas=0

echo "=== 2. Pulizia MongoDB ==="
kubectl exec -n $NAMESPACE $MONGO_POD -- \
  mongosh "$MONGO_URI" --eval '
    db.sensor_readings.deleteMany({});
    db.alerts.deleteMany({});
    print("sensor_readings:", db.sensor_readings.countDocuments());
    print("alerts:", db.alerts.countDocuments());
  '

echo "=== 3. Reset offset Kafka ==="
for GROUP in mongo-writer-group ml-detector-group; do
  kubectl exec -n $NAMESPACE $KAFKA_POD -- \
    bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group $GROUP \
    --topic sensor-readings \
    --reset-offsets --to-latest --execute
done

echo "=== 4. Riavvio consumer ==="
kubectl scale deployment/mongo-writer -n $NAMESPACE --replicas=1
kubectl scale deployment/ml-detector -n $NAMESPACE --replicas=1

echo "=== Done ==="