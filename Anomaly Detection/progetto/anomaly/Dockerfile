FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mqtt_kafka_bridge.py .
COPY anomaly_detector_crew.py .
COPY mongo_writer.py .
COPY crew_analysis.py .
COPY telemetry_mcp.py .
COPY remediation_mcp.py .
COPY scada_executor.py .

ENV KAFKA_BOOTSTRAP=kafka:9092
ENV MQTT_HOST=mqtt-broker
ENV MQTT_PORT=1883
