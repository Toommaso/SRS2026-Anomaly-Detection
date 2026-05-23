FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mqtt_kafka_bridge.py .
COPY mongo_writer.py .
COPY scada_executor.py .

ENV KAFKA_BOOTSTRAP=kafka:9092
ENV MQTT_HOST=mqtt-broker
ENV MQTT_PORT=1883
