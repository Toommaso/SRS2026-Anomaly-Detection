FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY machine_simulator.py .
COPY scada_consumer.py .
COPY run_both.sh .
COPY baselines/* .
RUN chmod +x run_both.sh

ENV MQTT_HOST=mqtt-broker
ENV MQTT_PORT=1883
ENV PUBLISH_HZ=2
ENV FAULT_MACHINE=""
ENV KAFKA_BOOTSTRAP=kafka:9092

CMD ["./run_both.sh"]