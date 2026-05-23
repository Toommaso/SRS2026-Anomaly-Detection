FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ml_detector.py .

ENV KAFKA_BOOTSTRAP=kafka:9092
ENV INPUT_TOPIC=sensor-readings
ENV ANOMALY_TOPIC=anomalies
ENV GROUP_ID=ml-detector-group
ENV COOLDOWN_SECONDS=60

CMD ["python", "ml_detector.py"]
