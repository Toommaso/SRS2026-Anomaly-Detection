FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY crew_agent.py .
COPY crew_analysis.py .
COPY telemetry_mcp.py .
COPY remediation_mcp.py .

ENV KAFKA_BOOTSTRAP=kafka:9092
ENV ANOMALY_TOPIC=anomalies
ENV ALERT_TOPIC=alerts
ENV GROUP_ID=crew-agent-group
ENV MONGO_URI=mongodb://mongos1:27017,mongos2:27017
ENV MONGO_DB=factory
ENV MODEL=gemini-2.5-pro
ENV CREW_TIMEOUT_SECONDS=120
ENV AGENT_MAX_ITER=5
ENV MAX_PARALLEL_CREWS=5
ENV LOCAL_COOLDOWN_SECONDS=30
ENV HISTORY_LIMIT=10

CMD ["python", "crew_agent.py"]
