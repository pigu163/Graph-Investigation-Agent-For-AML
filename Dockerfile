FROM python:3.12-slim

WORKDIR /app
COPY src ./src
RUN pip install --no-cache-dir "confluent-kafka==2.16.0"

ENV PYTHONPATH="/app/src"
ENTRYPOINT ["python", "-m", "graph_investigation_agent_for_aml.producer"]
