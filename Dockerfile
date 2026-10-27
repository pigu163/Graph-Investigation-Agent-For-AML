FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir "confluent-kafka==2.16.0" "psycopg[binary]==3.3.6"
COPY src ./src

ENV PYTHONPATH="/app/src"
ENTRYPOINT ["python", "-m", "graph_investigation_agent_for_aml.producer"]
