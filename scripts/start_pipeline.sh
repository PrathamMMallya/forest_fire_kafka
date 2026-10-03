#!/usr/bin/env bash
# Start the end-to-end wildfire streaming pipeline on localhost.
#   infra:   Kafka (docker) + Elasticsearch (docker)
#   services: producer, streams, detector, indexer, notifier, dashboard(:9991)
set -euo pipefail
cd "$(dirname "$0")/.."

PY="${PYTHON:-.venv/bin/python}"
COMPOSE="docker compose -f deploy/docker-compose.yml"
LOGS=logs; mkdir -p "$LOGS"

echo "== 1/5 infra (Kafka + Elasticsearch) =="
$COMPOSE up -d kafka elasticsearch
echo "waiting for Kafka..."
until $PY -c "
from confluent_kafka.admin import AdminClient
AdminClient({'bootstrap.servers':'localhost:9092'}).list_topics(timeout=3)
" >/dev/null 2>&1; do sleep 2; done
echo "waiting for Elasticsearch..."
until curl -sf http://localhost:9200 >/dev/null 2>&1; do sleep 2; done
echo "infra ready."

echo "== 2/5 topics =="
$PY -m streaming.create_topics

echo "== 3/5 model artifacts =="
if [ ! -f artifacts/best_RandomForest.pkl ]; then
  echo "no model artifact - training now (~1 min)..."
  $PY train_stream_model.py
else
  echo "model artifacts present."
fi

echo "== 4/5 pipeline services =="
nohup $PY -m streaming.streams_app >"$LOGS/streams.log" 2>&1 &
echo $! >"$LOGS/streams.pid"
for svc in detector indexer notifier; do
  nohup $PY -m streaming.$svc >"$LOGS/$svc.log" 2>&1 &
  echo $! >"$LOGS/$svc.pid"
done
nohup $PY -m streaming.api >"$LOGS/api.log" 2>&1 &
echo $! >"$LOGS/api.pid"
sleep 2

echo "== 5/5 producer (replay) =="
EPS="${EPS:-50}"
nohup $PY -m streaming.producer --mode replay --eps "$EPS" --speed 60 --loop \
  >"$LOGS/producer.log" 2>&1 &
echo $! >"$LOGS/producer.pid"

echo
echo "Pipeline is live:"
echo "  dashboard   http://localhost:9991"
echo "  api stats   http://localhost:9991/api/stats"
echo "  metrics     http://localhost:9991/metrics"
echo "  kafka ui    docker compose -f deploy/docker-compose.yml --profile ui up -d"
echo "  kibana      docker compose -f deploy/docker-compose.yml --profile kibana up -d"
echo "stop: ./scripts/stop_pipeline.sh"
