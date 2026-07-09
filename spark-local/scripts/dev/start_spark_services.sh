#!/bin/bash
PROJECTS_DIR=$(git rev-parse --show-toplevel)
PROJECT_DIR="${PROJECTS_DIR}/spark-local"
PROFILE="${1:-all}"

docker compose -f "${PROJECT_DIR}/docker-compose-spark.yaml" --profile "${PROFILE}" up

docker compose -f "${PROJECT_DIR}/docker-compose-spark.yaml" --profile "${PROFILE}" down
