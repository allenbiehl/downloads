#!/bin/bash
PROJECTS_DIR=$(git rev-parse --show-toplevel)
PROJECT_DIR="${PROJECTS_DIR}/etl"
PROFILE="${1:-all}"

docker compose -f "${PROJECT_DIR}/docker-compose.yaml" --profile "${PROFILE}" up --build

docker compose -f "${PROJECT_DIR}/docker-compose-etl.yaml" --profile "${PROFILE}" down
