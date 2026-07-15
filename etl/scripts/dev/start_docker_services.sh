#!/bin/bash
set -e

PROJECTS_DIR=$(git rev-parse --show-toplevel)
PROJECT_DIR="${PROJECTS_DIR}/etl"
PROFILE=
BUILD=1

# Parse arguments
OPTS=$(getopt -o '' -l 'profile:,build,' -- "$@")

[ $? -eq 0 ] || { 
    echo "Incorrect options provided"
    exit 1
}

eval set -- "$OPTS"

while true; do
    case "${1}" in
    --profile)
        shift    
        PROFILE="${1}"
        ;;         
    --build)
        BUILD=0
        ;;                                        
    --)
        shift
        break
        ;;
    esac
    shift
done

if [ -z "${PROFILE}" ]; then
  echo "Profile is undefined"
  exit
fi

if [ ${BUILD} == 0 ]; then
  echo "Building image"
  docker compose -f "${PROJECT_DIR}/docker-compose.yaml" --profile "${PROFILE}" build

  if [ $? -ne 0 ]; then
    echo "ERROR: Podman build failed! Stopping script."
    exit 1
  fi
fi

docker compose -f "${PROJECT_DIR}/docker-compose.yaml" --profile "${PROFILE}" up

docker compose -f "${PROJECT_DIR}/docker-compose.yaml" --profile "${PROFILE}" down
