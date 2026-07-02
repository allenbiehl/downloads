#!/bin/bash
PROJECTS_DIR=$(git rev-parse --show-toplevel)
OUTPUT_DIR="${PROJECTS_DIR}/spark-local/resources/spark/data/input/silver-to-gold"

if [ -d "${OUTPUT_DIR}" ]; then
  rm -rf "${OUTPUT_DIR}"
fi

mkdir -p "${OUTPUT_DIR}"

python scripts/dev/generate_files.py --total-files 100 --output-dir "${OUTPUT_DIR}"