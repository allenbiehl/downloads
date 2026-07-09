#!/bin/bash
PROJECTS_DIR=$(git rev-parse --show-toplevel)
PROJECT_DIR="${PROJECTS_DIR}/spark-local"
SPARK_DIR="${PROJECTS_DIR}/spark-local/services/spark"
LIB_VERSION="${1:-3.5.1}"
LIB_DIR="${SPARK_DIR}/plugins/app-spark-common-${LIB_VERSION}"

# gradle -stop

# echo "Building spark plugin ${LIB_VERSION}"

# rm -rf "${LIB_DIR}/.gradle" \
#   "${LIB_DIR}/bin" \
#   "${LIB_DIR}/build" \
#   "${SPARK_DIR}/resources/plugins/app-spark-common-${LIB_VERSION}.jar"

# cd "${LIB_DIR}"

# gradle clean jar --refresh-dependencies

# if ! [ -f "${LIB_DIR}/build/libs/app-spark-common-${LIB_VERSION}.jar" ]; then
#   echo "Failed to build jar"
#   exit 1
# fi

# cd "${PROJECT_DIR}"

# cp "${LIB_DIR}/build/libs/app-spark-common-${LIB_VERSION}.jar" "${SPARK_DIR}/resources/plugins/"

# jar tf "${SPARK_DIR}/resources/plugins/app-spark-common-${LIB_VERSION}.jar"

docker compose -f "${PROJECT_DIR}/docker-compose-spark.yaml" --profile spark build