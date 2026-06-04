#!/bin/bash
CACHE_DIR=".cache"
TAG="local/service:latest"
SRC_PYPROJECT_FILE="pyproject.toml"
TGT_PYPROJECT_FILE="${CACHE_DIR}/pyproject.toml"
PROFILE=

# Parse arguments
OPTS=$(getopt -o '' -l 'profile:,' -- "$@")

[ $? -eq 0 ] || { 
    echo "Incorrect options provided"
    exit 1
}

eval set -- "$OPTS"

while true; do
    case "${1}" in        
    --profile)
        shift;
        PROFILE="${1}"
        ;;                                   
    --)
        shift
        break
        ;;
    esac
    shift
done

function log_info() {
    local MESSAGE="${1}"
    log_message "INFO" "${MESSAGE}"
}

function log_message() {
    local LEVEL="${1}"
    local MESSAGE="${2}"
    echo "${LEVEL}: ${MESSAGE}" 1>&2
}

function should_build_image() {
    local CACHE_DIR="${1}"
    local TAG="${2}"
    local SRC_PYPROJECT_FILE="${3}"
    local TGT_PYPROJECT_FILE="${4}"

    if ! [ -d "${CACHE_DIR}" ]; then
        log_info "Creating cache directory"
        mkdir "${CACHE_DIR}"
        return 0
    fi

    local SRC_PYPROJECT_DATA=$(cat "${SRC_PYPROJECT_FILE}")
    local TGT_PYPROJECT_DATA=$(cat "${TGT_PYPROJECT_FILE}")

    if [ "${SRC_PYPROJECT_DATA}" != "${TGT_PYPROJECT_DATA}" ]; then
        log_info "Detected pyproject.toml changes"
        return 0
    fi

    local IMAGE_ID=$(docker image ls  ${TAG} --quiet)
    if [ -z "${IMAGE_ID}" ]; then
        log_info "Container image ${TAG} is missing"
        return 0
    fi

    log_info "Latest container image ${TAG} exists"    
    return 1
}

function build_image() {
    local CACHE_DIR="${1}"
    local TAG="${2}"
    local SRC_PYPROJECT_FILE="${3}"
    local TGT_PYPROJECT_FILE="${4}"

    # if ! $(should_build_image "${CACHE_DIR}" \
    #     "${TAG}" \
    #     "${SRC_PYPROJECT_FILE}" \
    #     "${TGT_PYPROJECT_FILE}"); then
    #     return
    # fi

    echo "Building image ${TAG}"
    docker buildx build \
        --platform=linux/amd64 \
        --load \
        -f Dockerfile \
        -t "${TAG}" \
        .

    if [ -d "${CACHE_DIR}" ]; then
        cp "${SRC_PYPROJECT_FILE}" "${TGT_PYPROJECT_FILE}"
    fi
}

if [ -z "${PROFILE}" ]; then
    echo "Profile is undefined"
    exit 1
fi

build_image \
    "${CACHE_DIR}" \
    "${TAG}" \
    "${SRC_PYPROJECT_FILE}" \
    "${TGT_PYPROJECT_FILE}"

# docker compose -f docker-compose.yml --profile "${PROFILE}" build

docker compose -f docker-compose.yml --profile "${PROFILE}" up

docker compose -f docker-compose.yml --profile "${PROFILE}" down