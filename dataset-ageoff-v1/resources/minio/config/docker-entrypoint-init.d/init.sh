#!/bin/bash
set -m

function log_info() {
  MSG="${1}"
  echo "${MSG}" 1>&2
}

function create_alias() {
  ALIAS=${1}
  log_info "Creating alias ${ALIAS}..."  

  while true; do
    mc alias set ${ALIAS} http://localhost:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} 2>/dev/null

    if [ -n "$(mc alias ls  ${ALIAS} 2>/dev/null)" ]; then
      break
    fi
    sleep 1
  done

  log_info "${ALIAS} ready!"  
}

create_alias myminio

mc mb myminio/ageoff
