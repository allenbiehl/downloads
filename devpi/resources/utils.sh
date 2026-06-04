#!/bin/bash
set -m

DEVPI_SERVER_ROLE="${DEVPI_SERVER_ROLE:-standalone}"
DEVPI_SERVER_PORT="${DEVPI_SERVER_PORT:-3141}"
DEVPI_BASE_ENDPOINT=http://localhost:${DEVPI_SERVER_PORT}

function log_info() {
    local MESSAGE="${1}"
    log_message "INFO" "${MESSAGE}"
}

function log_error() {
    local MESSAGE="${1}"
    log_message "ERROR" "${MESSAGE}"
}

function log_warning() {
    local MESSAGE="${1}"
    log_message "WARNING" "${MESSAGE}"
}

function log_message() {
    local LEVEL="${1}"
    local MESSAGE="${2}"
    echo "${LEVEL}: ${MESSAGE}" 1>&2
}

function initialize_server() {
    if [ -z "${DEVPI_ROOT_PASSWORD}" ]; then
        log_error "'DEVPI_ROOT_PASSWORD' password is undefined. Exiting."
        exit 1
    fi

    if [ -f "${DEVPISERVER_SERVERDIR}/.serverversion" ]; then
        log_info "devpi server already initialized"
    fi

    log_info "Initializing devpi server"
    devpi-init \
        --role "${DEVPI_SERVER_ROLE}" \
        --serverdir="${DEVPISERVER_SERVERDIR}" \
        --root-passwd "${DEVPI_ROOT_PASSWORD}"
}

function start_server() {
    initialize_server

    log_info "Starting devpi server"
    devpi-server \
        --host 0.0.0.0 \
        --port ${DEVPI_SERVER_PORT} \
        --serverdir "${DEVPISERVER_SERVERDIR}" \
        --restrict-modify root \
        "${@}" &

    wait_for_server_ready
    initialize_clients
    initialize_indexes

    log_info "Server started"
    fg %1
}

function wait_for_server_ready() {
    while true; do
        echo "Attempting to connect to devpi api..."
        local API_HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${DEVPI_BASE_ENDPOINT}/+api")

        if [ "${API_HTTP_CODE}" == "200" ]; then
            log_info "Devpi server api is up"
            break
        fi
        sleep 1
    done
}

function initialize_clients() {
    client_login "root" "${DEVPI_ROOT_PASSWORD}"

    env | while IFS='=' read -r ENV_NAME ENV_VALUE; do
        if [[ "${ENV_NAME}" =~ (DEVPI_CLIENT_)(.*)(_USERNAME) ]]; then
            local CLIENT_USERNAME_VAR_NAME="${BASH_REMATCH[1]}${BASH_REMATCH[2]}${BASH_REMATCH[3]}"
            local CLIENT_PASSWORD_VAR_NAME="${BASH_REMATCH[1]}${BASH_REMATCH[2]}_PASSWORD"        
            create_client "${!CLIENT_USERNAME_VAR_NAME}" "${!CLIENT_PASSWORD_VAR_NAME}"
        fi
    done
}

function initialize_indexes() {
    client_login "root" "${DEVPI_ROOT_PASSWORD}"

    env | while IFS='=' read -r ENV_NAME ENV_VALUE; do
        if [[ "${ENV_NAME}" =~ (DEVPI_INDEX_)(.*)(_NAME) ]]; then
            local INDEX_NAME_VAR_NAME="${BASH_REMATCH[1]}${BASH_REMATCH[2]}${BASH_REMATCH[3]}"
            local INDEX_OWNER_VAR_NAME="${BASH_REMATCH[1]}${BASH_REMATCH[2]}_OWNER"
            local INDEX_BASES_VAR_NAME="${BASH_REMATCH[1]}${BASH_REMATCH[2]}_BASES"
            local INDEX_ACL_UPLOAD_VAR_NAME="${BASH_REMATCH[1]}${BASH_REMATCH[2]}_ACL_UPLOAD"
            local INDEX_VOLATILE_VAR_NAME="${BASH_REMATCH[1]}${BASH_REMATCH[2]}_VOLATILE"
            local INDEX_TYPE_VAR_NAME="${BASH_REMATCH[1]}${BASH_REMATCH[2]}_TYPE"
            local INDEX_MIRROR_URL_VAR_NAME="${BASH_REMATCH[1]}${BASH_REMATCH[2]}_MIRROR_URL"            
            create_index \
                "${!INDEX_NAME_VAR_NAME}" \
                "${!INDEX_OWNER_VAR_NAME}" \
                "${!INDEX_BASES_VAR_NAME}" \
                "${!INDEX_ACL_UPLOAD_VAR_NAME}" \
                "${!INDEX_VOLATILE_VAR_NAME}" \
                "${!INDEX_TYPE_VAR_NAME}" \
                "${!INDEX_MIRROR_URL_VAR_NAME}"
        fi
    done
}

function initialize_tokens() {
    env | while IFS='=' read -r ENV_NAME ENV_VALUE; do
        if [[ "${ENV_NAME}" =~ (DEVPI_TOKEN_)(.*)(_NAME) ]]; then
            local TOKEN_NAME_VAR_NAME="${BASH_REMATCH[1]}${BASH_REMATCH[2]}${BASH_REMATCH[3]}"
            local TOKEN_OWNER_VAR_NAME="${BASH_REMATCH[1]}${BASH_REMATCH[2]}_OWNER"
            local TOKEN_PERMISSIONS_VAR_NAME="${BASH_REMATCH[1]}${BASH_REMATCH[2]}_PERMISSIONS"
            local TOKEN_EXPIRES_VAR_NAME="${BASH_REMATCH[1]}${BASH_REMATCH[2]}_EXPIRES"
            local TOKEN_INDEXES_VAR_NAME="${BASH_REMATCH[1]}${BASH_REMATCH[2]}_INDEXES"
            local TOKEN_PROJECTS_VAR_NAME="${BASH_REMATCH[1]}${BASH_REMATCH[2]}_PROJECTS"            
            create_token \
                "${!TOKEN_NAME_VAR_NAME}" \
                "${!TOKEN_OWNER_VAR_NAME}" \
                "${!TOKEN_PERMISSIONS_VAR_NAME}" \
                "${!TOKEN_EXPIRES_VAR_NAME}" \
                "${!TOKEN_INDEXES_VAR_NAME}" \
                "${!TOKEN_PROJECTS_VAR_NAME}"
        fi
    done
}

function get_client_info() {
    local CLIENT_USERNAME="${1}"
    curl -s -H "Accept: application/json" ${DEVPI_BASE_ENDPOINT} | \
        jq '.result | select(."'${CLIENT_USERNAME}'" != null) | ."'${CLIENT_USERNAME}'"'
}

function get_index_info() {
    local INDEX_OWNER="${1}"
    local INDEX_NAME="${2}"
    local INDEX_URL=${DEVPI_BASE_ENDPOINT}/${INDEX_OWNER}
    curl -s -H "Accept: application/json" "${INDEX_URL}" | \
        jq '.result.indexes | select(."'${INDEX_NAME}'" != null) | ."'${INDEX_NAME}'"'    
}

function create_client() {
    local CLIENT_USERNAME="${1}"
    local CLIENT_PASSWORD="${2}"
    local CLIENT_INFO=$(get_client_info ${CLIENT_USERNAME})

    if [ "${CLIENT_INFO}" != "" ]; then
        return
    fi

    log_info "Creating devpi client ${CLIENT_USERNAME}"
    devpi user -c "${CLIENT_USERNAME}" password="${CLIENT_PASSWORD}"

    if [ $? -eq 1 ]; then
        log_info "Failed to create devpi client ${CLIENT_USERNAME}"
        return 1
    fi

    log_info "Successfully created devpi client ${CLIENT_USERNAME}"
    return 0
}

function create_index() {
    local INDEX_NAME="${1}"
    local INDEX_OWNER="${2}"    
    local INDEX_BASES="${3}"
    local INDEX_ACL_UPLOAD="${4}"
    local INDEX_VOLATILE="${5}"
    local INDEX_TYPE="${6}"
    local INDEX_MIRROR_URL="${7}"        
    local INDEX_INFO=$(get_index_info "${INDEX_OWNER}" "${INDEX_NAME}")

    if [ "${INDEX_INFO}" != "" ]; then
        return
    fi     

    log_info "Creating devpi index ${INDEX_OWNER}/${INDEX_NAME}"

    local CMD=("devpi" "index" "-c" "${INDEX_OWNER}/${INDEX_NAME}")

    if [ -n "${INDEX_BASES}" ]; then
        CMD+=("bases=${INDEX_BASES}")
    fi

    if [ -n "${INDEX_ACL_UPLOAD}" ]; then
        CMD+=("acl_upload=${INDEX_ACL_UPLOAD}")
    fi

    if [ -n "${INDEX_VOLATILE}" ]; then
        CMD+=("volatile=${INDEX_VOLATILE}")
    fi

    if [ -n "${INDEX_TYPE}" ]; then
        CMD+=("type=${INDEX_TYPE}")
    fi 

    if [ -n "${INDEX_MIRROR_URL}" ]; then
        CMD+=("mirror_url=${INDEX_MIRROR_URL}")
    fi

    log_info "$(echo "${CMD[@]}")"
    
    "${CMD[@]}"

    if [ $? -eq 1 ]; then
        log_info "Failed to create devpi index ${INDEX_OWNER}/${INDEX_NAME}"
        return 1
    fi

    log_info "Successfully created devpi index ${INDEX_OWNER}/${INDEX_NAME}"
    return 0       
}

function create_token() {
    local TOKEN_NAME="${1}"
    local TOKEN_OWNER="${2:-root}"    
    local TOKEN_PERMISSIONS="${3}"
    local TOKEN_EXPIRES="${4}"
    local TOKEN_INDEXES="${5}"   
    local TOKEN_PROJECTS="${6}"
    local OPTIONS=("--user=${TOKEN_OWNER}")

    if [ -n "${TOKEN_PERMISSIONS}" ]; then
        OPTIONS+=("--allowed=${TOKEN_PERMISSIONS}")
    fi

    if [ -n "${TOKEN_EXPIRES}" ]; then
        OPTIONS+=("--expires=${TOKEN_EXPIRES}")
    fi

    if [ -n "${TOKEN_INDEXES}" ]; then
        OPTIONS+=("--indexes=${TOKEN_INDEXES}")
    fi

    if [ -n "${TOKEN_PROJECTS}" ]; then
        OPTIONS+=("--projects=${TOKEN_PROJECTS}")
    fi

    log_info "Creating devpi token ${TOKEN_NAME} ${OPTIONS[@]}"
    devpi token-create -y -v "${OPTIONS[@]}"
                        
    # if [ $? -eq 1 ]; then
    #     log_info "Failed to create devpi index ${INDEX_OWNER}/${INDEX_NAME}"
    #     return 1
    # fi

    # log_info "Successfully created devpi index ${INDEX_OWNER}/${INDEX_NAME}"
    # return 0
}

function client_login() {
    local CLIENT_USERNAME="${1}"
    local CLIENT_PASSWORD="${2}"
    local NOT_LOGGED_IN_REGEX="not logged in"
    local RESPONSE=$(devpi use "${DEVPI_BASE_ENDPOINT}")

    if [[ ! "${RESPONSE}" =~ $NOT_LOGGED_IN_REGEX ]]; then
        return 0
    fi

    log_info "Initiating devpi client ${CLIENT_USERNAME} login" 
    devpi login "${CLIENT_USERNAME}" --password="${CLIENT_PASSWORD}"

    if [ $? -eq 1 ]; then
        log_info "Login failed for devpi client ${CLIENT_USERNAME}"
        return 1
    fi

    log_info "Login succeeded for devpi client ${CLIENT_USERNAME}"
    return 0
}
