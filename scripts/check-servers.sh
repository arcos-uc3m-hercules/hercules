#!/bin/bash

SERVER_TYPE=$1
SERVER_NUMBER=$2
ACTION=$3 # expected string action, e.g., down when servers are stopped.
HERCULES_PATH=$4

if [ -z "$SERVER_TYPE" ] || [ -z "$SERVER_NUMBER" ] || [ -z "$ACTION" ] || [ -z "$HERCULES_PATH" ]; then
    echo "[ERROR] Usage: $0 <server_type (m|d)> <server_number> <action (start|stop|remove)> <hercules_path>" >&2
    exit 1
fi

## Check if temporal directory exists.
if [ ! -d "${HERCULES_PATH}/tmp" ]; then
    echo "[ERROR] Temporal path ${HERCULES_PATH}/tmp does not exist. Please create it or set HERCULES_PATH." >&2
    exit 1
fi

## Set up output redirection to log file in temporal directory
LOG_FILE="${HERCULES_PATH}/tmp/check-servers-${SERVER_TYPE}-${SERVER_NUMBER}-${ACTION}.log"
exec > >(tee -a "${LOG_FILE}") 2>&1

CURRENT_HOST=$(hostname -s 2>/dev/null || echo "${HOSTNAME}")
ATTEMPTS="${HERCULES_CHECK_ATTEMPTS:-360}"
i=1

check_server_alive() {
    pgrep -f "^([^ ]*/)?hercules_server ${SERVER_TYPE:0:1}( |$)" >/dev/null 2>&1
    return $?
}

CheckForStatusFile() {
    FILE="${HERCULES_PATH}/tmp/$SERVER_TYPE-hercules-$SERVER_NUMBER-$ACTION"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')][${CURRENT_HOST}] Waiting for file ${FILE}"
    #rm ${FILE} 2> /dev/null
    local was_running=0
    ## Checks if the file exists.
    until [ -f "$FILE" ]; do
        echo "Waiting for $FILE, attempt $i" > "${HERCULES_PATH}/tmp/waiting-$SERVER_TYPE-$SERVER_NUMBER-$ACTION"
        
        # Check if the server process is still alive on this node
        if check_server_alive; then
            was_running=1
            local pid
            pid=$(pgrep -f "^([^ ]*/)?hercules_server ${SERVER_TYPE:0:1}( |$)" 2>/dev/null | head -n 1)
            if [ $((i % 5)) -eq 0 ]; then
                echo "[$(date '+%Y-%m-%d %H:%M:%S')][${CURRENT_HOST}] Waiting for server ${SERVER_NUMBER} (attempt ${i}/${ATTEMPTS}, PID ${pid} active)"
            fi
        else
            if [ "$ACTION" == "start" ]; then
                # If the process was seen running and then terminated without writing $FILE, it crashed.
                if [ $was_running -eq 1 ]; then
                    echo "[ERROR][${CURRENT_HOST}] Server process ${SERVER_TYPE} ${SERVER_NUMBER} crashed during initialization without writing ${FILE}." >&2
                    exit 1
                fi
            elif [ "$ACTION" == "stop" ] || [ "$ACTION" == "remove" ]; then
                # Process already stopped; wait 1 second for filesystem flush then check again
                sleep 1
                if [ -f "$FILE" ]; then
                    break
                fi
                echo "[ERROR][${CURRENT_HOST}] Server process ${SERVER_TYPE} ${SERVER_NUMBER} terminated without creating ${FILE}!" >&2
                exit 1
            fi
        fi

        i=$((i + 1))
        ## Waits "attempts" times, then an error is returned.
        if [ $i -gt $ATTEMPTS ]; then
            echo "[ERROR][${CURRENT_HOST}] Timeout waiting for ${FILE} after ${ATTEMPTS} attempts." >&2
            exit 1
        fi
        sleep 1
    done
    ## Checks if the server deployed or stopped correctly.
    STATUS=$(cat -- "$FILE" 2>/dev/null | grep "STATUS" | awk '{print $3}')
    if [ -z "$STATUS" ]; then
        STATUS=$(cat -- "$FILE" 2>/dev/null | grep -oE "OK|LOCKED|ERROR[A-Z0-9_]*" | head -n 1)
    fi
    echo "[$(date '+%Y-%m-%d %H:%M:%S')][${CURRENT_HOST}] Status file found: ${FILE}, STATUS=${STATUS}"
    ## Removes the file.
    # set -x
    # rm ${FILE}
    # set +x
}

echo "=== [$(date '+%Y-%m-%d %H:%M:%S')][${CURRENT_HOST}] Running check-servers script: type=${SERVER_TYPE} id=${SERVER_NUMBER} action=${ACTION} ==="
echo "HERCULES_PATH: ${HERCULES_PATH}"
CheckForStatusFile
# echo $STATUS > file-status.out
#echo "STATUS=$STATUS" >> ${HERCULES_PATH}/tmp/$SERVER_TYPE-hercules-$SERVER_NUMBER-$ACTION
# If the server is locked we will wait until it is unlocked.
until [ "$STATUS" != "LOCKED" ]; do
    echo "[+][$CURRENT_HOST] Server $SERVER_NUMBER is locked, $STATUS, waiting before re-checking..."
    sleep 1
    rm -f "${FILE}" 2>/dev/null
    CheckForStatusFile
done

if [ "$STATUS" != "OK" ]; then
    echo "[ERROR][${CURRENT_HOST}] Expected STATUS 'OK', got '${STATUS}' for server ${SERVER_NUMBER} action ${ACTION}." >&2
    exit 1
fi
echo "[OK][${CURRENT_HOST}] Server ${SERVER_NUMBER} ${ACTION} verified with STATUS=OK"
