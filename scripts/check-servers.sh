#!/bin/bash

CheckForStatusFile() {
    FILE="${HERCULES_PATH}/tmp/$SERVER_TYPE-hercules-$SERVER_NUMBER-$ACTION"
    echo "Waiting for file ${FILE}"
    #rm ${FILE} 2> /dev/null
    ## Checks if the file exists.
    until [ -f "$FILE" ]; do
        echo "Waiting for $FILE, attemp $i" > ${HERCULES_PATH}/tmp/waiting-$SERVER_TYPE-$SERVER_NUMBER-$ACTION
        i=$(($i + 1))
        ## Waits "attemps" times, then an error is return.
        if [ $i -gt $ATTEMPS ]; then
            exit 1
        fi
        t=$(($i % 5))
        if [ $t -eq 0 ]; then
            echo "[+][$HOSTNAME] Waiting for server $SERVER_NUMBER"
        fi
        sleep 1
    done
    ## Checks if the server was deploy correctly.
    STATUS=$(cat -- "$FILE" | grep "STATUS" | awk '{print $3}')
    ## Removes the file.
    # set -x
    # rm ${FILE}
    # set +x
}


SERVER_TYPE=$1
SERVER_NUMBER=$2
ACTION=$3 # expected string action, e.g., down when servers are stopped.
HERCULES_PATH=$4
ATTEMPS=300
i=1

## To check if the temporal directory exists.
if [ ! -d "${HERCULES_PATH}/tmp" ]; then
    echo "[ERROR] Temporal path ${HERCULES_PATH}/tmp does not exist. Please, create it, or set the "HERCULES_PATH" option in the configuration file to overwrite it."
    exit 1
fi

echo "Running check servers script"
echo ${HERCULES_PATH}
CheckForStatusFile
# echo $STATUS > file-status.out
#echo "STATUS=$STATUS" >> ${HERCULES_PATH}/tmp/$SERVER_TYPE-hercules-$SERVER_NUMBER-$ACTION
# Wait while the server is locked. The server rewrites the status file in
# place (fopen "w+"), so an empty status means it is being written: wait too.
polls=0
until [ "$STATUS" = "OK" ] || [ "$STATUS" = "ERROR" ]; do
    polls=$((polls + 1))
    if [ $polls -gt $((ATTEMPS * 5)) ]; then
        exit 1
    fi
    if [ $((polls % 25)) -eq 1 ]; then
        echo "[+][$HOSTNAME] Server $SERVER_NUMBER status: ${STATUS:-<being written>}"
    fi
    sleep 0.2
    CheckForStatusFile > /dev/null
done


if [ "$STATUS" != "OK" ]; then
    # echo "[X] Error deploying server $SERVER_NUMBER."
    exit 1
fi
