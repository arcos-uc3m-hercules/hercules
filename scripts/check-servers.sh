#!/bin/bash

CheckForStatusFile() {
    FILE="${HERCULES_PATH}/tmp/$SERVER_TYPE-hercules-$SERVER_NUMBER-$ACTION"
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


echo ${HERCULES_PATH}
CheckForStatusFile
#echo "STATUS=$STATUS" >> ${HERCULES_PATH}/tmp/$SERVER_TYPE-hercules-$SERVER_NUMBER-$ACTION
# If the server is locked we will wait until it is unlocked.
until [ "$STATUS" != "LOCKED" ]; do
    echo "[+][$HOSTNAME] Server $SERVER_NUMBER is locked, $STATUS"
    CheckForStatusFile
done


if [ "$STATUS" != "OK" ]; then
    # echo "[X] Error deploying server $SERVER_NUMBER."
    exit 1
fi
