#!/usr/bin/env bash
# Slurm on-demand data server launcher for Hercules malleability
# Usage: ./slurm_data_server.sh <server_id> <num_active> <conf_path> <hercules_path> <partition> <account> [log_dir] [time_limit]

SERVER_ID="$1"
NUM_ACTIVE="$2"
CONF_PATH="$3"
HERCULES_PATH="$4"
PARTITION="$5"
ACCOUNT="$6"
LOG_DIR="${7:-${HERCULES_PATH}/tmp/}"
TIME_LIMIT="${8:-${HERCULES_SLURM_TIME:-01:00:00}}"

if [[ -z "${SERVER_ID}" || -z "${NUM_ACTIVE}" || -z "${CONF_PATH}" || -z "${HERCULES_PATH}" || -z "${PARTITION}" || -z "${ACCOUNT}" ]]; then
    echo "Usage: $0 <server_id> <num_active> <conf_path> <hercules_path> <partition> <account> [log_dir] [time_limit]" >&2
    exit 1
fi

mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/hercules_server_${SERVER_ID}_log.txt"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Requesting 1 node via salloc for server ID ${SERVER_ID} (partition=${PARTITION}, account=${ACCOUNT})..." >> "${LOG_FILE}"

exec salloc -N 1 \
    --partition="${PARTITION}" \
    -A "${ACCOUNT}" \
    --job-name="hercules-ds-${SERVER_ID}" \
    --time="${TIME_LIMIT}" \
    --cpus-per-task=1 \
    bash -c "
        TARGET_NODE=\$(scontrol show hostnames \"\$SLURM_NODELIST\" | head -n 1)
        echo \"[Slurm] Allocated node: \${TARGET_NODE} for data server ID ${SERVER_ID}\" >> '${LOG_FILE}'

        srun -N 1 -n 1 -w \"\${TARGET_NODE}\" --overlap bash -c \"
            export HERCULES_CONF='${CONF_PATH}'
            export HERCULES_THREAD_POOL=1
            cd '${HERCULES_PATH}'
            exec '${HERCULES_PATH}/build/hercules_server' d '${SERVER_ID}' '${NUM_ACTIVE}'
        \" >> '${LOG_FILE}' 2>&1
    "
