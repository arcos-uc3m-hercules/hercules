#!/usr/bin/env bash
# Slurm on-demand data server launcher for Hercules malleability
# Usage: ./slurm_data_server.sh <server_id> <num_active> <conf_path> <hercules_path> <partition> <account> [log_dir] [exclude_nodes]

SERVER_ID="$1"
NUM_ACTIVE="$2"
CONF_PATH="$3"
HERCULES_PATH="$4"
PARTITION="$5"
ACCOUNT="$6"
LOG_DIR="${7:-${HERCULES_PATH}/tmp/}"
EXCLUDE_ARG="${8:-${HERCULES_EXCLUDE_NODES}}"
TIME_LIMIT="${HERCULES_SLURM_TIME:-01:00:00}"

if [[ -z "${SERVER_ID}" || -z "${NUM_ACTIVE}" || -z "${CONF_PATH}" || -z "${HERCULES_PATH}" || -z "${PARTITION}" || -z "${ACCOUNT}" ]]; then
    echo "Usage: $0 <server_id> <num_active> <conf_path> <hercules_path> <partition> <account> [log_dir] [exclude_nodes]" >&2
    exit 1
fi

mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/hercules_server_${SERVER_ID}_log.txt"

# Truncate log file for this commissioning attempt
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Requesting 1 node via salloc for server ID ${SERVER_ID} (partition=${PARTITION}, account=${ACCOUNT})..." > "${LOG_FILE}"

# Normalize the exclusion list to short hostnames matching Slurm NodeName format.
# Handles both a hostfile path (-f) and inline comma/space-separated strings.
# Strips domain suffixes (e.g., node01.local to node01), deduplicates,
# and outputs a comma-separated string for Slurm's --exclude flag.
SALLOC_EXCLUDE=()
if [[ -n "${EXCLUDE_ARG}" ]]; then
    if [[ -f "${EXCLUDE_ARG}" ]]; then
        # Read from hostfile: ignore empty/comment lines, extract short names
        CLEAN_EXCLUDE=$(awk '($1 != "" && $1 !~ /^#/) { split($1, a, "."); if (a[1] != "") print a[1] }' "${EXCLUDE_ARG}" | sort -u | paste -sd, -)
    else
        # Parse inline string: split by comma/space, extract short names
        CLEAN_EXCLUDE=$(echo "${EXCLUDE_ARG}" | tr ',' ' ' | awk '{
            for (i=1; i<=NF; i++) {
                split($i, a, ".")
                if (a[1] != "") print a[1]
            }
        }' | sort -u | paste -sd, -)
    fi

    if [[ -n "${CLEAN_EXCLUDE}" ]]; then
        SALLOC_EXCLUDE=(--exclude="${CLEAN_EXCLUDE}")
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Excluding already allocated Hercules nodes: ${CLEAN_EXCLUDE}" >> "${LOG_FILE}"
    fi
fi

# Resolve the server binary based on build type if configured
SERVER_BIN="${HERCULES_PATH}/build/hercules_server"
if [[ -n "${HERCULES_BUILD_TYPE}" && -x "${HERCULES_PATH}/build-${HERCULES_BUILD_TYPE}/hercules_server" ]]; then
    SERVER_BIN="${HERCULES_PATH}/build-${HERCULES_BUILD_TYPE}/hercules_server"
fi

# Clean up all inherited Slurm environment variables from the parent job/step
# (e.g. SLURM_JOB_ID, SLURM_STEP_ID, SLURM_CPU_BIND, SLURM_NODELIST) to ensure
# child salloc and srun create a clean, independent allocation without inheriting
# conflicting CPU masks or step affinities from the parent job.
for var in $(compgen -v | grep '^SLURM_'); do
    if [[ "${var}" != "SLURM_CONF" ]]; then
        unset "${var}"
    fi
done

exec salloc -N 1 \
    "${SALLOC_EXCLUDE[@]}" \
    --partition="${PARTITION}" \
    -A "${ACCOUNT}" \
    --job-name="hercules-ds-${SERVER_ID}" \
    --time="${TIME_LIMIT}" \
    --cpus-per-task=1 \
    bash -c "
        # Clear any binding variables inside the new allocation shell
        unset SLURM_CPU_BIND SLURM_CPU_BIND_LIST SLURM_CPU_BIND_TYPE SLURM_CPU_BIND_VERBOSE SLURM_MEM_BIND SLURM_MEM_BIND_TYPE SLURM_MEM_BIND_LIST

        TARGET_NODE=\$(scontrol show hostnames \"\$SLURM_NODELIST\" | head -n 1)
        echo \"[Slurm] Allocated node: \${TARGET_NODE} for data server ID ${SERVER_ID}\" >> '${LOG_FILE}'

        # Launch server with --cpu-bind=none so srun does not attempt to bind
        # task 0 to CPU 0 when the step is assigned a different core on the allocated node.
        srun --cpu-bind=none -N 1 -n 1 -w \"\${TARGET_NODE}\" --overlap bash -c \"
            export HERCULES_CONF='${CONF_PATH}'
            export HERCULES_THREAD_POOL=1
            export HERCULES_DATA_HOSTNAME='\${TARGET_NODE}'
            cd '${HERCULES_PATH}'
            exec '${SERVER_BIN}' d '${SERVER_ID}' '${NUM_ACTIVE}'
        \" >> '${LOG_FILE}' 2>&1
    "
