#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/hercules-docker-compose.yml"

NUM_RANKS="${1:-4}"
ITERATIONS="${2:-5}"
TRANSFER_SIZE="${3:-1m}"
BLOCK_SIZE="${4:-4m}"
TARGET_PATH="/mnt/hercules/ior_shared.dat"

cleanup() {
    echo "=== Stopping Hercules Cluster ==="
    docker compose -f "${COMPOSE_FILE}" down --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Starting Hercules Deployment with Docker Compose ==="
docker compose -f "${COMPOSE_FILE}" down --remove-orphans 2>/dev/null || true
docker compose -f "${COMPOSE_FILE}" up -d

echo "=== Waiting for Hercules Servers to Initialize (via check-servers.sh) ==="
docker exec -i hercules-server /hercules/code/scripts/check-servers.sh m 0 start /hercules/code
docker exec -i hercules-server /hercules/code/scripts/check-servers.sh d 0 start /hercules/code
docker exec -i hercules-server /hercules/code/scripts/check-servers.sh d 1 start /hercules/code
echo "=== Hercules Storage Cluster is Ready (2 Data Servers Active) ==="

echo "=== Executing Parallel IOR Benchmark (${NUM_RANKS} MPI Ranks) ==="
docker exec -u 1000:1000 -w /tmp hercules-client \
  mpirun --oversubscribe -np "${NUM_RANKS}" \
    -x HERCULES_CONF=/hercules/conf/hercules.conf \
    -x LD_PRELOAD=/hercules/code/build/tools/libhercules_posix.so \
    ior -a POSIX -w -r -W -R -i "${ITERATIONS}" -t "${TRANSFER_SIZE}" -b "${BLOCK_SIZE}" -s 1 \
        -o "${TARGET_PATH}"

echo "=== IOR Benchmark Finished Successfully ==="
