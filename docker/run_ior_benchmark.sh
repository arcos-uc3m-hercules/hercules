#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="${1:-arcosuc3m/hercules:escience2026}"
CONTAINER_NAME="hercules-test"

NUM_RANKS="${2:-4}"
ITERATIONS="${3:-5}"
TRANSFER_SIZE="${4:-1m}"
BLOCK_SIZE="${5:-4m}"
TARGET_PATH="/mnt/hercules/ior_shared.dat"

cleanup() {
    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        echo "FAILED (Exit Code: $EXIT_CODE) - Dumping Debug Logs:"
        echo "--- Container Logs ---"
        docker logs "${CONTAINER_NAME}" 2>&1 || true
        echo "--- Status Files in /hercules/code/tmp ---"
        docker exec -i "${CONTAINER_NAME}" ls -la /hercules/code/tmp 2>/dev/null || true
        docker exec -i "${CONTAINER_NAME}" head -n 20 /hercules/code/tmp/* 2>/dev/null || true
    fi
    echo "Stopping and Removing Hercules Container"
    docker stop "${CONTAINER_NAME}" 2>/dev/null || true
    docker rm "${CONTAINER_NAME}" 2>/dev/null || true
}
trap cleanup EXIT

echo "Starting Hercules Deployment (Single Data Server)"
docker stop "${CONTAINER_NAME}" 2>/dev/null || true
docker rm "${CONTAINER_NAME}" 2>/dev/null || true

docker run -d -t \
  --name "${CONTAINER_NAME}" \
  --hostname "${CONTAINER_NAME}" \
  --shm-size=2gb \
  -p 7500:7500 \
  -p 8500:8500 \
  "${IMAGE_NAME}"

echo "Waiting for Hercules Server to Initialize"
docker exec -i "${CONTAINER_NAME}" /hercules/code/scripts/check-servers.sh m 0 start /hercules/code
docker exec -i "${CONTAINER_NAME}" /hercules/code/scripts/check-servers.sh d 0 start /hercules/code
echo "Hercules Storage is Ready"

echo "Checking Server Logs"
docker logs "${CONTAINER_NAME}" | tail -n 20

echo "Checking Client Mount Access"
docker exec -i "${CONTAINER_NAME}" bash -c "
export HERCULES_CONF=/etc/hercules.conf
export LD_PRELOAD=/hercules/code/build/tools/libhercules_posix.so
ls -la /mnt/hercules/ || true
"

echo "Executing Parallel IOR Benchmark (${NUM_RANKS} MPI Ranks)"
docker exec -u 1000:1000 -w /tmp "${CONTAINER_NAME}" \
  mpirun \
    --mca pml ob1 \
    --mca btl self,vader,tcp \
    --mca mtl ^ofi,psm3 \
    --oversubscribe -np "${NUM_RANKS}" \
    -x HERCULES_CONF=/etc/hercules.conf \
    -x LD_PRELOAD=/hercules/code/build/tools/libhercules_posix.so \
    -x FI_PROVIDER="^psm3" \
    ior -a POSIX -w -r -W -R -i "${ITERATIONS}" -t "${TRANSFER_SIZE}" -b "${BLOCK_SIZE}" -s 1 \
        -o "${TARGET_PATH}"

echo "IOR Benchmark Finished Successfully"
