#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="${1:-arcosuc3m/hercules:escience2026}"
CONTAINER_NAME="hercules-test"

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

echo "Starting Hercules Deployment"
docker stop "${CONTAINER_NAME}" 2>/dev/null || true
docker rm "${CONTAINER_NAME}" 2>/dev/null || true

docker run -d -t \
  --name "${CONTAINER_NAME}" \
  --hostname localhost \
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

echo "Running Write and Readback Data Integrity Verification"
docker exec -i "${CONTAINER_NAME}" bash -c '
set -euo pipefail

TEST_SIZE="2M"
SRC_FILE="/tmp/test_src.bin"
DST_FILE="/tmp/test_dst.bin"
HERCULES_TARGET="/mnt/hercules/test_data_integrity.bin"

echo "[Client] Generating ${TEST_SIZE} test payload in ${SRC_FILE}..."
dd if=/dev/urandom of="${SRC_FILE}" bs=1M count=2 2>/dev/null
SRC_MD5=$(md5sum "${SRC_FILE}" | awk "{print \$1}")
echo "[Client] Source MD5: ${SRC_MD5}"

echo "[Client] Writing to Hercules mount (${HERCULES_TARGET})..."
export HERCULES_CONF=/etc/hercules.conf
export LD_PRELOAD=/hercules/code/build/tools/libhercules_posix.so
dd if="${SRC_FILE}" of="${HERCULES_TARGET}" bs=64k count=32 2>/dev/null

echo "[Client] Reading back from Hercules mount into ${DST_FILE}..."
dd if="${HERCULES_TARGET}" of="${DST_FILE}" bs=64k count=32 2>/dev/null
unset LD_PRELOAD

DST_MD5=$(md5sum "${DST_FILE}" | awk "{print \$1}")
echo "[Client] Readback MD5: ${DST_MD5}"

if [ "${SRC_MD5}" = "${DST_MD5}" ]; then
    echo "SUCCESS: MD5 Checksums Match 100%!"
    exit 0
else
    echo "ERROR: Checksum mismatch between write and read!"
    exit 1
fi
'

echo "Verification Finished"
