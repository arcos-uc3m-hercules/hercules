#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/hercules-docker-compose.yml"

cleanup() {
    echo "=== Stopping Hercules Deployment with Docker Compose ==="
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

echo "=== Checking Server Logs ==="
echo "--- Server 0 (hercules-server) Logs ---"
docker logs hercules-server | tail -n 20
echo "--- Server 1 (hercules-server-2) Logs ---"
docker logs hercules-server-2 | tail -n 20

echo "=== Checking Client Mount Access ==="
docker exec -i hercules-client bash -c "
export HERCULES_CONF=/hercules/conf/hercules.conf
export LD_PRELOAD=/hercules/code/build/tools/libhercules_posix.so
ls -la /mnt/hercules/ || true
"

echo "=== Running Write and Readback Data Integrity Verification ==="
docker exec -i hercules-client bash -c '
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
export HERCULES_CONF=/hercules/conf/hercules.conf
export LD_PRELOAD=/hercules/code/build/tools/libhercules_posix.so
dd if="${SRC_FILE}" of="${HERCULES_TARGET}" bs=64k count=32 2>/dev/null

echo "[Client] Reading back from Hercules mount into ${DST_FILE}..."
dd if="${HERCULES_TARGET}" of="${DST_FILE}" bs=64k count=32 2>/dev/null
unset LD_PRELOAD

DST_MD5=$(md5sum "${DST_FILE}" | awk "{print \$1}")
echo "[Client] Readback MD5: ${DST_MD5}"

if [ "${SRC_MD5}" = "${DST_MD5}" ]; then
    echo "==================================================="
    echo "  SUCCESS: MD5 Checksums Match 100%!"
    echo "==================================================="
    exit 0
else
    echo "==================================================="
    echo "  ERROR: Checksum mismatch between write and read!"
    echo "==================================================="
    exit 1
fi
'

echo "=== Verification Finished Successfully ==="

docker compose -f "${COMPOSE_FILE}" down
