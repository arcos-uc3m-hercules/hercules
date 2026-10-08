#!/bin/bash

# If hercules.conf does not exist in the mounted /hercules/conf directory, create it from template
if [ ! -f /hercules/conf/hercules.conf ]; then
    echo "[Entrypoint] /hercules/conf/hercules.conf not found. Initializing from template..."
    cp /hercules/code/conf/hercules-template.conf /hercules/conf/hercules.conf
    sed -ri 's|/home/hercules|/hercules/code|g' /hercules/conf/hercules.conf
    sed -ri 's|DATA_HOSTFILE = /home/data_hostfile|DATA_HOSTFILE = /hercules/conf/data_hostfile|g' /hercules/conf/hercules.conf
    sed -ri 's|ALLOC_DATA_HOSTFILE = /home/data_hostfile|ALLOC_DATA_HOSTFILE = /hercules/conf/data_hostfile|g' /hercules/conf/hercules.conf
    sed -ri 's|METADATA_HOSTFILE = /home/meta_hostfile|METADATA_HOSTFILE = /hercules/conf/metadata_hostfile|g' /hercules/conf/hercules.conf
    sed -ri 's|^NUM_DATA_SERVERS[[:space:]]*=.*|NUM_DATA_SERVERS = 2|g' /hercules/conf/hercules.conf
fi

# Determine configuration and hostfiles (prefer custom mounts if provided, otherwise default to standalone single-node)
CONF_FILE="/etc/hercules.conf"
if [ -f /hercules/conf/hercules.conf ]; then
    CONF_FILE="/hercules/conf/hercules.conf"
fi

DATA_HOSTFILE="/hercules/data"
if [ -f /hercules/conf/data_hostfile ]; then
    DATA_HOSTFILE="/hercules/conf/data_hostfile"
elif [ ! -f /hercules/data ]; then
    echo "localhost" > /hercules/data
fi

META_HOSTFILE="/hercules/metadata"
if [ -f /hercules/conf/metadata_hostfile ]; then
    META_HOSTFILE="/hercules/conf/metadata_hostfile"
elif [ ! -f /hercules/metadata ]; then
    echo "localhost" > /hercules/metadata
fi

# If remote data servers are specified in data_hostfile, wait for SSH to be reachable
if [ -f "${DATA_HOSTFILE}" ]; then
    for host in $(cat "${DATA_HOSTFILE}"); do
        if [ "${host}" != "localhost" ] && [ "${host}" != "127.0.0.1" ] && [ "${host}" != "$(hostname)" ]; then
            echo "[Entrypoint] Waiting for SSH on ${host}..."
            until ssh -o ConnectTimeout=1 -o StrictHostKeyChecking=no "${host}" "echo ok" >/dev/null 2>&1; do
                sleep 0.5
            done
            echo "[Entrypoint] SSH on ${host} is ready."
        fi
    done
fi

# Start Hercules
hercules start -m "${META_HOSTFILE}" -d "${DATA_HOSTFILE}" -f "${CONF_FILE}"

# Execute command passed to container or run an interactive bash shell
if [ $# -gt 0 ]; then
    exec "$@"
else
    exec bash
fi
