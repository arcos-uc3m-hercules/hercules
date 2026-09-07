#!/bin/sh

# If hercules.conf does not exist in the mounted /hercules/conf directory, create it from template
if [ ! -f /hercules/conf/hercules.conf ]; then
    echo "[Entrypoint] /hercules/conf/hercules.conf not found. Initializing from template..."
    cp /hercules/code/conf/hercules-template.conf /hercules/conf/hercules.conf
    sed -ri 's|/home/hercules|/hercules/code|g' /hercules/conf/hercules.conf
    sed -ri 's|DATA_HOSTFILE = /home/data_hostfile|DATA_HOSTFILE = /hercules/data|g' /hercules/conf/hercules.conf
    sed -ri 's|ALLOC_DATA_HOSTFILE = /home/data_hostfile|ALLOC_DATA_HOSTFILE = /hercules/data|g' /hercules/conf/hercules.conf
    sed -ri 's|METADATA_HOSTFILE = /home/meta_hostfile|METADATA_HOSTFILE = /hercules/metadata|g' /hercules/conf/hercules.conf
fi

# Start SSH service
service ssh restart 

# Start Hercules
hercules start -m /hercules/metadata -d /hercules/data -f /hercules/conf/hercules.conf

# Execute command passed to container or run an interactive bash shell
if [ $# -gt 0 ]; then
    exec "$@"
else
    exec bash
fi
