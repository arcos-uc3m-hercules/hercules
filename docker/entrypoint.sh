#!/bin/sh

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
