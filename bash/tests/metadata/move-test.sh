#!/bin/bash
HERCULES_DEBUG_LEVEL=none HERCULES_CONF=/beegfs/home/javier.garciablas/hercules/bash/tests/metadata/configurations_hercules/hercules.conf LD_PRELOAD=/beegfs/home/javier.garciablas/hercules/build/tools/libhercules_posix.so mkdir /mnt/hercules/dir
HERCULES_DEBUG_LEVEL=none HERCULES_CONF=/beegfs/home/javier.garciablas/hercules/bash/tests/metadata/configurations_hercules/hercules.conf LD_PRELOAD=/beegfs/home/javier.garciablas/hercules/build/tools/libhercules_posix.so touch /mnt/hercules/dir/myfile.txt
HERCULES_DEBUG_LEVEL=all HERCULES_CONF=/beegfs/home/javier.garciablas/hercules/bash/tests/metadata/configurations_hercules/hercules.conf LD_PRELOAD=/beegfs/home/javier.garciablas/hercules/build/tools/libhercules_posix.so mv /mnt/hercules/dir/myfile.txt /mnt/hercules/dir

