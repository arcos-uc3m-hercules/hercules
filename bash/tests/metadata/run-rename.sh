#!/bin/bash
set +x
export HERCULES_CONF=/beegfs/home/javier.garciablas/hercules/bash/tests/metadata/configurations_hercules/hercules.conf 
export LD_PRELOAD=/beegfs/home/javier.garciablas/hercules/build/tools/libhercules_posix.so
export HERCULES_DEBUG_LEVEL=all
set -x
touch /mnt/hercules/file.out
mkdir /mnt/hercules/dir
ls -l /mnt/hercules/
mv /mnt/hercules/file.out /mnt/hercules/x.out
ls -l /mnt/hercules/
mv /mnt/hercules/x.out /mnt/hercules/dir/
ls -l /mnt/hercules/
touch /mnt/hercules/file.out
ls -l /mnt/hercules/
mv /mnt/hercules/dir/x.out /mnt/hercules/file.out
ls -l /mnt/hercules/
set +x
HERCULES_DEBUG_LEVEL=all mv /mnt/hercules/file.out /mnt/hercules/dir/file.out
