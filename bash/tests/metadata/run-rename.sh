#!/bin/bash
set +x
export HERCULES_CONF=/beegfs/home/javier.garciablas/hercules/bash/tests/metadata/configurations_hercules/hercules.conf 
export LD_PRELOAD=/beegfs/home/javier.garciablas/hercules/build/tools/libhercules_posix.so
touch /mnt/hercules/file.out
mkdir /mnt/hercules/dir
ls -l /mnt/hercules/
mv /mnt/hercules/file.out /mnt/hercules/x.out
mv /mnt/hercules/x.out /mnt/hercules/dir/
ls -l /mnt/hercules/
