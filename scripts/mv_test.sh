#!/bin/bash

LD_PRELOAD=/beegfs/home/javier.garciablas/gabriel/hercules/build/tools/libhercules_posix.so mkdir /mnt/hercules/kk
LD_PRELOAD=/beegfs/home/javier.garciablas/gabriel/hercules/build/tools/libhercules_posix.so touch /mnt/hercules/kk/file1
LD_PRELOAD=/beegfs/home/javier.garciablas/gabriel/hercules/build/tools/libhercules_posix.so touch /mnt/hercules/kk/file2
LD_PRELOAD=/beegfs/home/javier.garciablas/gabriel/hercules/build/tools/libhercules_posix.so touch /mnt/hercules/kk/file3
LD_PRELOAD=/beegfs/home/javier.garciablas/gabriel/hercules/build/tools/libhercules_posix.so mkdir /mnt/hercules/kk/dir
LD_PRELOAD=/beegfs/home/javier.garciablas/gabriel/hercules/build/tools/libhercules_posix.so touch /mnt/hercules/kk/dir/file
LD_PRELOAD=/beegfs/home/javier.garciablas/gabriel/hercules/build/tools/libhercules_posix.so mkdir /mnt/hercules/dest
LD_PRELOAD=/beegfs/home/javier.garciablas/gabriel/hercules/build/tools/libhercules_posix.so mv /mnt/hercules/kk /mnt/hercules/dest
