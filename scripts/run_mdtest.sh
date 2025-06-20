#!/bin/bash

for n in 4096 8192 16384 32768 65536; do
	echo "Running mdtest with -n $n..."
	LD_PRELOAD=/beegfs/home/javier.garciablas/gabriel/hercules/build/tools/libhercules_posix.so /beegfs/home/javier.garciablas/gsanchez/ior/bin/mdtest -i 3 -n $n -d /mnt/hercules/ > ../../evaluation/hercules/hercules_$n.txt
	done
