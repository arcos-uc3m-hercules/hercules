#!/bin/bash
#SBATCH --job-name=hercules    # Job name
#SBATCH --output=logs/hercules/%j_mdtest.log   # Standard output and error log
#SBATCH --time=06:00:00               # Time limit hrs:min:sec
#SBATCH --cpus-per-task=32

###SBATCH --exclude=broadwell-008,broadwell-010
###SBATCH --nodelist=broadwell-[013-028]
##SBATCH --mem=0
###SBATCH --exclusive=user
##SBATCH --overcommit
##SBATCH --oversubscribe

## broadwell-[046-062]

CONFIG_PATH=$1
NUMBER_OF_FILES_PER_PROCESS=$2
NUMBER_OF_PROCESS=$3
PROCESS_PER_NODE=$4
MDTEST_ITERATIONS=$5

## Uncomment when working in Tucan.
# IOR_PATH=/home/software/io500/bin
# module unload mpi
# module load mpi/mpich3/3.2.1

## Uncomment when working in Unito.
## MDTEST is part of the IOR package.
MDTEST_PATH=/beegfs/home/javier.garciablas/gsanchez/ior/bin
#spack unload mpich openmpi
#spack load mpich@3.2.1%gcc@=9.4.0 arch=linux-ubuntu20.04-zen
spack load mpich
whereis mpirun


## Uncomment when working in MN4.
# IOR_PATH=/apps/IOR/3.3.0/INTEL/IMPI/bin
# module unload impi
# module load gcc/9.2.0
# module load java/8u131
# module load openmpi/4.1.0
# module load ucx/1.13.1
# module load cmake/3.15.4
# module unload openmpi
# module load impi
# module load ior

## Local
# IOR_PATH=/usr/local/bin
# export UCX_TLS=ib
# export UCX_NET_DEVICES=ibs1 # Slow!
# spack load /sxjvb77
#set -x
# export UCX_NET_DEVICES="opap6s0:1"
#export UCX_IB_RCACHE_MAX_REGIONS="100"
#export UCX_TLS=rc,sm


# mpiexec -env UCX_NET_DEVICES "opap6s0:1" -n=1 ucx_info -T
#mpiexec -n=1 ucx_info -T
#set +x

#echo "temporal dir $TMPDIR"


echo "Starting Hercules with $POLICY policy"
start_=$(date +%s.%N)
if [ -z "$CONFIG_PATH" ]; then
   echo "No configuration file"
   source hercules start
else
   echo "Configuration file pass $CONFIG_PATH"
#   export HERCULES_DEBUG_LEVEL=SLOG_TIME
#   export HERCULES_DEBUG_LEVEL=none
   source /beegfs/home/javier.garciablas/hercules/scripts/hercules start \
   -f "$CONFIG_PATH" 
   unset HERCULES_DEBUG_LEVEL
fi
end_=$(date +%s.%N)
runtime=$(echo "$end_ - $start_" | bc -l)
echo "Hercules started in $runtime seconds, start=$start_, end=$end_"

# exit 0

echo "DATA SERVERS $HERCULES_NUM_DATA"
echo "METADATA SERVERS $HERCULES_NUM_METADATA"

echo "Running clients"

# -n to set the number of files/directories per process.
## Note that 100,000 files/directories is probably the minimum value that will deliver a meaningful result (such that MDS cacheing does not affect results).
## See more: https://wiki.lustre.org/MDTest
# number_of_files_per_process=$((10000/${NUMBER_OF_PROCESS}))
#number_of_files_per_process=$((1000/${NUMBER_OF_PROCESS}))
#number_of_files_per_process=$((10000))
# -n every process will creat/stat/read/remove # directories and files
COMMAND="$MDTEST_PATH/mdtest -n ${NUMBER_OF_FILES_PER_PROCESS}"
# -I number of items per directory in tree
#COMMAND="$MDTEST_PATH/mdtest -I ${number_of_files_per_process}"
# -C only create files/dirs
# COMMAND="$COMMAND -C"

# Number of iterations.
COMMAND="$COMMAND -i ${MDTEST_ITERATIONS}"

# Verbose
COMMAND="$COMMAND -V 1"
# -C to create files/directories only.
# COMMAND="$COMMAND -C"

## Add the working directory.
## The -u flag tells the program to assign a unique working directory per task.
COMMAND="$COMMAND -u -d /mnt/hercules/"
# COMMAND="$COMMAND -d /mnt/hercules/"

## Removes the data.out file from the checkpointing folder.
#rm ./HerculesCheckpoint/*
#rm /beegfs/home/javier.garciablas/hercules/bash/tests/disk/HerculesSnapshot/*

set -x
# HERCULES_DEBUG_LEVEL=none HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD  mkdir /mnt/hercules/directory_
# COMMAND=hostname

#mpiexec -np="$NUMBER_OF_PROCESS" --map-by node "$HERCULES_MPI_PPN"="$HERCULES_NCPN"  "$HERCULES_MPI_HOSTFILE_DEF"="$HERCULES_MPI_HOSTFILE_NAME" \
mpiexec -np="$NUMBER_OF_PROCESS" "$HERCULES_MPI_PPN""$HERCULES_NCPN" \
   "$HERCULES_MPI_HOSTFILE_DEF""$HERCULES_MPI_HOSTFILE_NAME" \
   "$HERCULES_MPI_ENV_DEF" HERCULES_CONF="$HERCULES_CONF" \
   "$HERCULES_MPI_ENV_DEF" LD_PRELOAD="$HERCULES_POSIX_PRELOAD" \
   ${COMMAND}

echo "listing directory"
## Checksum to the file.
HERCULES_DEBUG_LEVEL=none HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD  ls -lh /mnt/hercules/
#HERCULES_DEBUG_LEVEL=none HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD  ls -R /mnt/hercules/
#HERCULES_DEBUG_LEVEL=all HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD  ls -lh /mnt/hercules/directory_
#HERCULES_DEBUG_LEVEL=all HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD  md5sum /mnt/hercules/data.out
#HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD  cp /mnt/hercules/data.out /beegfs/home/javier.garciablas/hercules/bash/tests/disk/HerculesSnapshot/download_file
#HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD  md5sum /mnt/hercules/data.out

#mpiexec -np=8 $HERCULES_MPI_PPN=1 $HERCULES_MPI_HOSTFILE_DEF=./data_hostfile \
#	cat /tmp/hercules_pkill_operation

## Waits some seconds to allow Hercules finishing copying all blocks to disk.
#/beegfs/home/javier.garciablas/hercules/scripts/hercules stop \
#	   -f "$HERCULES_CONF"

echo "done!"
