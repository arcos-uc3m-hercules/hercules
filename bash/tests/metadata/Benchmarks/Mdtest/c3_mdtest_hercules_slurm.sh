#!/bin/bash
#SBATCH --job-name=hercules    # Job name
#SBATCH --output=logs/hercules/%j_mdtest.log   # Standard output and error log
#SBATCH --time=8:00:00               # Time limit hrs:min:sec
#SBATCH --cpus-per-task=1
#SBATCH --partition=large
#SBATCH --ntasks-per-node=100 # fix the mpirun error: "There are not enough slots available in the system.."
#SBATCH --exclude=srvgpu[01-05],srv103,srv108

CONFIG_PATH=$1
NUMBER_OF_FILES_PER_PROCESS=$2
NUMBER_OF_PROCESS=$3
PROCESS_PER_NODE=$4
MDTEST_ITERATIONS=$5

## Print the deployment script and the configuration file
echo "------------------"
echo "Deployment script content:"
echo "------------------"
cat c3_HERCULES_MDTEST_NO_MALL_CONFIGFILE.sh
echo "------------------"
echo "Configuration file content:"
echo "------------------"
cat "${CONFIG_PATH}"


## MDTEST is part of the IO500 package.
MDTEST_PATH=/home/tester004/gesanche/io500/bin/

source "/home/tester004/load-local-spack.sh"
source "/home/tester004/gesanche/hercules/stuff/c3-spack-modules.sh"
module load hpcx

export UCX_NET_DEVICES="ib0"

echo "Starting Hercules with $POLICY policy"
start_=$(date +%s.%N)
if [ -z "$CONFIG_PATH" ]; then
   echo "Error: No Hercules configuration file has been provided."
   echo "This script must be executed using c3_HERCULES_MDTEST_NO_MALL_CONFIGFILE.sh"
   exit 0
else
   echo "Configuration file pass $CONFIG_PATH"
#   export HERCULES_DEBUG_LEVEL=SLOG_TIME
#   export HERCULES_DEBUG_LEVEL=none
   source /home/tester004/gesanche/hercules/scripts/hercules start \
   -f "$CONFIG_PATH" 
   unset HERCULES_DEBUG_LEVEL
fi
end_=$(date +%s.%N)
runtime=$(echo "$end_ - $start_" | bc -l)
echo "Hercules started in $runtime seconds, start=$start_, end=$end_"


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

set -x
# HERCULES_DEBUG_LEVEL=none HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD  mkdir /mnt/hercules/directory_
# COMMAND=hostname

#mpiexec -np="$NUMBER_OF_PROCESS" --map-by node "$HERCULES_MPI_PPN"="$HERCULES_NCPN"  "$HERCULES_MPI_HOSTFILE_DEF"="$HERCULES_MPI_HOSTFILE_NAME" \
#mpiexec -np="$NUMBER_OF_PROCESS" "$HERCULES_MPI_PPN""$HERCULES_NCPN" \
mpiexec --oversubscribe $HERCULES_MPI_NP$HERCULES_NNFC $HERCULES_MPI_PPN$HERCULES_NCPN  $HERCULES_MPI_HOSTFILE_DEF$HERCULES_MPI_HOSTFILE_NAME \
   $HERCULES_MPI_ENV_DEF HERCULES_CONF="$HERCULES_CONF" \
   $HERCULES_MPI_ENV_DEF LD_PRELOAD="$HERCULES_POSIX_PRELOAD" \
   ${COMMAND}

echo "listing directory"
## Checksum to the file.
HERCULES_DEBUG_LEVEL=none HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD  ls -lh /mnt/hercules/
#HERCULES_DEBUG_LEVEL=none HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD  ls -R /mnt/hercules/
#HERCULES_DEBUG_LEVEL=all HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD  ls -lh /mnt/hercules/directory_
#HERCULES_DEBUG_LEVEL=all HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD  md5sum /mnt/hercules/data.out
#HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD  md5sum /mnt/hercules/data.out

#mpiexec -np=8 $HERCULES_MPI_PPN=1 $HERCULES_MPI_HOSTFILE_DEF=./data_hostfile \
#	cat /tmp/hercules_pkill_operation


echo "done!"
