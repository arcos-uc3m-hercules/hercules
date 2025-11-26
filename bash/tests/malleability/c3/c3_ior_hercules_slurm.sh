#!/bin/bash
#SBATCH --job-name=hercules    # Job name
#SBATCH --output=logs/hercules/%j-ior.log   # Standard output and error log
#SBATCH --time=8:00:00               # Time limit hrs:min:sec
#SBATCH --cpus-per-task=36
#SBATCH --partition=large
#SBATCH --exclude=srvgpu[01-05],srv103,srv108
#SBATCH --nodelist=srv[101-104,122-125]
##SBATCH --nodefile=/home/tester004/gesanche/hercules/tmp/hostfile_3419

## I am passing a nodefile to use the same nodes for all tests, because some 
## nodes could have less resources.

CONFIG_PATH=$1
FILE_SIZE_PER_CLIENT=$2
NUMBER_OF_PROCESS=$3
PROCESS_PER_NODE=$4
IOR_FILE_PER_PROCESS=$5
IOR_AVOID_CACHE=$6


## Print the deployment script and the configuration file
echo "------------------"
echo "Deployment script content:"
echo "------------------"
cat c3_HERCULES_IOR_MALL_ON_CONFIGFILE.sh
echo "------------------"
echo "Configuration file content:"
echo "------------------"
cat "${CONFIG_PATH}"
echo "------------------"

## Uncomment when working in C3.
# IOR is part of IO500
IOR_PATH=/home/tester004/gesanche/io500/bin
## C3 packages.
source "/home/tester004/load-local-spack.sh"
source "/home/tester004/gesanche/hercules/Stuff/c3-spack-modules.sh"
module load hpcx

export UCX_NET_DEVICES="ib0"

echo "Starting Hercules with $POLICY policy"
start_=$(date +%s.%N)
if [ -z "$CONFIG_PATH" ]; then
   echo "No configuration file"
   source hercules start
else
   echo "Configuration file pass $CONFIG_PATH"
#   export HERCULES_DEBUG_LEVEL=none
   source /home/tester004/gesanche/hercules/scripts/hercules start \
   -f "$CONFIG_PATH" 
   unset HERCULES_DEBUG_LEVEL
fi
end_=$(date +%s.%N)
runtime=$(echo "$end_ - $start_" | bc -l)
echo "Hercules started in $runtime seconds, start=$start_, end=$end_"

echo "DATASERVERS $HERCULES_NUM_DATA"
echo "THREADS $THREAD_POOL"
echo "Running clients"

TRANSFER_SIZE=$FILE_SIZE_PER_CLIENT

# -W -R for Write and Read verification. 
# -k to keep the file (do not delete it after test).
COMMAND="$IOR_PATH/ior -w -r -W -R -t ${TRANSFER_SIZE}kb -b ${FILE_SIZE_PER_CLIENT}kb -s 1 -i 5"

if [ "$IOR_FILE_PER_PROCESS" -eq 1 ]; then
    ## -F for File-per-process.
    COMMAND="$COMMAND -F"
fi

if [ "$IOR_AVOID_CACHE" -eq 1 ]; then
    ## -C to reorder Tasks and -e to work around the effects of the page cache by using fsync.
    COMMAND="$COMMAND -C -e"
fi


set -x
#HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD ${COMMAND}
time mpiexec $HERCULES_MPI_NP$HERCULES_NNFC $HERCULES_MPI_PPN$HERCULES_NCPN  $HERCULES_MPI_HOSTFILE_DEF$HERCULES_MPI_HOSTFILE_NAME \
   $HERCULES_MPI_ENV_DEF HERCULES_CONF=$HERCULES_CONF \
   $HERCULES_MPI_ENV_DEF LD_PRELOAD=$HERCULES_POSIX_PRELOAD \
   $COMMAND

echo "done!"
exit 0
