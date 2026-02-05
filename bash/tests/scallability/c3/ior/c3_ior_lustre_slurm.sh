#!/bin/bash
#SBATCH --job-name=lustre    # Job name
#SBATCH --time=00:30:00               # Time limit hrs:min:sec
#SBATCH --output=logs/lustre/%j.log   # Standard output and error log
#SBATCH --cpus-per-task=1
#SBATCH --partition=large
#SBATCH --ntasks-per-node=100 # fix the mpirun error: "There are not enough slots available in the system.."
#SBATCH --exclude=srvgpu[01-05]

FILE_SIZE_PER_CLIENT=$1
NUMBER_OF_PROCESS=$2
PROCESS_PER_NODE=$3
IOR_FILE_PER_PROCESS=$4
IOR_AVOID_CACHE=$5

LUSTRE_PATH=/lustre/uc3m_a0/sciot/gesanche/ior_output/
IOR_PATH=/home/tester004/gesanche/io500/bin
## C3 packages.
source "/home/tester004/load-local-spack.sh"
source "/home/tester004/gesanche/hercules/stuff/c3-spack-modules.sh"
module load hpcx

echo "Running $NUMBER_OF_PROCESS processes with $PROCESS_PER_NODE processes per node, slurm nodes: $SLURM_NNODES"
# TRANSFER_SIZE=$((1024*1))
TRANSFER_SIZE=$FILE_SIZE_PER_CLIENT


COMMAND="$IOR_PATH/ior -w -r -W -R -t ${TRANSFER_SIZE}kb -b ${FILE_SIZE_PER_CLIENT}kb -s 1 -i 5"

if [ "$IOR_FILE_PER_PROCESS" -eq 1 ]; then
## File-per-process with write and read verification.
#COMMAND="$IOR_PATH/ior -w -r -k -e -t ${TRANSFER_SIZE}kb -b ${FILE_SIZE_PER_CLIENT}kb -s 1 -i 5 -F -o /lustre/scratch/javier.garciablas/ior_output/data.txt"
COMMAND="$COMMAND -F"
#else
## Single-shared-file with write and read verification.
#COMMAND="$IOR_PATH/ior -w -r -k -e -t ${TRANSFER_SIZE}kb -b ${FILE_SIZE_PER_CLIENT}kb -s 1 -i 5 -o /lustre/scratch/javier.garciablas/ior_output/data.txt"
fi

if [ "$IOR_AVOID_CACHE" -eq 1 ]; then
## -C to reorder Tasks and -e to work around the effects of the page cache by using fsync.
COMMAND="$COMMAND -C -e"
fi

##  Add the output file path.
COMMAND="$COMMAND -o ${LUSTRE_PATH}/data.txt"
#COMMAND="$COMMAND -o /lustre/tester004/ior_output/data.txt"

set -x

mpiexec -np $NUMBER_OF_PROCESS -npernode $PROCESS_PER_NODE  $COMMAND

set +x

rm ${LUSTRE_PATH}/*
