#!/bin/bash
#SBATCH --job-name=lustre-io500    # Job name
#SBATCH --output=logs/lustre/%j.log   # Standard output and error log
#SBATCH --time=24:00:00               # Time limit hrs:min:sec
#SBATCH --cpus-per-task=64
#SBATCH --partition=large
#SBATCH --exclude=srvgpu[01-05],srv103

##SBATCH --nodefile=/home/tester004/gesanche/hercules/tmp/hostfile_3419


## I am passing a nodefile to use the same nodes for all tests, because some 
## nodes could have less resources.


TOTAL_NUMBER_OF_PROCESSES=$1
PROCESSES_PER_NODE=$2
IO500_CONFFILE=$3

echo "------------------"
echo "Deployment script content:"
echo "------------------"
cat c3_LUSTRE_IO500.sh
echo "------------------"
echo "IO500 configuration file content:"
echo "------------------"
cat "${IO500_CONFFILE}"

## Uncomment when working in C3.
IOR_PATH=/home/tester004/gesanche/io500/
#spack unload ucx openmpi
## C3 packages.
source "/home/tester004/load-local-spack.sh"
source "/home/tester004/gesanche/hercules/Stuff/c3-spack-modules.sh"
module load hpcx

#exit 0

# Binary.
COMMAND="$IOR_PATH/io500"

# Configuration file for io500.
COMMAND="$COMMAND ${IO500_CONFFILE}"

set -x
mpiexec --oversubscribe -np ${TOTAL_NUMBER_OF_PROCESSES} -npernode ${PROCESSES_PER_NODE} \
   ${COMMAND}

echo "done!"
exit 0
