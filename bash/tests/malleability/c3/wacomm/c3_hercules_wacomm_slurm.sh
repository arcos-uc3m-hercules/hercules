#!/bin/bash
#SBATCH --job-name=hercules    # Job name
#SBATCH --output=logs/hercules/%j-wacomm.log   # Standard output and error log
#SBATCH --time=3:00:00               # Time limit hrs:min:sec
#SBATCH --cpus-per-task=1
#SBATCH --partition=large
#SBATCH --ntasks-per-node=100 # fix the mpirun error: "There are not enough slots available in the system.."
#SBATCH --exclude=srvgpu[01-05],srv103,srv108
##SBATCH --nodelist=srv[101-108]
##SBATCH --nodefile=/home/tester004/gesanche/hercules/tmp/hostfile_3419


## I am passing a nodefile to use the same nodes for all tests, because some 
## nodes could have less resources.

CONFIG_PATH=$1
NUMBER_OF_PROCESS=$2

# ---------------------------------------------------------------
## Print the deployment script and the configuration file
echo "------------------"
echo "Deployment script content:"
echo "------------------"
cat ${PARENT_SCRIPT}
echo "------------------"
echo "Configuration file content:"
echo "------------------"
cat "${CONFIG_PATH}"
echo "------------------"

## Uncomment when working in C3.
## C3 packages.
echo "Running ${HERCULES_PATH}/scripts/c3/c3-load-dependencies.sh"
source "${HERCULES_PATH}/scripts/c3/c3-load-dependencies.sh"

export UCX_NET_DEVICES="ib0"

echo "Starting Hercules with $POLICY policy"
start_=$(date +%s.%N)
if [ -z "$CONFIG_PATH" ]; then
   echo "No configuration file"
   source hercules start
   # Get the exit status of the hercules script.
   STATUS=$?
else
   echo "Configuration file: $CONFIG_PATH"
#   export HERCULES_DEBUG_LEVEL=none
   source ${HERCULES_PATH}/scripts/hercules start \
   -f "$CONFIG_PATH" 
   # Get the exit status of the hercules script.
   STATUS=$?
   unset HERCULES_DEBUG_LEVEL
fi
end_=$(date +%s.%N)
runtime=$(echo "$end_ - $start_" | bc -l)

if [ $STATUS -eq 0 ]; then
   echo "Hercules started in $runtime seconds, start=$start_, end=$end_"
else
   echo "Error running Hercules, exit code: $STATUS"
   exit $STATUS
fi

echo "DATASERVERS $HERCULES_NUM_DATA"
echo "THREADS $THREAD_POOL"
echo "Running clients"
# ---------------------------------------------------------------

##  Add the program.
COMMAND="${HOME_LUSTRE}/apps/wacomm-kernel/m_wacomm1_mpi/m_wacomm1_mpi-disk ${NUMBER_OF_PARTICLES}"

## Output directory.
#export PARTICLE_OUTPUT_DIR=/lustre/tester004/apps/wacomm-kernel/m_wacomm1_mpi/output-files/
export PARTICLE_OUTPUT_DIR=/mnt/hercules/

set -x

#HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD ${COMMAND}
# --oversubscribe
time mpiexec --map-by node  $HERCULES_MPI_NP$NUMBER_OF_PROCESS  $HERCULES_MPI_HOSTFILE_DEF$HERCULES_MPI_HOSTFILE_NAME \
   $HERCULES_MPI_ENV_DEF HERCULES_CONF=$HERCULES_CONF \
   $HERCULES_MPI_ENV_DEF LD_PRELOAD=$HERCULES_POSIX_PRELOAD \
   $COMMAND

#LD_PRELOAD=$HERCULES_POSIX_PRELOAD HERCULES_CONF=$HERCULES_CONF ls -lh /mnt/hercules/

#LD_PRELOAD=$HERCULES_POSIX_PRELOAD HERCULES_CONF=$HERCULES_CONF du -sh /mnt/hercules/
## print the total size in kilobytes.
#LD_PRELOAD=$HERCULES_POSIX_PRELOAD HERCULES_CONF=$HERCULES_CONF du -s /mnt/hercules/
#LD_PRELOAD=$HERCULES_POSIX_PRELOAD HERCULES_CONF=$HERCULES_CONF cp /mnt/hercules/data.out ./data-test.out

echo "done!"
exit 0
