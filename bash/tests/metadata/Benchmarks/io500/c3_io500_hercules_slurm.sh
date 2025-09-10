#!/bin/bash
#SBATCH --job-name=hercules    # Job name
#SBATCH --output=logs/hercules/%j-io500.log   # Standard output and error log
#SBATCH --time=8:00:00               # Time limit hrs:min:sec
#SBATCH --cpus-per-task=36
#SBATCH --partition=large
#SBATCH --exclude=srvgpu[01-05],srv103,srv108
#SBATCH --nodefile=/home/tester004/gesanche/hercules/tmp/hostfile_3419

## I am passing a nodefile to use the same nodes for all tests, because some 
## nodes could have less resources.


##SBATCH --overcommit
##SBATCH --hint=compute_bound
##SBATCH --mem=100G
##SBATCH --mem-per-cpu=16GB
##SBATCH --oversubscribe
###SBATCH --oversubscribe
##SBATCH --exclude=broadwell-008,broadwell-010
###SBATCH --nodelist=broadwell-[012-027]
###SBATCH --exclusive=user



CONFIG_PATH=$1
NUMBER_OF_PROCESS=$2
IO500_CONFFILE=$3


## Print the deployment script and the configuration file
echo "------------------"
echo "Deployment script content:"
echo "------------------"
cat c3_HERCULES_IO500.sh
echo "------------------"
echo "Configuration file content:"
echo "------------------"
cat "${CONFIG_PATH}"
echo "------------------"
echo "IO500 configuration file content:"
echo "------------------"
cat "${IO500_CONFFILE}"

## Uncomment when working in Tucan.
# IOR_PATH=/home/software/io500/bin
# module unload mpi
# module load mpi/mpich3/3.2.1

## Uncomment when working in Unito.
#IOR_PATH=/beegfs/home/javier.garciablas/io500/bin
#IOR_PATH=/beegfs/home/javier.garciablas/gsanchez/ior/bin
#IOR_PATH=/home/tester004/gesanche/ior/bin/
#spack load mpich@3.2.1%gcc@=9.4.0
#spack load openmpi@4.1.5
#spack unload mpich openmpi
#spack load openmpi@4.1.5%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell
#spack load mpich@3.2.1%gcc@=9.4.0 arch=linux-ubuntu20.04-zen

## Uncomment when working in C3.
IOR_PATH=/home/tester004/gesanche/io500/
#spack unload ucx openmpi
## C3 packages.
#spack load pcre@8.45/jeglz37
#spack load mpich@4.2.3/gercqqr
#spack load ucx/altjaeg
#spack load glib@2.78.3/4c7p6mc
#spack load cmake@3.31.5/bbzu7or
source "/home/tester004/load-local-spack.sh"
source "/home/tester004/gesanche/hercules/Stuff/c3-spack-modules.sh"
module load hpcx
#whereis mpiexec
# spack load \
#    cmake@3.24.3%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#    glib@2.74.1%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#    ucx@1.14.0%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#    pcre@8.45%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#    openmpi@4.1.5%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#    jemalloc

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
#export UCX_TLS=rc_v,rc_verbs
#mm,posix,rc,rc_v,rc_verbs,self,shm,sm,sysv,tcp,ud,ud_v,ud_verbs,cma
#export UCX_RNDV_SCHEME=put_zcopy
# export UCX_NET_DEVICES=ibs1 # Slow!
# spack load /sxjvb77
##export UCX_TLS=ib
#set -x
#export UCX_NET_DEVICES="opap6s0:1"
#export UCX_NET_DEVICES=all
#export UCX_IB_RCACHE_MAX_REGIONS="100"
export UCX_NET_DEVICES="ib0"
#export UCX_NET_DEVICES="mlx5_0"

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

# exit 0
echo "Running clients"

# Binary.
COMMAND="$IOR_PATH/io500"

# Configuration file for io500.
COMMAND="$COMMAND ${IO500_CONFFILE}"

set -x
#sleep 10
#time 
#HERCULES_CONF=$HERCULES_CONF LD_PRELOAD=$HERCULES_POSIX_PRELOAD ${COMMAND}
mpiexec $HERCULES_MPI_NP$HERCULES_NNFC $HERCULES_MPI_PPN$HERCULES_NCPN  $HERCULES_MPI_HOSTFILE_DEF$HERCULES_MPI_HOSTFILE_NAME \
   $HERCULES_MPI_ENV_DEF HERCULES_CONF=$HERCULES_CONF \
   $HERCULES_MPI_ENV_DEF LD_PRELOAD=$HERCULES_POSIX_PRELOAD \
   $COMMAND

echo "done!"
exit 0
