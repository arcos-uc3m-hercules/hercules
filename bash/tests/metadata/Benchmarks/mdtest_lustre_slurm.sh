#!/bin/bash
#SBATCH --job-name=ior_g    # Job name
#SBATCH --output=logs/lustre/%j_mdtest.log   # Standard output and error log
#SBATCH --time=01:00:00               # Time limit hrs:min:sec
#SBATCH --cpus-per-task=32

NUMBER_OF_FILES_PER_PROCESS=$1
NUMBER_OF_PROCESS=$2
PROCESS_PER_NODE=$3

## Uncomment when working in Unito.
## MDTEST is part of the IOR package.
MDTEST_PATH=/beegfs/home/javier.garciablas/gsanchez/ior/bin
# spack unload mpich openmpi
# spack load mpich@3.2.1%gcc@=9.4.0 arch=linux-ubuntu20.04-zen
spack load mpich
# whereis mpiexec

echo "Running $NUMBER_OF_PROCESS processes with $PROCESS_PER_NODE processes per node, slurm nodes: $SLURM_NNODES"


# -n to set the number of files/directories per process.
## Note that 100,000 files/directories is probably the minimum value that will deliver a meaningful result (such that MDS cacheing does not affect results).
## See more: https://wiki.lustre.org/MDTest
#  every process will creat/stat/read/remove # directories and files
COMMAND="$MDTEST_PATH/mdtest -n ${NUMBER_OF_FILES_PER_PROCESS}"
# -I number of items per directory in tree
#COMMAND="$MDTEST_PATH/mdtest -I ${number_of_files_per_process}"
# -C only create files/dirs
# COMMAND="$COMMAND -C"

# Number of iterations.
COMMAND="$COMMAND -i 5"

# Verbose
COMMAND="$COMMAND -V 1"

## Add the working directory.
## The -u flag tells the program to assign a unique working directory per task.
COMMAND="$COMMAND -u -d /lustre/scratch/javier.garciablas/mdtest_output/"

set -x

mpiexec -np $NUMBER_OF_PROCESS -ppn $PROCESS_PER_NODE  $COMMAND

set +x

rm /lustre/scratch/javier.garciablas/mdtest_output/*

