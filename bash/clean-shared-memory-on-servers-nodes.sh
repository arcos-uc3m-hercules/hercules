#!/bin/bash
#SBATCH --job-name=memory    # Job name
#SBATCH --time=00:00:30               # Time limit hrs:min:sec
#SBATCH --output=logs/shared-memory/%j_memory_cleaning.log   # Standard output and error log

### NOTE ###
## Command to run this scrip:
## sbatch --nodelist=./data_hostfile clean-shared-memory-on-servers-nodes.sh
############

spack load mpich

mpiexec -f=./data_hostfile \
	        ipcrm -a

echo "done"
