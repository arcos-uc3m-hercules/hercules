#!/bin/bash
#SBATCH --job-name=memory    # Job name
#SBATCH --time=00:00:30               # Time limit hrs:min:sec
#SBATCH --output=logs/shared-memory/%j_memory_checking.log   # Standard output and error log

### NOTE ###
## Command to run this scrip:
## sbatch --nodelist=./data_hostfile check-shared-memory-on-servers-nodes.sh
############

spack load mpich
set -x
#mpiexec -ppn=1 -f=./data_hostfile \
mpiexec -ppn=1 \
	bash -c "(hostname && ipcs -a) | paste -sd ' '"

set +x

echo "done"
