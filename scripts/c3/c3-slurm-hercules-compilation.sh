#!/bin/bash
#SBATCH --job-name=hercules-build    
#SBATCH --time=00:05:00               
#SBATCH --output=./tmp/%j-hercules-build.log  
#SBATCH -N 1
#SBATCH --cpus-per-task=32

echo "Workdir: ${PWD}"
source scripts/c3/c3-load-dependencies.sh
cmake --preset default
cmake --build --preset default --target install

# Important: run the following command before running an Hercules deployment.
# export LD_LIBRARY_PATH=${HOME}/hercules/install/lib:$LD_LIBRARY_PATH

echo "+ done"

exit 0
