#!/bin/bash
#SBATCH --job-name=hercules-build    # Job name
#SBATCH --time=00:20:00               # Time limit hrs:min:sec
#SBATCH --output=./tmp/%j-hercules-build.log   # Standard output and error log
#SBATCH -N 1
#SBATCH --cpus-per-task=32

module load hpcx
source Stuff/c3-spack-modules.sh
#spack load autoconf
#spack load gcc
#spack load binutils/ibfyczh
rm -rf ./build
mkdir build
cd build
cmake ..
make -j

echo "+ done"

exit 0