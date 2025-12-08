#!/bin/bash
#SBATCH --job-name=build    # Job name
#SBATCH --time=01:00:00               # Time limit hrs:min:sec
#SBATCH --output=logs/build/%j-io500.log   # Standard output and error log
#SBATCH -N 1
#SBATCH --cpus-per-task=32
#SBATCH --exclusive

module load hpcx
spack load autoconf
#spack load gcc
spack load binutils/ibfyczh
# Note: if lz4 fails, go to ../io500/build/pfind/compile.sh and change:
# FILES+=./lz4/lib/*.o to FILES+=./lz4/lib/*.a
# Expected error:
#Building parallel find;                                                                                                                                                   Using LZ4 for optimization                                                                                                                                                ar: ./lz4/lib/*.o: No such file or directory
./prepare.sh
make clean
make

exit 0
