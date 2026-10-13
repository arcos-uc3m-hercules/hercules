#!/bin/bash
## Last modification: 23/09/2026
## Script to load dependencies required by HERCULES 
## on the C3 cluster.
module load hpcx
spack load pcre@8.45/jeglz37
spack load glib@2.86.1/dyhoo7u
spack load cmake@3.31.5/bbzu7or
spack load ucx@1.18.0/3wclteq
export LD_LIBRARY_PATH=~/spack/opt/spack/linux-zen3/ucx-1.18.0-3wclteq3uiji63cv5g6cxwov4eanaw3h/lib:$LD_LIBRARY_PATH
echo "+ done"
# exit 0
