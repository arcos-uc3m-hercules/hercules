#!/bin/bash
## Last modification: 31/01/2026
## Script to load dependencies required by HERCULES 
## on the C3 cluster.
module load hpcx
spack load pcre@8.45/jeglz37
spack load glib@2.86.1/dyhoo7u
spack load cmake@3.31.5/bbzu7or
echo "+ done"
# exit 0