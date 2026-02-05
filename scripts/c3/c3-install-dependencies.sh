#!/bin/bash
## Last modification: 31/01/2026
## Script to install with Spack the missing dependencies required by HERCULES 
## on the C3 cluster. UCX, cmake and mpi are already installed; if not you can 
## modify this script according to your needs.
spack install glib@2.86.1
echo "+ done"
exit 0