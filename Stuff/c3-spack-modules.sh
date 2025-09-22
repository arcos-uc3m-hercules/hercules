#!/bin/bash
#NOTE: To work in interactive mode:
# salloc -N4 --time=08:00:00 --job-name=hercules --exclusive --partition=large -A uc3m_a0-sciot
spack load pcre@8.45/jeglz37
#spack load mpich@4.2.3/gercqqr
#spack load mpich@4.2.3/kn7bnmv 
#spack load ucx/altjaeg
spack load glib@2.78.3/4c7p6mc
spack load cmake@3.31.5/bbzu7or
