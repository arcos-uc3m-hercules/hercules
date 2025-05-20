#!/bin/bash
#SBATCH --job-name=hercules    # Job name
#SBATCH --exclude=broadwell-022

srun -N1 sleep 10
srun echo "Finishing"
