#!/bin/bash
#SBATCH --partition=compute
#SBATCH --account=mh0010
#SBATCH --job-name=goes_mapper
#SBATCH --time=08:00:00
#SBATCH --output=log/%x.%j.log
#SBATCH --error=log/%x.%j.log
#SBATCH --mail-user=nina.robbins@mpimet.mpg.de

. ~/.bashrc
mamba activate halodrops_env

chmod +x mapper_goes16.py

date_file="$1"

# Get the number of CPU cores
#NUM_CORES=$(nproc)
#--cpus-per-task=16
NUM_CORES=128

# Use xargs to run the Python script in parallel for each argument from the input file
cat "$date_file" | xargs -n 1 -P "$NUM_CORES" python -u mapper_goes16.py > log/mapper_goes16.txt

mamba deactivate
