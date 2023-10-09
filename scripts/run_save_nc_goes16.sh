#!/bin/bash
#SBATCH --partition=compute
#SBATCH --account=mh0010
#SBATCH --job-name=goes_data
#SBATCH --time=05:00:00
#SBATCH --output=log/%x.%j.log
#SBATCH --error=log/%x.%j.log
#SBATCH --mail-user=nina.robbins@mpimet.mpg.de

. ~/.bashrc
mamba activate halodrops_env

date_file="$1"

# Get the number of CPU cores
#NUM_CORES=$(nproc)
NUM_CORES=4

# Use xargs to run the Python script in parallel for each argument from the input file
cat "$date_file" | xargs -n 1 -P "$NUM_CORES" python -u save_nc_goes16.py > log/save_nc_goes16.txt

mamba deactivate
