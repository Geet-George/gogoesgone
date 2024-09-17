#!/bin/bash
#SBATCH --partition=compute
#SBATCH --account=mh0010
#SBATCH --job-name=goes_plot
#SBATCH --nodes=1
#SBATCH --time=08:00:00
#SBATCH --mail-type=ALL
#SBATCH --mail-user=nina.robbins@mpimet.mpg.de
#SBATCH --output=log/%x.%j.log
#SBATCH --error=log/%x.%j.log

. ~/.bashrc
mamba activate halodrops_env

date_file="$1"

# Get the number of CPU cores
#NUM_CORES=$(nproc)
NUM_CORES=16

# Use xargs to run the Python script in parallel for each argument from the input file
cat "$date_file" | xargs -n 1 -P "$NUM_CORES" python -u plot_goes16.py > log/plot_goes16.txt

mamba deactivate