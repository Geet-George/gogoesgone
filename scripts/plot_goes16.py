import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from mpl_toolkits.axes_grid1 import make_axes_locatable
import numpy as np
import xarray as xr
import argparse
import pandas as pd
from datetime import datetime, date, timedelta
import resource
import os
import time
import yaml
import eurec4a
from collections import defaultdict
from functools import reduce


def main():

    """
    Plot satellite images, optionally with HALO trajectories. Using code from HALO-DROPS.
    """
    
    parser = argparse.ArgumentParser()
    parser.add_argument("date")
    args = parser.parse_args()
    
    print(f"Plotting satellite images for {args.date}")

    channel = 2
    product = "ABI-L2-CMIPF"
    satellite = "goes16"
    extent = (-60,-50,11,16)
    
    starttime = time.time()
    print("Processing started ", datetime.now())

    # Plotting parameters
    if channel == 2:
        cmin = 0
        cmax = 0.2
        use_cmap = "Greys_r"
        save_path = "/scratch/m/m300931/data/GOES-16/snapshots/channel_2/"

    elif channel == 13:
        cmin = 280
        cmax = 297
        use_cmap = "Greys"
        save_path = "/scratch/m/m300931/data/GOES-16/snapshots/channel_13/"
        
    # Load subset data to plot
    subset_filepath = f"/scratch/m/m300931/data/GOES-16/CMI/channel_{channel}/CMI_subset_{args.date}_{satellite}_{product}_{channel}.nc"
    subset = xr.open_dataset(subset_filepath)

    for i, t in enumerate(subset.t):
        
        print(t.values)
        
        save_file = f"{satellite}_{product}_ch{channel}_{np.datetime_as_string(t.values, unit='s')}.png"
        
        # Check if file already exists
        if os.path.isfile(save_path+save_file):
            print("File already exists:", save_file)
            continue

        fig = plt.figure(figsize=(8,5))
        ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
        
        # Plot satellite image
        im_sat = subset.CMI.isel(t=i).plot(ax=ax,x='lon',y='lat',cmap=use_cmap,add_colorbar=False,vmin=cmin,vmax=cmax)
        ax.set_extent(extent, crs=ccrs.PlateCarree())
        ax.coastlines(resolution='10m', color='red', linewidth=1)

        plt.title(t.values)
        plt.show()

        plt.savefig(save_path+save_file, dpi=300, bbox_inches="tight")
        plt.close()

    # Check time and memory urage
    print("time:", (time.time() - starttime)/60, "min")
    print("memory usage:", resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, "Kb")
        
if __name__ == "__main__":
    main()