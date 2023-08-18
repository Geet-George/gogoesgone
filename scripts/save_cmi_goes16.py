import sys
sys.path.append('..')

import numpy as np
import xarray as xr
import fsspec
import argparse
import time
import os
import resource
from datetime import datetime

from src.gogoesgone import processing as pr

def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("date")
    args = parser.parse_args()
    print(args.date)
    
     # Satellite specifications
    channel = 2
    satellite = "goes16"
    product="ABI-L2-CMIPF"
    
    mapper_path = f"/scratch/m/m300931/mappers/GOES-16/channel_{channel}/"
    save_path = f"/scratch/m/m300931/data/GOES-16/CMI/channel_{channel}/"
    filename = f"CMI_subset_{args.date}_{satellite}_{product}_{channel}.nc"
    
    if os.path.isfile(save_path+filename):
        print("File already exists:", filename)
        return
        
    starttime = time.time()
    print("Processing started ", datetime.now())

    class Image(pr.Image):
        def __init__(self, dataset):
            self.dataset = dataset
            self.r_eq = self.dataset["goes_imager_projection"].attrs["semi_major_axis"]
            self.inv_f = self.dataset["goes_imager_projection"].attrs["inverse_flattening"]
            self.r_pol = self.dataset["goes_imager_projection"].attrs["semi_minor_axis"]
            self.e = np.sqrt((self.r_eq**2 - self.r_pol**2) / self.r_eq**2)
            self.sat_height_above_ellipsoid = self.dataset["goes_imager_projection"].attrs[
                "perspective_point_height"
            ]
            self.H = self.r_eq + self.sat_height_above_ellipsoid
            self.lambda_0 = np.deg2rad(
                self.dataset["goes_imager_projection"].attrs[
                    "longitude_of_projection_origin"
                ]
            )

    # Access mapper from daily json files
    m = fsspec.get_mapper(f"reference::{mapper_path}{args.date}_{satellite}_{product}_{channel}.json",
                          remote_protocol="s3", remote_options={"anon": True})

    # Open entire dataset
    ds = xr.open_dataset(m, chunks="auto", engine="zarr", consolidated=False)

    # Create image object (re-structutre coordinates in lon/lat)
    img = Image(ds)

    # Select spatial extent
    extent = (-60,-50,11,16)
    subset = img.subset_region_from_latlon_extents(extent, unit="degree")

    # Select CMI variable and load data
    cmi = subset.CMI.load()

    # Save to daily netcdf files
    cmi.to_netcdf(save_path+filename)
    
    # Check time and memory urage
    print("time:", (time.time() - starttime)/60, "min")
    print("memory usage:", resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, "Kb")
    
if __name__ == "__main__":
    main()