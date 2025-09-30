import functools
import numpy as np
import xarray as xr
import fsspec
import os
import shutil
import dask.bag as db
from glob import glob

from gogoesgone import processing as pr
from gogoesgone import zarr_access as za

if __name__ == "__main__":
    channel = 13
    product = "ABI-L2-CMIPF"
    satellite = "goes16"
    timestamp = [
        "20210202 12:02:12",
        "20210122 11:43:46",
        "20210202 18:02:12",
        "20210122 19:43:46",
    ]  # time format in "%Y%m%d %H:%M:%S"
    extent = (-70, -20, -10, 30)
    image_dir = f"/net/labdata/geet/Data/GOES-CMIP-C{channel}-{extent}/images/"

    if not os.path.exists(image_dir):
        os.makedirs(image_dir)

    flist = []

    for time in timestamp:
        flist.append(za.nearest_time_url(time))

    bag = db.from_sequence(flist, npartitions=72).map(
        functools.partial(
            pr.create_image,
            image_dir=image_dir,
            extent=extent,
        ),
    )
    bag.compute()