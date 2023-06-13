from importlib import reload
import sys

sys.path.append("..")

from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr

import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from mpl_toolkits.axes_grid1 import make_axes_locatable
import s3fs
import datetime as dt

import logging
import fsspec
import ujson
from tqdm import tqdm
from glob import glob
import seaborn as sb

from src import processing as pr

reload(pr)

fs = fsspec.filesystem("s3", anon=True)
urls = []

for i in tqdm(range(210, 240)):
    for f in fs.glob(f"s3://noaa-goes16/ABI-L2-CMIPF/2021/{i}/*/*C13*.nc"):
        urls.append("s3://" + f)

import dask
from dask.distributed import Client

client = Client(n_workers=8)
client

import os


def gen_json(u):
    so = dict(mode="rb", anon=True, default_fill_cache=False, default_cache_type="none")
    with fsspec.open(u, **so) as inf:
        h5chunks = SingleHdf5ToZarr(inf, u, inline_threshold=300)
        if os.path.isfile("jsons/{u.split('/')[-1]}.json"):
            pass
        else:
            with open(f"jsons/{u.split('/')[-1]}.json", "wb") as outf:
                outf.write(ujson.dumps(h5chunks.translate()).encode())


import pathlib

pathlib.Path("./jsons/").mkdir(exist_ok=True)

dask.compute(*[dask.delayed(gen_json)(u) for u in urls])

json_list = sorted(glob("./jsons/*CMIPF*.json"))

m_list = []
for js in tqdm(json_list):
    with open(js) as f:
        m_list.append(
            fsspec.get_mapper(
                "reference://",
                fo=ujson.load(f),
                remote_protocol="s3",
                remote_options={"anon": True},
            )
        )

json_list = sorted(glob("./jsons/*.json"))

mzz = MultiZarrToZarr(
    json_list,
    remote_protocol="s3",
    remote_options={"anon": True},
    concat_dims="t",
    inline_threshold=0,
)

mzz.translate("./combined.json")

fs = fsspec.filesystem(
    "reference",
    fo="./combined.json",
    remote_protocol="s3",
    remote_options={"anon": True},
    skip_instance_cache=True,
)
m = fs.get_mapper("")

img = pr.Image(m)
extent_ds = (-65, -10, -8, 20)
extent = (-60, -20, -5, 15)

ds = img.subset_region_from_latlon_extents(extent_ds, unit="degree")

data = ds.metpy.parse_cf("CMI")
geostationary = data.metpy.cartopy_crs

pc = ccrs.PlateCarree()

# Sweep the ABI data from the x (north/south) and y (east/west) axes
x = data.x
y = data.y

# Use the geostationary projection to plot the image on a map
# This method streches the image across a map with the same projection and dimensions as the data
sb.set_context("paper")

for t in tqdm(ds.t):
    save_file = f"Plots/{t.values}.png"

    if os.path.isfile(save_file):
        pass
    else:
        fig = plt.figure()

        # Create axis with Geostationary projection
        ax = fig.add_subplot(1, 1, 1, projection=pc)
        ax.set_extent(extent, crs=pc)

        # Add the RGB True Color image to the figure. The data is in the same projection as the axis created
        im = ax.imshow(
            ds.sel(t=t).CMI,
            origin="upper",
            extent=(x.min(), x.max(), y.min(), y.max()),
            transform=geostationary,
            cmap="cubehelix_r",
            vmin=185,
            vmax=310,
        )
        # ax.set_extent(extent, crs=ccrs.PlateCarree())
        # Add coastlines and states
        ax.coastlines(resolution="10m", color="w", linewidth=0.25)

        # Add title
        plt.title(f"{t.values}")
        plt.colorbar(
            im, orientation="horizontal", label="Channel 13 Brightness Temperature / K"
        )

        # # # Draw grid.
        gl = ax.gridlines(
            ccrs.PlateCarree(),
            linewidth=0.5,
            color="w",
            alpha=0.5,
            linestyle=":",
            draw_labels=True,
        )
        gl.top_labels = False
        gl.right_labels = False
        gl.xlines = True
        gl.ylines = True
        gl.xlocator = mticker.FixedLocator(np.arange(-180, 180, 10))
        gl.ylocator = mticker.FixedLocator(np.arange(-90, 90, 10))
        gl.xformatter = LONGITUDE_FORMATTER
        gl.yformatter = LATITUDE_FORMATTER
        gl.xlabel_style = {"color": "grey"}
        gl.ylabel_style = {"color": "grey"}

        plt.savefig(save_file, dpi=250, bbox_inches="tight")
        plt.close()
