import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from mpl_toolkits.axes_grid1 import make_axes_locatable
import numpy as np
import seaborn as sb
from tqdm import tqdm
import xarray as xr

from gogoesgone import processing as pr
from gogoesgone import zarr_access as za

hour = 17
year = 2020
dayofyear = 24
channel = 13
product = "ABI-L2-CMIPF"  #'ABI-L1b-RadF'
satellite = "goes16"  #'goes17'


def main():
    gs = za.generate_globsearch_string(year, dayofyear, hour)
    flist = za.generate_url_list(gs)
    m = za.get_mapper_from_mzz(flist)

    img = pr.Image(m)
    extent = (-62, -48, 10, 20)

    subset = img.subset_region_from_latlon_extents(extent, unit="degree")

    sb.set_context("paper")

    for t in tqdm(subset.t):
        fig = plt.figure(figsize=(8, 5))

        ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())

        im = subset.CMI.sel(t=t).plot(
            ax=ax,
            x="lon",
            y="lat",
            cmap="cubehelix_r",
            add_colorbar=False,
            vmin=280,
            vmax=300,
        )
        subset.CMI.sel(t=t).plot.contour(
            ax=ax,
            x="lon",
            y="lat",
            cmap="Reds",
            add_colorbar=False,
            levels=[
                290,
            ],
            linewidths=0.5,
        )
        ax.set_extent(extent, crs=ccrs.PlateCarree())
        ax.coastlines(resolution="10m", color="black", linewidth=0.5)

        time_str = str(t.values).rsplit(":", 1)[0].replace(":", "h")
        plt.title(time_str)
        plt.colorbar(im, orientation="horizontal")

        gl = ax.gridlines(
            ccrs.PlateCarree(),
            linewidth=1,
            color="w",
            alpha=0.5,
            linestyle=":",
            draw_labels=True,
        )
        gl.top_labels = False
        gl.right_labels = False
        gl.xlines = True
        gl.ylines = True
        gl.xlocator = mticker.FixedLocator(np.arange(-180, 180, 2.5))
        gl.ylocator = mticker.FixedLocator(np.arange(-90, 90, 2.5))
        gl.xformatter = LONGITUDE_FORMATTER
        gl.yformatter = LATITUDE_FORMATTER
        gl.xlabel_style = {"color": "grey"}
        gl.ylabel_style = {"color": "grey"}
        plt.savefig(
            f"notebooks/Plots/{satellite}_{product}_{channel}_{time_str}_{extent}.png",
            dpi=300,
        )
        plt.close()


if __name__ == "__main__":
    main()
