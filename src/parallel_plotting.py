# from functools import partial
# from multiprocessing import cpu_count
# from multiprocessing.pool import ThreadPool


# import cartopy.crs as ccrs
# import cartopy.feature as cfeature
# from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
# import xarray as xr
# import numpy as np
# import matplotlib.pyplot as plt
# import matplotlib.ticker as mticker
# from mpl_toolkits.axes_grid1 import make_axes_locatable
# import s3fs
# import datetime as dt

# plt.switch_backend("Agg")


# def create_plot(t, args_tuple):
#     print("R")
#     ds, extent, x, y, transform, projection = args_tuple
#     fig = plt.figure()

#     # Create axis with Geostationary projection
#     # ax = fig.add_subplot(1, 1, 1, projection=geostationary)
#     ax = fig.add_subplot(1, 1, 1, projection=projection)
#     ax.set_extent(extent, crs=ccrs.PlateCarree())

#     # Add the RGB True Color image to the figure. The data is in the same projection as the axis created
#     im = ax.imshow(
#         ds.sel(t=t).CMI,
#         origin="upper",
#         extent=(x.min(), x.max(), y.min(), y.max()),
#         transform=transform,
#         cmap="cubehelix_r",
#         vmin=185,
#         vmax=310,
#     )

#     # Add coastlines and states
#     ax.coastlines(resolution="10m", color="w", linewidth=0.25)

#     # Add title
#     plt.title(f"{str(t)}")
#     plt.colorbar(
#         im, orientation="horizontal", label="Channel 13 Brightness Temperature / K"
#     )

#     # # # Draw grid.
#     gl = ax.gridlines(
#         ccrs.PlateCarree(),
#         linewidth=0.5,
#         color="w",
#         alpha=0.5,
#         linestyle=":",
#         draw_labels=True,
#     )
#     gl.top_labels = False
#     gl.right_labels = False
#     gl.xlines = True
#     gl.ylines = True
#     gl.xlocator = mticker.FixedLocator(np.arange(-180, 180, 10))
#     gl.ylocator = mticker.FixedLocator(np.arange(-90, 90, 10))
#     gl.xformatter = LONGITUDE_FORMATTER
#     gl.yformatter = LATITUDE_FORMATTER
#     gl.xlabel_style = {"color": "grey"}
#     gl.ylabel_style = {"color": "grey"}

#     plt.savefig(f"Plots/{str(t)}.png", dpi=200, bbox_inches="tight")
#     plt.close()


# def create_plot_parallel(t_array, args_tuple):
#     cpus = cpu_count()
#     print(f"{cpus} CPUs available :)")
#     results = ThreadPool(cpus - 1).imap_unordered(
#         partial(create_plot, args_tuple=args_tuple), t_array
#     )

#     for result in results:
#         pass


# if __name__ == "__main__":
#     create_plot_parallel(t_array, args_tuple)
