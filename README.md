# Go-GOES-Gone

Some scripts to help work with GOES data without downloading locally. It uses the NC metadata on the server and makes mappers to access the data in the Zarr format, thanks very much to the [kerchunk](https://fsspec.github.io/kerchunk/) library. :)

## Installation

For now, simply clone this repository (from the `main` branch) by writing the following command in whichever directory you want the repo to lie:

`git clone git@github.com:Geet-George/gogoesgone.git`

Enter the directory by:

`cd gogoesgone`

Make sure you are in the environment where you want the package to be installed. This environment should have Python version >= 3.10. Now, simply install with pip:

`pip install .`

Have fun! :)

If these steps don't seem to work, please [raise an issue](https://github.com/Geet-George/gogoesgone/issues/new).

To see an example of you can get started, check out [this notebook](notebooks/how_to_access_via_zarr.ipynb).
