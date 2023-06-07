import os

import numpy as np
import xarray as xr


class Image:
    """Class to read and process images"""

    def __init__(self, filepath):
        self.filepath = filepath
        self.filename = os.path.basename(filepath)
        self.dataset = xr.open_dataset(self.filepath)
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

    def geocentric_latitude(self, geodetic_latitude):
        return np.arctan(
            ((self.r_pol**2) / (self.r_eq**2)) * np.tan(geodetic_latitude)
        )

    def geocentric_distance_to_point_on_ellipsoid(self, geocentric_latitude):
        return self.r_pol / (
            np.sqrt(1 - ((self.e**2) * (np.cos(geocentric_latitude) ** 2)))
        )

    def s_xyz_from_latlon_to_xy(self, geodetic_latitude, geodetic_longitude):
        geocentric_latitude = self.geocentric_latitude(geodetic_latitude)
        r_c = self.geocentric_distance_to_point_on_ellipsoid(geocentric_latitude)

        return (
            self.s_x_from_latlon_to_xy(r_c, geocentric_latitude, geodetic_longitude),
            self.s_y_from_latlon_to_xy(r_c, geocentric_latitude, geodetic_longitude),
            self.s_z_from_latlon_to_xy(r_c, geocentric_latitude),
        )

    def s_x_from_latlon_to_xy(self, r_c, geocentric_latitude, geodetic_longitude):
        return self.H - r_c * np.cos(geocentric_latitude) * np.cos(
            geodetic_longitude - self.lambda_0
        )

    def s_y_from_latlon_to_xy(self, r_c, geocentric_latitude, geodetic_longitude):
        return (
            -r_c
            * np.cos(geocentric_latitude)
            * np.sin(geodetic_longitude - self.lambda_0)
        )

    def s_z_from_latlon_to_xy(self, r_c, geocentric_latitude):
        return r_c * np.sin(geocentric_latitude)

    def check_point_visible_for_satellite(
        self, geodetic_latitude, geodetic_longitude, return_s_xyz=False
    ):
        s_x, s_y, s_z = self.s_xyz_from_latlon_to_xy(
            geodetic_latitude, geodetic_longitude
        )

        lhs = self.H * (self.H - s_x)
        rhs = s_y**2 + ((self.r_eq**2) / (self.r_pol**2)) * (s_z**2)

        if lhs < rhs:
            i = False
        else:
            i = True

        if return_s_xyz:
            return i, s_x, s_y, s_z
        else:
            return i

    def latlon_to_xy(self, lat, lon, unit="degree"):
        if unit == "degree":
            geodetic_latitude, geodetic_longitude = np.deg2rad(lat), np.deg2rad(lon)
        elif unit == "radian":
            geodetic_latitude, geodetic_longitude = lat, lon
        else:
            return print("Unit provided should be either degree or radian")

        i, s_x, s_y, s_z = self.check_point_visible_for_satellite(
            geodetic_latitude, geodetic_longitude, return_s_xyz=True
        )

        if i:
            y = np.arctan(s_z / s_x)
            x = np.arcsin((-s_y) / (np.sqrt(s_x**2 + s_y**2 + s_z**2)))

            return y, x

        else:
            return print(
                f"Provided coordinates, {lat=}, {lon=} do not lie within the satellite's visibility range."
            )

    def subset_region_from_latlon_extents(self, extents, unit="degree"):
        w_extent, e_extent, s_extent, n_extent = extents

        try:
            ne_y, ne_x = self.latlon_to_xy(n_extent, e_extent, unit=unit)
            nw_y, nw_x = self.latlon_to_xy(n_extent, w_extent, unit=unit)
            se_y, se_x = self.latlon_to_xy(s_extent, e_extent, unit=unit)
            sw_y, sw_x = self.latlon_to_xy(s_extent, w_extent, unit=unit)

            return self.dataset.sel(y=slice(ne_y, sw_y)).sel(x=slice(sw_x, ne_x))

        except:
            return print(
                "Some exception encountered. Check if all extents provided are within satellite's visibility."
            )
