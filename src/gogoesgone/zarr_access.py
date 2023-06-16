from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr
import datetime
import dask
import dask.bag as db
from dask.distributed import Client
import multiprocessing
import fsspec
import numpy as np

def generate_globsearch_string(
    year, dayofyear, hour=None, channel=13, product="ABI-L2-CMIPF", satellite="goes16"
):
    """returns string for glob search in AWS

    if hour is not provided, it will download for all files in the given day
    """
    
    if hour is None:
        return f"s3://noaa-{satellite}/{product}/{year}/{str(dayofyear).zfill(3)}/*/*C{str(channel).zfill(2)}*.nc"
    else:
        return f"s3://noaa-{satellite}/{product}/{year}/{str(dayofyear).zfill(3)}/{str(hour).zfill(2)}/*C{str(channel).zfill(2)}*.nc"


def generate_url_list(globsearch_string):
    """Returns available URLs' list for AWS filepaths"""
    fs = fsspec.filesystem("s3", anon=True)
    flist = []
    for f in fs.glob(globsearch_string):
        if f:
            flist.append("s3://" + f)

    if not flist:
        print("No files found!")
    else:
        return flist


def nearest_time_url(time, format="%Y%m%d %H:%M:%S", channel=13, product="ABI-L2-CMIPF", satellite="goes16"):
    """Returns URL of file with nearest observation starting time to provided time

    Accuracy only to the nearest second

    Searched times are in UTC, provided should also be UTC
    Function doesn't know timezones
    """

    dt_given = datetime.datetime.strptime(time, format)
    pre_dt_given = dt_given - datetime.timedelta(hours=1)
    post_dt_given = dt_given + datetime.timedelta(hours=1)

    url_list_hours = [
        generate_url_list(
            generate_globsearch_string(i.year, i.timetuple().tm_yday, i.hour, channel, product, satellite)
        )
        for i in [pre_dt_given, dt_given, post_dt_given]
    ]
    flist = [item for sublist in url_list_hours for item in sublist]

    dt_files = [
        datetime.datetime.strptime(
            f"{dt_given.year}" + i.split(f"_s{dt_given.year}")[1].split("_")[0][:-1],
            "%Y%j%H%M%S",
        )
        for i in flist
    ]

    nearest_time_string = min(dt_files, key=lambda d: abs(d - dt_given)).strftime(
        "%Y%j%H%M%S"
    )

    return flist[
        [i for i, x in enumerate([nearest_time_string in i for i in flist]) if x][0]
    ]


def get_days_between_dates(start_date, end_date):
    
    '''
    Get a list of days of the year between two dates.
    '''
    
    date_list = []
    current_date = start_date

    while current_date <= end_date:
        day_of_year = current_date.timetuple().tm_yday
        date_list.append((current_date, day_of_year))
        current_date += datetime.timedelta(days=1)

    return date_list


def periods_url(time_period, extent=(-62,-48,10,20), format="%Y%m%d %H:%M:%S", channel=13, product="ABI-L2-CMIPF", satellite="goes16", all_hours=True):
    
    '''
    Returns a list of URLs for the images contained in the specified time period.
    '''
    
    start = datetime.datetime.strptime(time_period[0], format)  
    end = datetime.datetime.strptime(time_period[1], format)
    
    if all_hours == True:
        hour = None
    
    print(f'Collecting urls from {start} to {end}')
    
    # Handle year transition
    if start.year != end.year:
        start_of_next_year = datetime.datetime(start.year + 1, 1, 1) - datetime.timedelta(days=1)

        days_list = get_days_between_dates(start, start_of_next_year)
        days_list += get_days_between_dates(datetime.datetime(end.year, 1, 1), end)
    else:
        days_list = get_days_between_dates(start, end)        
    
    # Get url in glob format for the period     
    flist = []
    for date, day_number in days_list:
        print(date.year, day_number, channel, product, satellite)
        
        gs = generate_globsearch_string(date.year, day_number, hour, channel, product, satellite)
        flist = generate_url_list(gs)
        
    return(flist)


def generate_references(f):
    with fsspec.open(f, mode="rb", anon=True) as infile:
        return SingleHdf5ToZarr(infile, f, inline_threshold=300).translate()


def get_mzz_from_references(flist, save=False, save_file="./combined.json"):
    """Provide dict of references and optionally save them as json"

    Parameters
    ----------
    flist : List of references (list of paths to JSON)
        JSON paths option is yet to be tested
    """

    if isinstance(flist, str):
        flist = [flist]
    elif isinstance(flist, list):
        pass
    else:
        return print("Please pass flist as either list or string")

    bag = db.from_sequence(flist, npartitions=64).map(generate_references)

    dicts = bag.compute()
    mzz = MultiZarrToZarr(
        dicts,
        remote_protocol="s3",
        remote_options={"anon": True},
        concat_dims="t",
        inline_threshold=0,
    )

    if save:
        return mzz.translate(save_file)
    else:
        return mzz.translate()


def get_mapper_from_mzz(flist):
    fs = fsspec.filesystem(
        "reference",
        fo=get_mzz_from_references(flist),
        remote_protocol="s3",
        remote_options={"anon": True},
        skip_instance_cache=True,
    )
    return fs.get_mapper("")
