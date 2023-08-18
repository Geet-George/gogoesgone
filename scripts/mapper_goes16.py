import sys
sys.path.append('..')

from datetime import datetime
import time
import resource
import os
import argparse
from itertools import chain

from src.gogoesgone import processing as pr
from src.gogoesgone import zarr_access as za

# Satellite specifications
channel = 2
satellite = "goes16"
product="ABI-L2-CMIPF"
savepath = f"/scratch/m/m300931/mappers/GOES-16/channel_{channel}/"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("date")
    args = parser.parse_args()
    
    print(args.date)
    
    starttime = time.time()
    print("Processing started ", datetime.now())
    
    filename = savepath + f"{args.date}_{satellite}_{product}_{channel}.json"

    if os.path.isfile(filename):
        print("File already exists:", filename)
        return
    
    date_object = datetime.strptime(args.date, '%Y%m%d')
    year = date_object.year
    day_of_year = date_object.timetuple().tm_yday

    print("Generating file list...")

    if channel == 2:
        # get only daylight hours
        hour_range = [11,21] # in UTC time
        flist = []
        for hour in range(hour_range[0], hour_range[-1]):
            gs = za.generate_globsearch_string(year, day_of_year, hour, channel, product, satellite)
            flist.append(za.generate_url_list(gs))
        flist = sorted(list(chain(*flist)))

    elif channel == 13:
        hour = None # None for all hours
        gs = za.generate_globsearch_string(year, day_of_year, hour, channel, product, satellite)
        flist.append(za.generate_url_list(gs))

    if flist == None:
        return

    print(f"Mapping...")
    za.get_mzz_from_references(flist, save=True, save_file=filename + ".partial")
    os.rename(filename + ".partial", filename)
    print('Mapping done!')
    print((time.time() - starttime)/60, 'min')
    print("memory usage:", resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    
if __name__ == "__main__":
    main()
