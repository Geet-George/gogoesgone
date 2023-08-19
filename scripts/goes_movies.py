import imageio
import glob
from datetime import datetime, timedelta

def video_from_snapshots(snapshot_folder, save_path, fps=8):
    '''
    Make a video from all images in a folder and save to path.
    '''

    print(f'Making video of snapshots in {snapshot_folder} ...')

    writer = imageio.get_writer(save_path, fps=fps)

    image_path = sorted(glob.glob(snapshot_folder+'goes16_ABI*'))

    for im in image_path:
        writer.append_data(imageio.imread(im))
    writer.close()
    
    print('video saved!')
    

# Make list of dates for movies
start_date = datetime(2020, 1, 20)
end_date = datetime(2020, 2, 20)

days_for_videos = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((end_date - start_date).days + 1)]

# Choose channel
channel = 2

# Make and save movies
for day in days_for_videos:

    video_from_snapshots(snapshot_folder=f"/scratch/m/m300931/data/GOES-16/snapshots/channel_{channel}/{day}/",
                         save_path=f"/scratch/m/m300931/data/GOES-16/movies/channel_{channel}/goes16_ABI-L2-CMIPF_ch{channel}_{day}.mp4")
    