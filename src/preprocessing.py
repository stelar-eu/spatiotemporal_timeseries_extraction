import numpy as np
from sentinelhub import BBox
from stelar_spatiotemporal.eolearn.core import EOPatch, OverwritePermission
import os
import datetime as dt

def combine_npys_into_eopatches(npy_paths: list, 
                 outpath: str,
                 feature_name:str,
                 bbox: BBox,
                 dates:list = None,
                 delete_after:bool = False,
                 partition_size:int = 10):
    """
    Combine multiple numpy arrays into one eopatch by stacking them along the time axis.
    If dates are not given, infer the dates from the filenames.
    """
    dateformat = "%Y_%m_%d"

    # Get all the dates that have info for all bands
    if dates is None:
        dates = [dt.datetime.strptime(os.path.basename(file).replace(".npy",""), dateformat) for file in npy_paths]
    else: # Check if dates are valid
        if len(npy_paths) != len(dates):
            raise ValueError("Number of dates does not match number of files")
        
    # Process each partition
    if partition_size > len(dates): partition_size = len(dates)
    partitions = np.arange(0, len(dates), partition_size)
    print(f"Processing {len(partitions)} partitions of {partition_size} dates each")

    for i,start in enumerate(partitions):
        print(f"Processing partition {i+1}/{len(partitions)}", end="\r")

        end = min(start+partition_size, len(npy_paths))

        # Stack data
        arrays = [np.load(npy_paths[i]) for i in range(start, end)]
        part_data = np.stack(arrays, axis=0)
        part_dates = dates[start:end]
        
        # Create eopatch
        eopatch = EOPatch()
        eopatch.data[feature_name] = part_data[..., np.newaxis]
        eopatch.bbox = bbox
        eopatch.timestamp = part_dates

        # Save eopatch
        print(f"Saving eopatch {i+1}/{len(partitions)}", end="\r")
        part_outpath = outpath if len(partitions) == 1 else os.path.join(outpath, f"partition_{i+1}")
        eopatch.save(part_outpath, overwrite_permission=OverwritePermission.OVERWRITE_PATCH)

    # (Optional) Delete all the individual files
        if delete_after:
            for file in npy_paths[start:end]:
                os.remove(file)