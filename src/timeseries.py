import numpy as np
import os
import math
import datetime as dt
import pandas as pd
import tqdm
import glob
import shutil
from shapely.geometry import Polygon, box
import rasterio
from rasterio.mask import mask as mask_func
import geopandas as gpd
from typing import Union
import fiona
import time

from stelar_spatiotemporal.eolearn.core import EOPatch, FeatureType, OverwritePermission
from stelar_spatiotemporal.lib import check_types, multiprocess_map, export_eopatch_to_tiff, df_to_csv_manual, load_bbox, get_filesystem
from stelar_spatiotemporal.preprocessing.preprocessing import split_array_into_patchlets, split_patch_into_patchlets, combine_dates_for_eopatch


def get_px_csv_path(outdir:str, prefix:str, x:str, y:str):
        dirname = "LAI_px_ts"
        if prefix is None:
                return os.path.join(outdir, dirname, x, y + '.csv')
        else:
                return os.path.join(outdir, dirname, prefix, x, y + '.csv')
        

def save_long_csv(csv_path:str, dates:list, values:list):
    # Create the directory if necessary
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    df = pd.DataFrame({"datetime": dates, "value": values})
    df.to_csv(csv_path, index=False)


def extract_px_timeseries_wrapper(eop_path:str, outdir:str = None, band:str='LAI') -> Union[None, pd.DataFrame]:
    eopatch = EOPatch.load(eop_path, lazy_loading=True)

    if outdir is None:
        return extract_px_timeseries(eopatch, band=band)
    else:
        os.makedirs(outdir, exist_ok=True)
        outpath = os.path.join(outdir, os.path.basename(eop_path) + ".csv")
        return extract_px_timeseries(eopatch, outpath=outpath, band=band)

def extract_px_timeseries(eopatch:EOPatch, outpath:str = None, band:str='LAI') -> Union[None, pd.DataFrame]:
    """
    This function extracts the timeseries of a given band for an eopatch and saves it as a csv file.
    """
    # Check if band in eopatch
    if band not in eopatch.data.keys():
        raise ValueError(f"Band {band} not in eopatch")

    # Check if array has only one dimension
    if eopatch.data[band].shape[-1] > 1:
        raise ValueError(f"Band {band} has more than one dimension")

    # Get the array (t, w, h)
    arr = eopatch.data[band][...,0]

    ts = eopatch.timestamp

    del eopatch

    # Get x and y coordinates
    xs, ys = np.meshgrid(np.arange(arr.shape[1]), np.arange(arr.shape[2]))

    # Turn xs into "x0 x1 x2 ... xn" and ys into "y0 y1 y2 ... yn
    xs = np.char.add(xs.ravel().astype(str), "_")
    ys = ys.ravel().astype(str)
    cols = np.char.add(xs, ys)

    # Flatten image (t, w, h) -> (t, w*h)
    arr = arr.reshape(arr.shape[0], -1)

    # Turn into column-wise dataframe (i.e. each column is a pixel)
    df = pd.DataFrame(arr, columns=cols, index=ts)

    # Make sure index is date only
    df.index = df.index.date

    # Drop rows with only negative values
    df = df.loc[(df >= 0).any(axis=1)]

    if outpath is None:
        return df

    # Save to csv 
    if not outpath.endswith(".csv"):
          outpath += ".csv"

    fs = get_filesystem(outpath)
    exists = fs.exists(outpath)
    wmode = "w" if not exists else "a"
    df_to_csv_manual(df, outpath, index=True, mode=wmode, header=(wmode=="w"))


def lai_to_csv_px(eop_paths:list, patchlet_dir:str, outdir:str, n_jobs:int=16, delete_patchlets:bool=True):
    """
    This function extracts the timeseries of a given band for each pixel and saves it as a csv file.
    It does this by doing the following:
    1. We break up each image into a series of patchlets.
    2. We combine the data for each patchlet into a single eopatch.
    3. We convert the eopatch into a timeseries of LAI values for each pixel.
    """
    patchlet_size = (1128,1128)
    buffer = 0
    for eop_path in tqdm.tqdm(eop_paths, total=len(eop_paths), desc="1. Splitting tiles into patchlets"):
        local_outdir = os.path.join(patchlet_dir, os.path.basename(eop_path))
        success = split_patch_into_patchlets(eop_path=eop_path, patchlet_size=patchlet_size, buffer=buffer, output_dir=local_outdir)
        if success is not None:
            print(f"Error with {eop_path}")
    
    # 2. Combine the data for each patchlet
    patchlet_paths = glob.glob(os.path.join(patchlet_dir, os.path.basename(eop_paths[0]), "*"))
    patchlet_packages = [os.path.join(patchlet_dir, os.path.basename(eop_path)) for eop_path in eop_paths]
    pnames = [os.path.basename(p) for p in patchlet_paths]

    # Iterate over patchlets and combine dates
    for pname in tqdm.tqdm(pnames, total=len(pnames), desc="2. Combining dates per patchlet"):
        ppaths = [os.path.join(eop_path, pname) for eop_path in patchlet_packages]
        combine_dates_for_eopatch(eop_name=pname, eop_paths=ppaths, outdir=patchlet_dir, delete_after=True)

    # Delete the patchlet packages
    for p in patchlet_packages:
        shutil.rmtree(p)
        
    # 3. Extracting time series from each patchlet
    patchlet_paths = glob.glob(os.path.join(patchlet_dir, "patchlet_*"))
    patchlet_paths.sort()
    for ppath in tqdm.tqdm(patchlet_paths, total=len(patchlet_paths), desc="3. Extracting timeseries per patchlet"):
        extract_px_timeseries_wrapper(eop_path=ppath, outdir = outdir)

    # 4. Deleting the patchlets
    if delete_patchlets:
        print("4. Deleting the patchlets")
        for p in patchlet_paths:
            shutil.rmtree(p)


def lai_to_csv_px_append(npy_path:str, outdir:str):
    timestamp = dt.datetime.strptime(os.path.basename(npy_path).replace(".npy",""), "%Y_%m_%d")

    # 1. Split into patchlets
    patchlets = split_array_into_patchlets(npy_path, patchlet_size=(1128,1128), buffer=0, feature_name='LAI', timestamp=timestamp)

    # 2. Save patchlet data to timeseries csv
    nx = ny = math.sqrt(len(patchlets))
    if nx - int(nx) != 0: # cannot do perfect division
        raise ValueError("Wrong number of patchlets; sqrt(len(patchlets)) is not int")
    
    nx = ny = int(nx)

    for i in range(len(patchlets)):
        print(f"Saving timeseries for patchlet {i+1}/{len(patchlets)}", end='\r')
        x = i % nx
        y = i // ny
        outpath = os.path.join(outdir, f"patchlet_{x}_{y}")
        eop = patchlets[i]
        extract_px_timeseries(eop, outpath=outpath, band="LAI", append=True)


def long_to_wide_ts(csv_path: str, startdate: dt.datetime, enddate: dt.datetime) -> pd.DataFrame:
    """
    Transforms long-format csv files into wide-format dataframes.
    """

    df = pd.read_csv(csv_path, index_col=0, parse_dates=True).sort_index().loc[startdate:enddate]

    # Make sure the dates do not include the time
    df.index = df.index.date

    # Pivot the dataframe and drop the column names
    df = df.T.reset_index(drop=True)

    return df


def long_to_wide_ts_dict(csv_path: str, startdate: dt.datetime, enddate: dt.datetime) -> pd.DataFrame:
     """
     Wrapper for long_to_wide_ts that returns a dictionary instead of a dataframe.
     """
     df = long_to_wide_ts(csv_path, startdate, enddate)
     return df.iloc[0].to_dict()


def combine_timeseries(csv_paths:list, startdate: dt.datetime, enddate: dt.datetime, n: int, n_jobs:int = 8, out_path: str = None) -> pd.DataFrame:
    """
    Combines the timeseries of several csvs into one dataframe.
    """
    if len(csv_paths) < n:
        n = len(csv_paths)    
    path_view = csv_paths[:n]

    # Read the csv files into a dataframe
    print("Reading csv files into a dataframe", end="\r")
    dicts = multiprocess_map(func=long_to_wide_ts_dict, object_list=path_view, startdate=startdate, enddate=enddate, n_jobs=n_jobs)

#     Concatenate the series into one dataframe
    df = pd.DataFrame(dicts)

    if out_path is None:
        return df
    else:
        df.to_csv(out_path, index=True)
        return None


def combine_timeseries_px(csv_paths:list, startdate: dt.datetime, enddate: dt.datetime, n: int, n_jobs:int = 8, out_path: str = None) -> pd.DataFrame:
    df = combine_timeseries(csv_paths, startdate, enddate, n, n_jobs)

    pnames = []
    x_coords = []
    y_coords = []

    for path in csv_paths[:n]:
        path_parts = path.split(os.sep)
        pnames.append(path_parts[-3])
        x_coords.append(int(path_parts[-2]))
        y_coords.append(int(path_parts[-1].replace(".csv","")))

    df["patch"] = pnames
    df["x"] = x_coords
    df["y"] = y_coords

    # Set the index to the patch name and the x and y coordinates
    df.set_index(["patch", "x", "y"], inplace=True)

    # Sort the columns by date
    df.sort_index(axis=1, inplace=True)

    if out_path is None:
        return df
    else:
        print(f"Saving dataframe to {out_path}")
        df.to_csv(out_path, index=True)
        return None

# Load a file of fields if possible
def load_fields(field_path:str, nrows:int = None, min_area:int = 0, max_area:int = None):
    # Get the filesystem
    filesystem = get_filesystem(field_path)

    # Copy and read the fields with geopandas
    tmpdir = os.environ.get("TMPDIR", "/tmp")
    tmp_fieldpath = os.path.join(tmpdir, os.path.basename(field_path))

    if not os.path.exists(tmp_fieldpath):
        # Download the file if it is on a remote filesystem
        filesystem.get(field_path, tmp_fieldpath)

    # Read the fields with geopandas
    df = gpd.read_file(tmp_fieldpath, rows=nrows)

    # Filter the fields by area
    if min_area > 0:
        df = df[df.geometry.area > min_area]
    if max_area is not None:
        df = df[df.geometry.area < max_area]

    # CRS and bbox check
    crs = df.crs
    bbox = df.total_bounds
    print("Loaded fields with CRS:", crs, "and bbox:", bbox)

    # Read fields with fiona and store as a geopandas dataframe
    # lines = []
    # c = 0
    # ids = []
    # skipped_fields = 0
    # with fiona.open(filesystem.open(field_path, 'rb')) as fields_file:
    #     for i, line in enumerate(fields_file):
    #         # Get feature
    #         feature = line['geometry']

    #         # Get area
    #         area = Polygon(feature['coordinates'][0]).area

    #         if area > min_area and (max_area is None or area < max_area):
    #             lines.append(line)
    #             c += 1
    #             ids.append(i)
    #         else:
    #             skipped_fields += 1

    #         if nrows is not None and c >= nrows:
    #             break

    #     df = gpd.GeoDataFrame.from_features(lines, crs=fields_file.crs)
    #     df.index = ids
    # print(f"Loaded {c} fields with area between {min_area} and {max_area} m2, skipped {skipped_fields} fields")

    return df


# Slice tiff with field shape
def mask_field(field: Polygon, src: rasterio.DatasetReader):
    mask, transform = mask_func(src, [field], crop=True, nodata=0)
    return mask
        
def get_field_csv_path(outdir:str, prefix:str, field_id:str):
        dirname = "LAI_field_ts"
        if prefix is None:
                return os.path.join(outdir, dirname, field_id + '.csv')
        else:
                return os.path.join(outdir, dirname, prefix, field_id + '.csv')
        

def field_to_ts(field: Polygon, src: rasterio.DatasetReader):
    # Mask the array
    try:
        mask_array = mask_field(field, src)
    except Exception as e:
        print(f"Error masking field: {e}")
        return np.ones((src.count,)) * np.nan  # Return an array of nans if the mask fails

#     Check if the mask is empty
    if mask_array.shape[0] == 0:
        print(f"Empty mask for field")
        return None

    # Flatten the images -> (w*h, n_times)
    mask_array = mask_array.reshape(mask_array.shape[0], -1).astype(np.float16)

    # Get median of correct values per date
    mask_array[mask_array <= 0] = np.nan
    return np.nanmedian(mask_array, axis=1)

import warnings
warnings.filterwarnings("ignore")

from typing import Tuple

def field_to_df(data:Tuple[int,Polygon], tiff_path:str, datetimes:list):
    field_id, field = data

    # Open the raster
    src = rasterio.open(tiff_path)

    # Get the values
    values = field_to_ts(field, src)

    # Create df
    df = pd.DataFrame(data=[values], columns=datetimes)

    # Sort columns by date
    df.sort_index(axis=1, inplace=True)

    df.index = [field_id]

    return df


def field_to_csv(fields:gpd.GeoDataFrame, tiff_path:str,
                 datetimes:list, outpath:str, n_jobs:int = 8, tile_id:str = None):
    # Iteratively create df of field timeseries
    dfs = multiprocess_map(field_to_df, list(fields.geometry.items()), tiff_path=tiff_path, datetimes=datetimes, n_jobs=n_jobs)

    # Concatenate the dfs
    print(f"Concatenating timeseries", end="\r")
    df = pd.concat(dfs, axis=0)

    # Sort the index by field id
    df.sort_index(inplace=True)

    # Remove columns with only nans
    df = df.dropna(axis=1, how="all")

    # Remove rows with only nans
    # df = df.dropna(axis=0, how="all")

    # Transpose the df to enable iterative appending
    df = df.T

    if not outpath.endswith(".csv"):
        outpath += ".csv"

    # Append df to field_ts csv if exists
    fs = get_filesystem(outpath)
    exists = fs.exists(outpath)
    wmode = "w" if not exists else "a"
    df_to_csv_manual(df, outpath, index=True, mode=wmode, header=(wmode=="w"))


def lai_to_csv_field(eop_paths:list, fields_path:str, outpath:str, nfields:int = None, n_jobs:int = 8, delete_tmp:bool=False, tmpdir:str = "/tmp"):
        # Make temporary tiff dir
        tif_dir = os.path.join(tmpdir, "tiffs")
        os.makedirs(tif_dir, exist_ok=True)

        # Load the fields
        print("Loading fields", end="\r")
        # minarea = 1000 # 1 km2
        # maxarea = 500_000 # 50 hectares

        if nfields is None:
                fields = load_fields(fields_path)
        else:
                fields = load_fields(fields_path, nrows=nfields)

        for i, eop_path in enumerate(eop_paths):
                print(f"Processing eopatch {i+1}/{len(eop_paths)}")

                start = time.time()

                # 0. Get the datetimes
                eop = EOPatch.load(eop_path, lazy_loading=True)
                datetimes = eop.timestamp

                # Make sure the fields are in the same coordinate system as the eopatch
                if fields.crs != eop.bbox.crs:
                    fields = fields.to_crs(eop.bbox.crs.ogc_string())

                # Filter the fields down to only those that intersect with the eopatch bbox
                shapebox = box(*eop.bbox)
                fields = fields[fields.intersects(shapebox)]
                if len(fields) == 0:
                    print(f"No fields intersect with eopatch {eop_path}, skipping")
                    continue

                # 1. Save the eopatches as tiff if necessary
                print(f"1. Temporarily saving eopatch as tiff")
                tiff_path = os.path.join(tif_dir, os.path.basename(eop_path) + ".tiff")
                if not os.path.exists(tiff_path):
                        export_eopatch_to_tiff(eop_path, tiff_path, feature=(FeatureType.DATA, "LAI"), nodata=0, channel_pos=0)
                print(f"Time taken: {time.time() - start} seconds")

                start = time.time()
                # 3. For each field, mask the tiff, get median and save timeseries as csv
                print(f"2. Masking tiff and saving timeseries")
                field_to_csv(fields, tiff_path, datetimes, outpath, n_jobs=n_jobs)
                print(f"Time taken: {time.time() - start} seconds")
                
                # 4. Delete the temporary tiff
                if delete_tmp:
                        print(f"4. Deleting temporary tiff")
                        os.remove(tiff_path)


def lai_to_csv_field_append(npy_path:str, bbox_path:str, fields_path:str, outpath:str, n_jobs:int=8):
    tmp_eop_path = "tmp_eop"
    timestamp = dt.datetime.strptime(os.path.basename(npy_path).replace(".npy",""), "%Y_%m_%d")

    # 1. Temporarily save the npy array as an eopatch
    print(f"Temporarily saving npy as eopatch")
    bbox = load_bbox(bbox_path) 
    eop = EOPatch()
    eop.timestamp = [timestamp]
    eop.bbox = bbox
    eop.data['LAI'] = np.load(npy_path)[np.newaxis, ..., np.newaxis]
    eop.save(tmp_eop_path, overwrite_permission=OverwritePermission.OVERWRITE_FEATURES)

    # 2. Process the eopatch with the lai_to_csv_field function
    lai_to_csv_field([tmp_eop_path], fields_path, outpath=outpath, n_jobs=n_jobs)

    # 3. Remove the temporarily saved eopatch
    print(f"Removing temporarily saved eopatch")
    shutil.rmtree(tmp_eop_path)


def combine_timeseries_field(csv_paths:list, startdate: dt.datetime, enddate: dt.datetime, n: int, n_jobs:int = 8, out_path: str = None) -> pd.DataFrame:
        df = combine_timeseries(csv_paths, startdate, enddate, n, n_jobs)

        field_ids = [os.path.basename(p).replace(".csv","") for p in csv_paths[:n]]
        df['field_id'] = field_ids

        # Set the index to the patch name and the x and y coordinates
        df.set_index(["field_id"], inplace=True)

        # Sort the columns by date
        df.sort_index(axis=1, inplace=True)

        if out_path is None:
                return df
        else:
                print(f"Saving dataframe to {out_path}")
                df.to_csv(out_path, index=True)
                return None
        
if __name__ == "__main__":
    pass