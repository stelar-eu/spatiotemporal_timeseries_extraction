import json
import os
import glob
import sys
import rasterio.transform
from sentinelhub import CRS
from typing import List
from stelar_spatiotemporal.preprocessing.preprocessing import combine_npys_into_eopatches, max_partition_size, unpack_tif
from stelar_spatiotemporal.preprocessing.vista_preprocessing import unpack_vista_unzipped
from stelar_spatiotemporal.preprocessing.timeseries import lai_to_csv_px, lai_to_csv_field
from stelar_spatiotemporal.lib import load_bbox, get_filesystem, save_bbox
import argparse
import rasterio
from rasterio.io import MemoryFile
import re
import datetime as dt
import numpy as np
from sentinelhub import BBox, CRS
import time


def unpack_ras(ras_paths:List[str], rhd_paths:List[str], out_path:str):
    for ras_path, rhd_path in zip(ras_paths, rhd_paths):
        unpack_vista_unzipped(ras_path, rhd_path, out_path, delete_after=False, crs=CRS('32630'))

def combining_npys(npy_dir:str, out_path:str): 

    if not os.path.exists(npy_dir):
        raise ValueError("Something went wrong in previous steps; no npys folder found in {}".format(out_path))

    npy_paths = glob.glob(os.path.join(npy_dir, "*.npy"))
    mps = max_partition_size(npy_paths[0], MAX_RAM=int(4 * 1e9))

    bbox = load_bbox(os.path.join(npy_dir, "bbox.pkl"))

    combine_npys_into_eopatches(
        npy_paths=npy_paths, outpath=out_path,
                            feature_name="LAI",
                            bbox=bbox,
                            partition_size=mps,
                            delete_after=False)
    
def create_px_ts(eop_dir:str, patchlet_dir:str, out_path:str):
    eop_paths = glob.glob(os.path.join(eop_dir, "partition_*"))
    if len(eop_paths) == 0: eop_paths = [eop_dir]

    # Turn the LAI values into a csv file
    lai_to_csv_px(eop_paths, patchlet_dir=patchlet_dir, outdir=out_path, delete_patchlets=False)

def create_field_ts(eop_dir:str, out_path:str, fields_path:str):
    eop_paths = glob.glob(os.path.join(eop_dir, "partition_*"))
    if len(eop_paths) == 0: eop_paths = [eop_dir]

    eop_paths.sort()

    # Perform the process as described above
    lai_to_csv_field(eop_paths, fields_path=fields_path, outdir=out_path, n_jobs=16, delete_tmp=False)

def cleanup(tmp_path:str):
    npy_dir = os.path.join(tmp_path, "npys")
    eops_dir = os.path.join(tmp_path, "lai_eopatch")
    patchlets_dir = os.path.join(tmp_path, "patchlets")
    todel = [npy_dir, eops_dir, patchlets_dir]
    for todel_path in todel:
        if os.path.exists(todel_path):
            print("Deleting {}".format(todel_path))
            os.system("rm -rf {}".format(todel_path))

def check_ras(input_path):
    # Check if rhd_paths match with ras_paths

    fs = get_filesystem(input_path)
    ras_paths = fs.glob(os.path.join(input_path, "*.RAS"))
    rhd_paths = fs.glob(os.path.join(input_path, "*.RHD"))
    if len(ras_paths) == 0:
        raise ValueError("No RAS files found in the input folder.")
    if len(rhd_paths) == 0:
        raise ValueError("No RHD files found in the input folder.")
    if len(ras_paths) != len(rhd_paths):
        raise ValueError("Number of RAS and RHD files do not match.")
    
    ras_paths_match = []
    rhd_paths_match = []
    for ras_path in ras_paths:
        exp_rhd_basename = os.path.basename(ras_path).replace(".RAS", ".RHD")
        for rhd_path in rhd_paths:
            if rhd_path.endswith(exp_rhd_basename): 
                ras_paths_match.append(ras_path)
                rhd_paths_match.append(rhd_path)
                break
        else:
            raise ValueError("No matching rhd_path found for ras_path: {}".format(ras_path))
        
    return ras_paths_match, rhd_paths_match


def image2ts_pipeline(input_path:str, extension:str,
                      output_path:str, 
                      field_path:str, 
                      skip_pixel:bool):
    """
    This function takes a directory of raster files and headers and converts them to time series dataset.

    Parameters
    ----------
    input_path : str
        Path to the folder containing the input images.
    extension : str
        Extension of the input files. Default is 'RAS'.
    output_path : str
        Path to the folder where the output files will be saved.
    field_path : str
        Path to the shapefile containing the boundaries of the agricultural fields. If not given, field-level time series will not be created.
    skip_pixel : bool
        Skip creating pixel-level time series
    """
    total_start = time.time()

    # Check if we do pixel, field, or both
    pixel = not skip_pixel
    field = field_path is not None

    # Check if the extension is supported
    if extension not in ["RAS", "TIF", "TIFF"]:
        raise ValueError("Extension {} is not supported.".format(extension))
    
    TMP_PATH = '/tmp'
    npy_dir = os.path.join(TMP_PATH, "npys")
    if not os.path.exists(npy_dir):
        os.makedirs(npy_dir)

    partial_times = []

    start = time.time()
    if extension == "RAS":
        ras_paths, rhd_paths = check_ras(input_path)

        # 1. Unpack the RAS files
        print("1. Unpacking RAS files...")
        unpack_ras(ras_paths=ras_paths, 
                rhd_paths=rhd_paths, 
                out_path=npy_dir)
    else:
        # 1. Unpack the TIF files
        unpack_tif(indir=input_path,
                    outdir=npy_dir,
                    extension=extension,)
    
    partial_times.append({
        "step": "Unpacking RAS files",
        "runtime": time.time() - start
    })

    npys = glob.glob(os.path.join(npy_dir, "*.npy"))
    n_images = len(npys)

    # Get width and height of the images
    arr = np.load(npys[0])
    height, width = arr.shape

    # 2. Combining the images into eopatches
    start = time.time()

    print("2. Combining the images into eopatches...")
    eopatches_dir = os.path.join(TMP_PATH, "lai_eopatch")
    combining_npys(npy_dir=npy_dir, out_path=eopatches_dir)

    partial_times.append({
        "step": "Combining the images into eopatches",
        "runtime": time.time() - start
    })

    # 3. Create pixel-level time series
    if pixel:
        start = time.time()

        patchlets_dir = os.path.join(TMP_PATH, "patchlets")
        px_path = os.path.join(output_path, "pixel_timeseries")
        print("3. Creating pixel-level time series...")
        create_px_ts(eop_dir=eopatches_dir,
                     patchlet_dir=patchlets_dir,
                     out_path=px_path)
        
        partial_times.append({
            "step": "Creating pixel-level time series",
            "runtime": time.time() - start
        })

    # 4. Create field-level time series
    if field:
        start = time.time()

        field_ts_path = os.path.join(output_path, "field_timeseries")
        print("4. Creating field-level time series...")
        create_field_ts(eop_dir=eopatches_dir,
                        out_path=field_ts_path, 
                        fields_path=field_path)
        
        partial_times.append({
            "step": "Creating field-level time series",
            "runtime": time.time() - start
        })
        
    # 5. Create the output json
    output_json = {
        "message": "Time series data has been created successfully.",
        "output": [
            {
                "path": output_path,
                "type": "Directory containing the time series data."
            }
        ],
        "metrics": {
            "number_of_images": n_images,
            "image_width": width,
            "image_height": height,
            "total_runtime": time.time() - total_start,
            "partial_runtimes": partial_times,
        }
    }

    return output_json
        

if __name__ == "__main__":
    if len(sys.argv) < 3: # If no arguments are given, use the default values
        input_json_path = "/home/jens/ownCloud/Documents/3.Werk/0.TUe_Research/0.STELAR/0.VISTA/VISTA_workbench/src/modules/image2ts/resources/input.json"
        output_json_path = "/home/jens/ownCloud/Documents/3.Werk/0.TUe_Research/0.STELAR/0.VISTA/VISTA_workbench/src/modules/image2ts/resources/output.json"
    else:
        input_json_path = sys.argv[1]
        output_json_path = sys.argv[2]

    # Read and parse the input JSON file
    with open(input_json_path, "r") as f:
        input_json = json.load(f)

    # Required parameters
    try:
        input_path = input_json["input"][0]["path"]
    except Exception as e:
        raise ValueError("Input path is required. See the documentation for the suggested input format. Error: {}".format(e))
    
    try:
        output_path = input_json["parameters"]["output_path"]
    except Exception as e:
        raise ValueError("Output path is required. See the documentation for the suggested input format. Error: {}".format(e))
    
    # Optional parameters
    file_extension = input_json["parameters"].get("extension", "RAS")
    field_path = input_json["parameters"].get("field_path", None)
    skip_pixel = input_json["parameters"].get("skip_pixel", False)

    # Check if minio credentials are provided
    if "minio" in input_json:
        try:
            id = input_json["minio"]["id"]
            key = input_json["minio"]["key"]
            url = input_json["minio"]["endpoint_url"]

            os.environ["MINIO_ACCESS_KEY"] = id
            os.environ["MINIO_SECRET_KEY"] = key
            os.environ["MINIO_ENDPOINT_URL"] = url

            os.environ["AWS_ACCESS_KEY_ID"] = id
            os.environ["AWS_SECRET_ACCESS_KEY"] = key

            # Add s3:// to the input and output paths
            input_path = "s3://" + input_path
            output_path = "s3://" + output_path
            field_path = "s3://" + field_path if field_path is not None else None

        except Exception as e:
            raise ValueError("Access and secret keys are required if any path is on MinIO. Error: {}".format(e))

    response = image2ts_pipeline(input_path=input_path,
                                extension=file_extension,
                                output_path=output_path,
                                field_path=field_path,
                                skip_pixel=skip_pixel)
    

    print(response)
    
    with open(output_json_path, "w") as f:
        json.dump(response, f, indent=4)

    



