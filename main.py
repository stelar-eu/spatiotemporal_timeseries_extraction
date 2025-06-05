import json
import os
import glob
import sys
import rasterio.transform
from sentinelhub import CRS
from typing import List, Text
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

def cleanup(tmp_path:str='/tmp'):
    """
    Clean up the temporary directories created during the image2ts pipeline.
    
    Parameters
    ----------
    tmp_path : str
        Path to the temporary directory. Default is '/tmp'.
    """
    npy_dir = os.path.join(tmp_path, "npys")
    eops_dir = os.path.join(tmp_path, "lai_eopatch")
    patchlets_dir = os.path.join(tmp_path, "patchlets")
    todel = [npy_dir, eops_dir, patchlets_dir]
    for todel_path in todel:
        if os.path.exists(todel_path):
            print("Deleting {}".format(todel_path))
            os.system("rm -rf {}".format(todel_path))

def check_ras(input_paths: List[Text]):
    ras_paths = [p for p in input_paths if p.endswith(".RAS")]
    rhd_paths = [p for p in input_paths if p.endswith(".RHD")]

    ras_path_filtered = []
    rhd_path_filtered = []

    # Filter out the RAS files that do not have a corresponding RHD file in the same directory
    for ras_path in ras_paths:
        base_name = os.path.basename(ras_path)
        rhd_path = os.path.join(os.path.dirname(ras_path), base_name.replace(".RAS", ".RHD"))
        rhd_path = rhd_paths.get(rhd_path)
        if rhd_path:
            ras_path_filtered.append(ras_path)
            rhd_path_filtered.append(rhd_path)
        else:
            print(f"Warning: No corresponding RHD file found for {ras_path}. Skipping this file.")

    # Check if the number of RAS and RHD files match
    if len(ras_path_filtered) != len(rhd_path_filtered):
        raise ValueError("Number of RAS and RHD files do not match. Please check the input paths.")
    
    return ras_path_filtered, rhd_path_filtered

def image2ts_pipeline(input_paths: List[Text], extension:str,
                      output_path:str, 
                      field_path:str, 
                      skip_pixel:bool,
                      tmp_path:str='/tmp'):
    """
    This function takes a directory of raster files and headers and converts them to time series dataset.

    Parameters
    ----------
    input_paths : List[Text]
        List of paths to the input files.
    extension : str
        Extension of the input files. Default is 'TIF'.
    output_path : str
        Path to the folder where the output files will be saved.
    field_path : str
        Path to the shapefile containing the boundaries of the agricultural fields. If not given, field-level time series will not be created.
    skip_pixel : bool
        Skip creating pixel-level time series
    tmp_path : str
        Path to the temporary directory for intermediate files. Default is '/tmp'.
    """
    total_start = time.time()

    # Check if we do pixel, field, or both
    pixel = not skip_pixel
    field = field_path is not None

    # Check if the extension is supported
    if extension not in ["RAS", "TIF", "TIFF"]:
        raise ValueError("Extension {} is not supported.".format(extension))
    
    # Use the provided tmp_path instead of hardcoded '/tmp'
    npy_dir = os.path.join(tmp_path, "npys")
    if not os.path.exists(npy_dir):
        os.makedirs(npy_dir)

    partial_times = []

    start = time.time()

    if extension == '.RAS':
        parsed_paths = [path for path in input_paths if path.endswith((".RAS", '.RHD'))]
    else:
        parsed_paths = [path for path in input_paths if path.endswith(extension)]

    # If no input files are found then the provided path might be a directory, so let's list the files in that directory with the given extension
    if len(parsed_paths) == 0:
        print("No input files found. Checking if the provided path is a directory...")
        path = input_paths[0]
        fs = get_filesystem(path)
        if fs.isdir(path):
            print("The provided path is a directory. Listing the files in that directory...")
            parsed_paths = fs.glob(os.path.join(path, "*{}".format(extension)))
        else:
            raise ValueError("No input files found. Please check the input paths.")

    if extension == "RAS":
        ras_paths, rhd_paths = check_ras(parsed_paths)

        # 1. Unpack the RAS files
        print("1. Unpacking RAS files...")
        unpack_ras(ras_paths=ras_paths, 
                rhd_paths=rhd_paths, 
                out_path=npy_dir)
    else:
        # 1. Unpack the TIF files
        unpack_tif(image_paths=parsed_paths,
                    outdir=npy_dir,
                    extension=extension,)
        pass
    
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
    eopatches_dir = os.path.join(tmp_path, "lai_eopatch")
    combining_npys(npy_dir=npy_dir, out_path=eopatches_dir)

    partial_times.append({
        "step": "Combining the images into eopatches",
        "runtime": time.time() - start
    })

    # 3. Create pixel-level time series
    if pixel:
        start = time.time()

        patchlets_dir = os.path.join(tmp_path, "patchlets")
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
    parser = argparse.ArgumentParser(description="Image to Time Series Pipeline")
    parser.add_argument("input_json", nargs="?", help="Path to the input JSON file")
    parser.add_argument("output_json", nargs="?", help="Path to the output JSON file")
    parser.add_argument("--TMPDIR", help="Path to the temporary directory", default="/tmp")
    args = parser.parse_args()

    # Set default paths if not provided
    if args.input_json is None:
        input_json_path = "/home/jens/ownCloud/Documents/3.Werk/0.TUe_Research/0.STELAR/0.VISTA/VISTA_workbench/src/modules/image2ts/resources/input_tmp.json"
    else:
        input_json_path = args.input_json
        
    if args.output_json is None:
        output_json_path = "/home/jens/ownCloud/Documents/3.Werk/0.TUe_Research/0.STELAR/0.VISTA/VISTA_workbench/src/modules/image2ts/resources/output.json"
    else:
        output_json_path = args.output_json

    # Read and parse the input JSON file
    with open(input_json_path, "r") as f:
        input_json = json.load(f)
    
    # Handle the new input format which includes a "result" section
    if "result" in input_json:
        input_data = input_json["result"]
    else:
        input_data = input_json

    # Required parameters - now handling a list of image paths
    try:
        # The input images are now in result.input.images which is a list
        input_paths = input_data["input"]["images"]
        if not isinstance(input_paths, list):
            input_paths = [input_paths]
    except Exception as e:
        raise ValueError(f"Input paths are required. See the documentation for the suggested input format. Error: {e}")
    
    try:
        # The output path is now in result.output.timeseries
        output_path = input_data["output"]["timeseries"]
    except Exception as e:
        raise ValueError(f"Output path is required. See the documentation for the suggested input format. Error: {e}")
    
    # Optional parameters
    file_extension = input_data.get("parameters", {}).get("extension", "TIF")
    field_path = input_data.get("parameters", {}).get("field_path", None)
    skip_pixel = input_data.get("parameters", {}).get("skip_pixel", False)
    
    # Get temporary directory from JSON if provided, otherwise use command-line argument
    tmp_path = input_data.get("parameters", {}).get("tmp_path", args.TMPDIR)
    
    # Create the tmp_path directory if it doesn't exist
    if not os.path.exists(tmp_path):
        os.makedirs(tmp_path, exist_ok=True)
        print(f"Created temporary directory: {tmp_path}")
    else:
        print(f"Using existing temporary directory: {tmp_path}")

    # Check if minio credentials are provided
    if "minio" in input_data:
        try:
            id = input_data["minio"]["id"]
            key = input_data["minio"]["key"]
            token = input_data["minio"].get("skey")
            url = input_data["minio"]["endpoint_url"]

            os.environ["MINIO_ACCESS_KEY"] = id
            os.environ["MINIO_SECRET_KEY"] = key
            os.environ["MINIO_ENDPOINT_URL"] = url
            if token:
                os.environ["MINIO_SESSION_TOKEN"] = token

        except Exception as e:
            raise ValueError(f"Access and secret keys are required if any path is on MinIO. Error: {e}")

    response = image2ts_pipeline(input_paths=input_paths,
                                extension=file_extension,
                                output_path=output_path,
                                field_path=field_path,
                                skip_pixel=skip_pixel,
                                tmp_path=tmp_path)
    
    print(response)
    
    with open(output_json_path, "w") as f:
        json.dump(response, f, indent=4)
        
    # Cleanup temporary directories
    print("Cleaning up temporary directories...")
    cleanup(tmp_path=tmp_path)





