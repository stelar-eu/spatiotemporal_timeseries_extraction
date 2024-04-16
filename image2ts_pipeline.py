import os
import glob
import sys
import rasterio.transform
from sentinelhub import CRS
from typing import List
from stelar_spatiotemporal.preprocessing.preprocessing import combine_npys_into_eopatches, max_partition_size
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

parser = argparse.ArgumentParser(description='Convert raster images to time series dataset')
   

def unpack_ras(ras_paths:List[str], rhd_paths:List[str], out_path:str):
    for ras_path, rhd_path in zip(ras_paths, rhd_paths):
        unpack_vista_unzipped(ras_path, rhd_path, out_path, delete_after=False, crs=CRS('32630'))

def unpack_tif(tif_paths:List[str], outdir:str):
    fs = get_filesystem(input_path)
    paths = fs.glob(os.path.join(input_path, "*.{}".format(extension)))

    if len(paths) == 0:
        raise ValueError("No {} files found in the input folder.".format(extension))
    
    # 1. Unpack the TIF files and save to npy
    # TODO package this into a function in stelar spatiotemporal
    print(f"Unpacking {len(paths)} files...")
    gbbox = None
    for path in paths:
        # Open the raster
        with fs.open(path, 'rb') as f, MemoryFile(f) as memfile:
            with memfile.open() as src:
                bbox = rasterio.transform.array_bounds(src.height, src.width, src.transform)
                crs = CRS(src.crs.to_string())
                bbox = BBox(bbox, crs)

                # Check if the bbox is the same for all images
                if not gbbox:
                    gbbox = bbox
                elif bbox != gbbox:
                    # print(bbox, gbbox)
                    raise ValueError("Bounding boxes of the images do not match.")
                
                # See if there are timestamps in the metadata in format YYYYMMDD
                timestamps = src.tags().get("TIFFTAG_DATETIME").split(" ") if "TIFFTAG_DATETIME" in src.tags() else None

                if timestamps is None and src.count == 1:
                    # Check if there is a timestamp in the filename in the format YYYYMMDD
                    filename = os.path.basename(path)
                    timestamps = re.search(r"\d{8}", filename)
                    if timestamps:
                        timestamps = [timestamps.group()]

                if not timestamps:
                    raise ValueError("No timestamps found in the metadata or the filename.")
                
                # Parse the timestamps
                timestamps = [dt.datetime.strptime(timestamp, "%Y%m%d") for timestamp in timestamps]

                # Read each of the images and save them to npy
                for timestamp in timestamps:
                    img = src.read().squeeze()

                    # Save the image to npy
                    npy_path = os.path.join(outdir, "{}.npy".format(timestamp.strftime("%Y_%m_%d")))
                    np.save(npy_path, img)

    # Save the bbox
    bbox_path = os.path.join(outdir, "bbox.pkl")
    save_bbox(bbox, bbox_path)


def combining_npys(npy_dir:str, out_path:str): 

    if not os.path.exists(npy_dir):
        raise ValueError("Something went wrong in previous steps; no npys folder found in {}".format(out_path))

    npy_paths = glob.glob(os.path.join(npy_dir, "*.npy"))
    mps = max_partition_size(npy_paths[0], MAX_RAM=int(4 * 1e9))

    bbox = load_bbox(os.path.join(npy_dir, "bbox.pkl"))

    combine_npys_into_eopatches(npy_paths=npy_paths, 
                            outpath=out_path,
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

    if extension == "RAS":
        ras_paths, rhd_paths = check_ras(input_path)

        # 1. Unpack the RAS files
        print("1. Unpacking RAS files...")
        unpack_ras(ras_paths=ras_paths, 
                rhd_paths=rhd_paths, 
                out_path=npy_dir)
    else:
        # TODO do not do temporary unpacking for TIF files for field level time series -> directly read the TIF files
        # 1. Unpack the TIF files
        print("1. Unpacking {} files...".format(extension))
        
        fs = get_filesystem(input_path)
        tif_paths = fs.glob(os.path.join(input_path, "*.{}".format(extension)))

        unpack_tif(tif_paths=tif_paths,
                     outdir=npy_dir)

    # 2. Combining the images into eopatches
    print("2. Combining the images into eopatches...")
    eopatches_dir = os.path.join(TMP_PATH, "lai_eopatch")
    combining_npys(npy_dir=npy_dir,
                   out_path=eopatches_dir)

    # 3. Create pixel-level time series
    if pixel:
        patchlets_dir = os.path.join(TMP_PATH, "patchlets")
        px_path = os.path.join(output_path, "pixel_timeseries")
        print("3. Creating pixel-level time series...")
        create_px_ts(eop_dir=eopatches_dir,
                     patchlet_dir=patchlets_dir,
                     out_path=px_path)

    # 4. Create field-level time series
    if field:
        field_ts_path = os.path.join(output_path, "field_timeseries")
        print("4. Creating field-level time series...")
        create_field_ts(eop_dir=eopatches_dir,
                        out_path=field_ts_path, 
                        fields_path=field_path)

    # 5. Cleanup
    print("5. Cleaning up...")
    cleanup(TMP_PATH)
    

parser.add_argument("-i", "--input_path", 
                    type=str, 
                    required=True,
                    help="Path to the folder containing the input images.")
parser.add_argument("-x", "--file_extension", 
                    type=str, 
                    required=False,
                    default="RAS",
                    help="Extension of the input files. Default is 'RAS'.")
parser.add_argument("-o", "--output_path",
                    type=str,
                    required=True,
                    help="Path to the folder where the output files will be saved.")
parser.add_argument("-f", "--field_path",
                    type=str,
                    required=False,
                    default=None,
                    help="Path to the shapefile containing the boundaries of the agricultural fields. If not given, field-level time series will not be created.")
parser.add_argument("-skippx", "--skip_pixel",
                    action="store_true",
                    help="Skip creating pixel-level time series")
parser.add_argument("--MINIO_ACCESS_KEY",
                    type=str,
                    required=False,
                    default=None,
                    help="Access key for the MinIO server. Required if the input and output paths are on MinIO (i.e., start with 's3://').")
parser.add_argument("--MINIO_SECRET_KEY",
                    type=str,
                    required=False,
                    default=None,
                    help="Secret key for the MinIO server. Required if the input and output paths are on MinIO (i.e., start with 's3://').")
parser.add_argument("--MINIO_ENDPOINT_URL",
                    type=str,
                    required=False,
                    default=None,
                    help="Endpoint URL for the MinIO server. Required if the input and output paths are on MinIO (i.e., start with 's3://').")
    
if __name__ == "__main__":
    if len(sys.argv) == 1:
        input_path = "s3://stelar-spatiotemporal/LAI_fused"
        output_path = "s3://stelar-spatiotemporal"
        extension = 'TIF'
        # field_path = "s3://stelar-spatiotemporal/fields_2020_07_27.gpkg"
        field_path = None
        skip_pixel = False
        MINIO_ACCESS_KEY = "minioadmin"
        MINIO_SECRET_KEY = "minioadmin"
        MINIO_ENDPOINT_URL = "http://localhost:9000"
    else:
        args = parser.parse_args()
        input_path = args.input_path
        extension = args.file_extension
        output_path = args.output_path
        field_path = args.field_path
        skip_pixel = args.skip_pixel
        MINIO_ACCESS_KEY = args.MINIO_ACCESS_KEY
        MINIO_SECRET_KEY = args.MINIO_SECRET_KEY
        MINIO_ENDPOINT_URL = args.MINIO_ENDPOINT_URL
    
    # Check dependencies between arguments
    isminio = input_path.startswith("s3://") or output_path.startswith("s3://") or field_path.startswith("s3://")
    nocred = MINIO_ACCESS_KEY is None or MINIO_SECRET_KEY is None or MINIO_ENDPOINT_URL is None
    if isminio and nocred:
        raise ValueError("Access and secret keys are required if any path is on MinIO.")
    
    # Set the environment variables
    if isminio:
        os.environ["MINIO_ACCESS_KEY"] = MINIO_ACCESS_KEY
        os.environ["MINIO_SECRET_KEY"] = MINIO_SECRET_KEY
        os.environ["MINIO_ENDPOINT_URL"] = MINIO_ENDPOINT_URL
    
    image2ts_pipeline(input_path=input_path,
                      extension=extension,
                      output_path=output_path,
                      field_path=field_path,
                      skip_pixel=skip_pixel)

    



