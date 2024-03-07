import os
import glob
import sys
from sentinelhub import CRS
from typing import List
from stelar_spatiotemporal.preprocessing.preprocessing import combine_npys_into_eopatches, max_partition_size
from stelar_spatiotemporal.preprocessing.vista_preprocessing import unpack_vista_unzipped
from stelar_spatiotemporal.preprocessing.timeseries import lai_to_csv_px, lai_to_csv_field
from stelar_spatiotemporal.lib import load_bbox, get_filesystem
import argparse

parser = argparse.ArgumentParser(description='Convert raster images to time series dataset')
   
def parse_args(args:List[str]) -> dict:
    pixel = True
    field = True

    # Check if unkonwn arguments are given
    for arg in args[1:]:
        if arg.startswith("-"):
            if arg not in ["-skippx", "-skipfields"]:
                print("Unknown argument: {}".format(arg))
                sys.exit(1)


    # Look if pixel or field needs to be skipped
    if "-skippx" in args:
        pixel = False
        args.remove("-skippx")
    if "-skipfields" in args:
        field = False
        args.remove("-skipfields")

    # If skip field no field path is needed
    error_str = "Usage: python image2ts_pipeline.py <path_to_ras1>,...,<path_to_rasn> <path_to_rhd1>,...<path_to_rhdn> <path_to_out>"
    if not field:
        fields_path = None
    else:
        # Look if field path is given
        if len(args) < 5:
            print(error_str, "<path_to_fields>")
            sys.exit(1)
        fields_path = args[4]

    if len(args) < 4:
        print(error_str)
        sys.exit(1)

    ras_paths = args[1].split(",")
    rhd_paths = args[2].split(",")
    out_path = args[3]

    args_dict = {"ras_paths":ras_paths, 
                 "rhd_paths":rhd_paths, 
                 "out_path":out_path, 
                 "fields_path":fields_path, 
                 "pixel":pixel, 
                 "field":field}
    
    print("Arguments: {}".format(args_dict))
    
    return args_dict

def unpacking(ras_paths:List[str], rhd_paths:List[str], out_path:str):
    for ras_path, rhd_path in zip(ras_paths, rhd_paths):
        unpack_vista_unzipped(ras_path, rhd_path, out_path, delete_after=False, crs=CRS('32630'))

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


def image2ts_pipeline(input_path:str, output_path:str, field_path:str, 
                      skip_pixel:bool):
    """
    This function takes a directory of raster files and headers and converts them to time series dataset.

    Parameters
    ----------
    input_path : str
        Path to the folder containing the input images as RAS and RHD files.
    output_path : str
        Path to the folder where the output files will be saved.
    field_path : str
        Path to the shapefile containing the boundaries of the agricultural fields. If not given, field-level time series will not be created.
    skip_pixel : bool
        Skip creating pixel-level time series
    """
    # Get the ras and rhd paths
    fs = get_filesystem(input_path)
    ras_paths = fs.glob(os.path.join(input_path, "*.RAS"))
    rhd_paths = fs.glob(os.path.join(input_path, "*.RHD"))

    # Check if we do pixel, field, or both
    pixel = not skip_pixel
    field = field_path is not None

    # Check if rhd_paths match with ras_paths
    if len(ras_paths) != len(rhd_paths):
        raise ValueError("Length of ras_paths and rhd_paths should be equal.")
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
        
    TMP_PATH = '/tmp'
        
    # 1. Unpack the RAS files
    print("1. Unpacking RAS files...")
    npy_dir = os.path.join(TMP_PATH, "npys")
    unpacking(ras_paths=ras_paths_match, 
              rhd_paths=rhd_paths_match, 
              out_path=npy_dir)
    
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
        create_field_ts(out_path=field_ts_path, 
                        fields_path=field_path)

    # 5. Cleanup
    print("5. Cleaning up...")
    cleanup(TMP_PATH)
    

parser.add_argument("-i", "--input_path", 
                    type=str, 
                    required=True,
                    help="Path to the folder containing the input images as RAS and RHD files.")
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
        input_path = "s3://stelar-spatiotemporal/LAI_small"
        output_path = "s3://stelar-spatiotemporal"
        field_path = None
        skip_pixel = False
        MINIO_ACCESS_KEY = "minioadmin"
        MINIO_SECRET_KEY = "minioadmin"
        MINIO_ENDPOINT_URL = "http://localhost:9000"
    else:
        args = parser.parse_args()
        input_path = args.input_path
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
                      output_path=output_path,
                      field_path=field_path,
                      skip_pixel=skip_pixel)

    



