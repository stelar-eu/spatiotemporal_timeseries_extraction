#!/usr/bin/env python3
import argparse
import sys
import os
import rasterio
import numpy as np
import json
from minio import Minio
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.windows import from_bounds
from rasterio.mask import mask
import glob
from pathlib import Path

def get_minio_client(credentials_file_path):
    """Initializes and returns a Minio client."""
    try:
        with open(credentials_file_path, 'r') as f:
            credentials = json.load(f)
        
        endpoint_url = credentials["url"].replace("https://", "").replace("http://", "")
        access_key = credentials["accessKey"]
        secret_key = credentials["secretKey"]
        
        # Determine if connection should be secure based on original URL
        secure_connection = credentials["url"].startswith("https://")

        client = Minio(
            endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure_connection 
        )
        # Test connection by trying to list buckets (lightweight operation)
        client.list_buckets() 
        print(f"Successfully connected to Minio at {credentials['url']}")
        return client
    except FileNotFoundError:
        print(f"Error: Credentials file not found at {credentials_file_path}", file=sys.stderr)
        sys.exit(1)
    except KeyError as e:
        print(f"Error: Malformed credentials file {credentials_file_path}. Missing key: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error connecting to Minio or validating connection: {e}", file=sys.stderr)
        sys.exit(1)

def get_reference_info(minio_client, bucket_name, reference_object_name):
    """Extract CRS and bounds from reference image."""
    try:
        # Download reference image to temporary location
        ref_temp_path = os.path.join('/tmp', os.path.basename(reference_object_name))
        minio_client.fget_object(
            bucket_name=bucket_name,
            object_name=reference_object_name,
            file_path=ref_temp_path
        )
        
        with rasterio.open(ref_temp_path) as ref_src:
            ref_crs = ref_src.crs
            ref_bounds = ref_src.bounds
            ref_transform = ref_src.transform
            ref_width = ref_src.width
            ref_height = ref_src.height
            
            print(f"Reference image info:")
            print(f"  CRS: {ref_crs}")
            print(f"  Bounds: {ref_bounds}")
            print(f"  Size: {ref_width}x{ref_height}")
            
            # Clean up temporary file
            os.remove(ref_temp_path)
            
            return {
                'crs': ref_crs,
                'bounds': ref_bounds,
                'transform': ref_transform,
                'width': ref_width,
                'height': ref_height
            }
    except Exception as e:
        print(f"Error reading reference image {reference_object_name}: {e}", file=sys.stderr)
        sys.exit(1)

def crop_image_to_reference(minio_client, bucket_name, input_object_name, output_object_name, ref_info):
    """Crop an image to match the reference CRS and bounds."""
    try:
        # Download input image to temporary location
        input_temp_path = os.path.join('/tmp', os.path.basename(input_object_name))
        minio_client.fget_object(
            bucket_name=bucket_name,
            object_name=input_object_name,
            file_path=input_temp_path
        )
        
        with rasterio.open(input_temp_path) as src:
            # Check if reprojection is needed
            if src.crs != ref_info['crs']:
                print(f"  Reprojecting from {src.crs} to {ref_info['crs']}")
                
                # Calculate transform for reprojection to reference CRS
                transform, width, height = calculate_default_transform(
                    src.crs, ref_info['crs'], src.width, src.height, *src.bounds
                )
                
                # Create temporary reprojected data
                temp_profile = src.profile.copy()
                temp_profile.update({
                    'crs': ref_info['crs'],
                    'transform': transform,
                    'width': width,
                    'height': height
                })
                
                # Reproject all bands
                reprojected_data = np.zeros((src.count, height, width), dtype=src.dtypes[0])
                
                for band_idx in range(1, src.count + 1):
                    reproject(
                        source=rasterio.band(src, band_idx),
                        destination=reprojected_data[band_idx - 1],
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=ref_info['crs'],
                        resampling=Resampling.bilinear
                    )
                
                # Calculate window for cropping to reference bounds
                try:
                    window = from_bounds(*ref_info['bounds'], transform)
                    
                    # Ensure window is within the reprojected image bounds
                    window = window.intersection(rasterio.windows.Window(0, 0, width, height))
                    
                    if window.width <= 0 or window.height <= 0:
                        print(f"  Warning: No overlap between {input_object_name} and reference bounds")
                        os.remove(input_temp_path)
                        return False
                    
                    # Read data from the window
                    row_start, row_stop = int(window.row_off), int(window.row_off + window.height)
                    col_start, col_stop = int(window.col_off), int(window.col_off + window.width)
                    
                    # Ensure indices are within bounds
                    row_start = max(0, row_start)
                    row_stop = min(height, row_stop)
                    col_start = max(0, col_start)
                    col_stop = min(width, col_stop)
                    
                    cropped_data = reprojected_data[:, row_start:row_stop, col_start:col_stop]
                    
                    # Calculate new transform for the cropped window
                    new_transform = rasterio.windows.transform(window, transform)
                    
                except Exception as e:
                    print(f"  Error calculating crop window: {e}")
                    os.remove(input_temp_path)
                    return False
                    
            else:
                # Same CRS, just crop to bounds
                print(f"  Same CRS, cropping to reference bounds")
                
                try:
                    window = from_bounds(*ref_info['bounds'], src.transform)
                    
                    # Ensure window is within the source image bounds
                    window = window.intersection(rasterio.windows.Window(0, 0, src.width, src.height))
                    
                    if window.width <= 0 or window.height <= 0:
                        print(f"  Warning: No overlap between {input_object_name} and reference bounds")
                        os.remove(input_temp_path)
                        return False
                    
                    # Read data from the window
                    cropped_data = src.read(window=window)
                    new_transform = src.window_transform(window)
                    
                except Exception as e:
                    print(f"  Error cropping image: {e}")
                    os.remove(input_temp_path)
                    return False
            
            # Create output profile
            output_profile = src.profile.copy()
            output_profile.update({
                'crs': ref_info['crs'],
                'transform': new_transform,
                'width': cropped_data.shape[2],
                'height': cropped_data.shape[1],
                'compress': 'lzw'  # Add compression to save space
            })
            
            # Write cropped image to temporary file
            output_temp_path = os.path.join('/tmp', f"cropped_{os.path.basename(input_object_name)}")
            with rasterio.open(output_temp_path, 'w', **output_profile) as dst:
                dst.write(cropped_data)
            
            # Upload cropped image to Minio
            minio_client.fput_object(
                bucket_name=bucket_name,
                object_name=output_object_name,
                file_path=output_temp_path
            )
            
            print(f"  Successfully cropped and uploaded to {output_object_name} (size: {cropped_data.shape[2]}x{cropped_data.shape[1]})")
            return True
            
    except Exception as e:
        print(f"  Error processing {input_object_name}: {e}", file=sys.stderr)
        return False

def main():
    parser = argparse.ArgumentParser(description="Crop TIF images to match a reference image's CRS and bounding box.")
    parser.add_argument("--bucket_name", required=True, help="Minio bucket name.")
    parser.add_argument("--reference", required=True, help="Minio object path to reference TIF image.")
    parser.add_argument("--input_prefix", required=True, help="Prefix for input TIF images in Minio bucket.")
    parser.add_argument("--output_prefix", required=True, help="Prefix for output TIF images in Minio bucket.")
    parser.add_argument("--suffix", default=".TIF", help="File suffix to process (case-sensitive). Default: .TIF")
    parser.add_argument("--credentials_file", default="resources/credentials.json", help="Path to Minio credentials JSON file. Default: resources/credentials.json.")

    # Default run
    if len(sys.argv) == 1:
        sys.argv = [
            sys.argv[0],
            "--bucket_name", "vista-bucket",
            "--reference", "Pilot_B/UCB2/30TYQ/06/small/S2A_30TYQATO_220622_IC_sample.TIF",
            "--input_prefix", "Pilot_B/UCB2/30TYQ/06/S2B_30TYQ2BP_220614_R",
            "--output_prefix", "Pilot_B/UCB2/30TYQ/06/small/",
            "--suffix", ".TIF",
            "--credentials_file", "resources/credentials.json"
        ]

    
    args = parser.parse_args()
    
    # Get Minio client
    minio_client = get_minio_client(args.credentials_file)
    
    # Validate reference image exists
    try:
        minio_client.stat_object(bucket_name=args.bucket_name, object_name=args.reference)
    except Exception:
        print(f"Error: Reference image not found in bucket: {args.reference}", file=sys.stderr)
        sys.exit(1)
    
    # Get reference image information
    print(f"Reading reference image: {args.reference}")
    ref_info = get_reference_info(minio_client, args.bucket_name, args.reference)
    
    # Find input files in Minio
    try:
        objects_generator = minio_client.list_objects(
            bucket_name=args.bucket_name,
            prefix=args.input_prefix,
            recursive=True 
        )
        
        input_files = [
            obj for obj in objects_generator 
            if obj.object_name.endswith(args.suffix) and not obj.is_dir and obj.object_name != args.reference
        ]
    except Exception as e:
        print(f"Error listing objects from Minio: {e}", file=sys.stderr)
        sys.exit(1)
    
    if not input_files:
        print(f"No files found with prefix '{args.input_prefix}' and suffix '{args.suffix}' in bucket '{args.bucket_name}'.", file=sys.stderr)
        sys.exit(1)
    
    print(f"Found {len(input_files)} files to process.")
    
    processed_count = 0
    skipped_count = 0
    
    for i, minio_object in enumerate(input_files, 1):
        input_object_name = minio_object.object_name
        filename = os.path.basename(input_object_name)
        
        output_object_name = os.path.join(args.output_prefix, filename).replace(os.sep, '/')
        
        # Skip if output already exists
        try:
            minio_client.stat_object(bucket_name=args.bucket_name, object_name=output_object_name)
            print(f"[{i}/{len(input_files)}] Skipping {filename} - output already exists: {output_object_name}")
            skipped_count += 1
            continue
        except Exception:
            # Object doesn't exist, proceed with processing
            pass
        
        print(f"[{i}/{len(input_files)}] Processing {filename}...")
        
        success = crop_image_to_reference(minio_client, args.bucket_name, input_object_name, output_object_name, ref_info)
        
        if success:
            processed_count += 1
        else:
            skipped_count += 1
    
    print(f"Processing complete. Processed: {processed_count}, Skipped: {skipped_count}, Total: {len(input_files)}")

if __name__ == "__main__":
    main()
