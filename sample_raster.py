#!/usr/bin/env python3
import argparse
import sys
import os
import rasterio
import numpy as np
import json
from minio import Minio
from rasterio.windows import Window

def get_minio_client(credentials_file_path):
    """Initializes and returns a Minio client."""
    try:
        with open(credentials_file_path, 'r') as f:
            credentials = json.load(f)
        
        # The endpoint URL for the Minio client, scheme (http/https) is removed as in the notebook.
        # The Minio client determines 'secure' based on the endpoint or its 'secure' parameter.
        # By default, Minio client tries HTTPS if endpoint suggests it, or if secure=True.
        # If no scheme, it might default to insecure or a specific port.
        # For robustness, one might parse the original URL and set 'secure' explicitly.
        # However, we follow the notebook's pattern for URL preparation.
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

def get_sample_window(src_height, src_width, sample_size_px):
    """Calculates the window for a centered sample."""
    # Calculate center coordinates
    center_x = src_width // 2
    center_y = src_height // 2

    # Calculate half size of the sample
    half_size = sample_size_px // 2

    # Calculate column and row offset for the top-left corner of the window
    # Ensure offset is not negative (i.e., starts at or after 0)
    col_off = max(0, center_x - half_size)
    row_off = max(0, center_y - half_size)
    
    # Calculate actual width and height of the window
    # It's the minimum of the sample_size_px and the available dimension from the offset
    actual_width = min(sample_size_px, src_width - col_off)
    actual_height = min(sample_size_px, src_height - row_off)

    if actual_width <= 0 or actual_height <= 0:
        raise ValueError(
            f"Calculated sample dimensions are non-positive: "
            f"width={actual_width}, height={actual_height}. "
            f"Image size: {src_width}x{src_height}, requested sample_size: {sample_size_px}"
        )
            
    return Window(col_off, row_off, actual_width, actual_height), actual_width, actual_height

def main():
    parser = argparse.ArgumentParser(description="Samples raster files from Minio and saves them locally.")
    parser.add_argument("--bucket_name", required=True, help="Minio bucket name.")
    parser.add_argument("--prefix", required=True, help="Prefix for files in Minio bucket (e.g., 'path/to/files/').")
    parser.add_argument("--suffix", default=".TIF", help="Suffix for files to process (e.g., '.TIF', case-sensitive). Default: .TIF")
    parser.add_argument("--sample_size", type=int, default=100, help="Size (pixels) of the square sample to take from the center. Default: 100.")
    parser.add_argument("--output_dir", required=True, help="Minio subdirectory name within the original file\\'s path to save samples (e.g., \\'small\\').")
    parser.add_argument("--credentials_file", default="resources/credentials.json", help="Path to Minio credentials JSON file. Default: resources/credentials.json.")

    # Default run
    if len(sys.argv) == 1:
        print("No arguments provided. Running with default parameters.")
        sys.argv.extend([
            "--bucket_name", "vista-bucket",
            "--prefix", "Pilot_B/UCB2/33UUP/DATA_FUSION/",
            "--suffix", ".TIF",
            "--sample_size", "1128",
            "--output_dir", "small",
            "--credentials_file", "resources/credentials.json"
        ])
    
    args = parser.parse_args()

    if args.sample_size <= 0:
        print("Error: sample_size must be a positive integer.", file=sys.stderr)
        sys.exit(1)

    minio_client = get_minio_client(args.credentials_file)
    
    print(f"Listing files in bucket '{args.bucket_name}' with prefix '{args.prefix}' and suffix '{args.suffix}'...")
    
    try:
        objects_generator = minio_client.list_objects(
            bucket_name=args.bucket_name,
            prefix=args.prefix,
            recursive=True 
        )
        
        files_to_process = [
            obj for obj in objects_generator 
            if obj.object_name.endswith(args.suffix) and not obj.is_dir
        ]
    except Exception as e:
        print(f"Error listing objects from Minio: {e}", file=sys.stderr)
        sys.exit(1)

    if not files_to_process:
        print(f"No files found matching prefix '{args.prefix}' and suffix '{args.suffix}' in bucket '{args.bucket_name}'.")
        return

    print(f"Found {len(files_to_process)} files to process.")

    # Window will be defined from the first image
    window = None
    actual_width = None
    actual_height = None
    
    processed_count = 0
    skipped_count = 0

    for i, minio_object in enumerate(files_to_process, 1):
        object_name = minio_object.object_name
        base_name = os.path.basename(object_name)
        
        # Check if output file already exists
        name_part, orig_ext = os.path.splitext(base_name)
        output_ext = args.suffix
        if not output_ext.startswith('.'): 
            if orig_ext.lower() == ('.' + output_ext.lower()): 
                output_ext = orig_ext 
            else: 
                output_ext = '.' + output_ext
        
        sample_base_name = f"{name_part}_sample{output_ext}"
        minio_output_object_name = os.path.join(
            os.path.dirname(object_name), 
            args.output_dir, 
            sample_base_name
        ).replace(os.sep, '/') # Ensure forward slashes for Minio
        
        # Check if output already exists
        try:
            minio_client.stat_object(bucket_name=args.bucket_name, object_name=minio_output_object_name)
            print(f"[{i}/{len(files_to_process)}] Skipping {object_name} - output already exists: {minio_output_object_name}")
            skipped_count += 1
            continue
        except Exception:
            # Object doesn't exist, proceed with processing
            pass
        
        local_temp_download_path = os.path.join('/tmp', base_name)

        print(f"[{i}/{len(files_to_process)}] Processing {object_name}...")
        try:
            minio_client.fget_object(
                bucket_name=args.bucket_name,
                object_name=object_name,
                file_path=local_temp_download_path
            )
            print(f"Downloaded to {local_temp_download_path}")

            with rasterio.open(local_temp_download_path) as src:
                if src.width == 0 or src.height == 0:
                    print(f"Skipping {base_name} as it has zero width or height.")
                    continue

                # Define window from the first image if not already defined
                if window is None:
                    window, actual_width, actual_height = get_sample_window(
                        src.height, src.width, args.sample_size
                    )
                    print(f"Using window: {window} (size: {actual_width}x{actual_height}) for all images")

                # Use the pre-calculated window from the first image
                
                data = src.read(window=window)
                
                window_transform = src.window_transform(window)
                new_profile = src.profile.copy()
                new_profile.update({
                    'height': actual_height,
                    'width': actual_width,
                    'transform': window_transform,
                    'compress': new_profile.get('compress', 'lzw') 
                })
                
                local_temp_sample_path = os.path.join('/tmp', sample_base_name)

                with rasterio.open(local_temp_sample_path, 'w', **new_profile) as dst:
                    dst.write(data)

                print(f"[{i}/{len(files_to_process)}] Uploading sample to Minio: {args.bucket_name}/{minio_output_object_name}")
                minio_client.fput_object(
                    bucket_name=args.bucket_name,
                    object_name=minio_output_object_name,
                    file_path=local_temp_sample_path,
                )
                print(f"[{i}/{len(files_to_process)}] Uploaded sample to {args.bucket_name}/{minio_output_object_name}")
                
                # Clean up temporary files
                os.remove(local_temp_download_path)
                os.remove(local_temp_sample_path)
                
                processed_count += 1

        except ValueError as ve: 
            print(f"[{i}/{len(files_to_process)}] Skipping {base_name} due to value error during window calculation: {ve}", file=sys.stderr)
            skipped_count += 1
        except Exception as e:
            print(f"[{i}/{len(files_to_process)}] Error processing file {object_name}: {e}", file=sys.stderr)
            skipped_count += 1

    print(f"Processing complete. Processed: {processed_count}, Skipped: {skipped_count}, Total: {len(files_to_process)}")

if __name__ == "__main__":
    main()
