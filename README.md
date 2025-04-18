# Spatio-Temporal Time Series Extraction Module
## Description
Spatio-Temporal time series extraction module, as part of STELAR Work Package 3 (WP3), Task 3.3: Spatio-temporal data alignment.
This module takes as input a series of satellite images with crop/soil statistics such as the Leaf Area Index (LAI), and outputs a time series of statistics for each pixel in the image. The time series can later be used as input for a classification or prediction model, or for further (pre-)processing modules such as missing value imputation.

Additionally, the module can take as input a shapefile containing the boundaries of agricultural fields and output an aggregated time series of statistics for each field. Such spatial aggregation is relevant because complex imputation algorithms often depend on the number of time series in the dataset, and reducing this number can therefore lead to significant improvements in efficiency. 
Also note that aggregation of pixel values within a field can be seen as a spatial imputation method in itself; when clouds cover only part of a certain field at a certain time step, the aggregated value of the field can be used as an approximation of the values of the covered pixels.

This module is part of the KLMS Tools in the STELAR architecture, which can be invoked by the workflow execution engines.
Due to its benefits with respect to both quality and efficiency of downstream tasks, the module can be used in combination with a variety of other tools, to form a workflow for several STELAR use cases. 
For instance, the module can be combined with the [Field Segmentation Module](https://github.com/stelar-eu/spatiotemporal_timeseries_extraction) (described in Deliverable 4.1 of the project) and the [Data Imputation Module](TODO:LINK) (see Section 2.2.3) to create dense LAI time series on both a pixel and field level, that can be subsequently used for efficient crop classification and yield prediction with the respective modules (see Deliverables 4.1 and 4.2). 
All these tasks are relevant for use case *B.1 Yield prediction for agricultural decision making* and *B.2 Food security through Early Warnings*. 
Therefore, time series extraction can play a crucial role in the quality and scalability of the final workflows for these use cases

|  |  |
| --- | --- |
| Context in project: | WP3 Task 3.3: Spatio-temporal data alignment |
| Relevant other tools: | [Field Segmentation Module](https://github.com/stelar-eu/field_segmentation), [Data Imputation Module](TODO:LINK) |
| Relevant use cases: | B.1 Yield prediction for agricultural decision making, B.2 Food security through Early Warnings |
| Report: | Deliverable 3.2 Spatio-temporal data alignment and correlations |

## Input format
The module takes as input the following:
1. *input_path* (required): Path to the folder containing the input images as RAS and RHD files. The input path should have the following structure:
    ```
    input_dir
    ├── image1.RAS
    ├── image1.RHD
    ├── image2.RAS
    └── image2.RHD
    ```
RAS files are compressed binary files containing the LAI values (or any other crop/land statistic) of satellite images. Each RAS file should have an accompanying header file (.RHD), which contains the metadata of the RAS file such as the bounding box, the coordinate reference system and the timestamps of the images. Based on the header files, the script first checks if the RAS files are aligned, i.e., if they have the same bounding box, coordinate reference system and timestamps. If the RAS files are not aligned, the script will raise an error.
**Note**: The input path can be either a local path or a path to a folder in a MinIO object storage. In the latter case, the MinIO access key, secret key and endpoint url should be passed as arguments (see below).

2. *output_path* (required): Path to the folder where the output files will be saved. 
**Note**: The output path can be either a local path or a path to a folder in a MinIO object storage. In the latter case, the MinIO access key, secret key and endpoint url should be passed as arguments (see below).

3. *field_path* (optional): Path to the shapefile containing the boundaries of the agricultural fields. The shapefile can be one of the following formats: .shp, .gpkg. If this argument is not provided, the script will skip extracting the time series of the fields and only extract the time series of the pixels.

4. *skip_pixel* (optional): Flag indicating whether to skip the extraction of the time series of the pixels.

5. *MINIO_ACCESS_KEY* (optional): Access key of the MinIO server. Required if the input or output path is in a MinIO object storage.

6. *MINIO_SECRET_KEY* (optional): Secret key of the MinIO server. Required if the input or output path is in a MinIO object storage.

7. *MINIO_ENDPOINT_URL* (optional): Endpoint URL of the MinIO server. Required if the input or output path is in a MinIO object storage.

## Output format
The module outputs the following:
1. *Pixel Time Series*: A folder containing (potentially multiple) CSV files with the time series of a set of pixels, in column-major format (i.e., each row corresponds to a timestamp and each column corresponds to a pixel). The folder has the following structure:
    ```
    output_dir
    └── pixel_timeseries
        ├── patchlet_0_0.csv
        ├── patchlet_0_1.csv
        └── ...
    ```
Each CSV file contains the time series for a subset of pixels (i.e., patchlet). The first column of the CSV file contains the timestamps, and the remaining columns contain the LAI values of the pixels. The pixel IDs are the row and column indices of the pixels in the image, e.g., `0_0` for the pixel in the first row and first column, `0_1` for the pixel in the first row and second column, etc.
The CSV files will therefore have the following structure:
| timestamps | 0_0 | 0_1 | ... |
|----------|-------------|-------------|-----|
| 2020-01-01        | 0.5         | 0.6         | ... |
| 2020-01-02        | 0.4         | 0.5         | ... |
| ...      | ...         | ...         | ... |

**Note:** This output is generated if the *skip_pixel* flag is not set.

2. *Field Time Series*: A CSV file containing the time series of the fields, in column-major format (i.e., each row corresponds to a timestamp and each column corresponds to a field). The file is named `field_timeseries.csv` and is saved in the output folder. The CSV file contains the time series for each field, with the first column containing the timestamps, and the remaining columns containing the aggregated LAI values of the fields. The CSV file will therefore have the following structure:
| timestamps | field_0 | field_1 | ... |
|----------|-------------|-------------|-----|
| 2020-01-01        | 0.5         | 0.6         | ... |
| 2020-01-02        | 0.4         | 0.5         | ... |
| ...      | ...         | ...         | ... |

## Metrics
The module outputs the following metrics about the run as metadata:
1. *number_of_images*: The number of input images.
2. *image_width*: The width of the input images.
3. *image_height*: The height of the input images.
4. *total_runtime*: The total runtime of the module in seconds.
5. *partial_runtimes*: A list containing the partial runtimes of the module in seconds.

## Installation & Example Usage
The module can be installed either by (1) cloning the repository and building the Docker image, or (2) by pulling the image from DockerHub.
Cloning the repository and building the Docker image:
```bash
git clone https://github.com/stelar-eu/spatiotemporal_timeseries_extraction.git
cd spatiotemporal_timeseries_extraction
docker build -t alexdarancio7/stelar_image2ts:latest .
```
Pulling the image from DockerHub:
```bash
docker pull alexdarancio7/stelar_image2ts:latest
```
### Example Usage
Then, given we have the following input parameters:
- *input_path*: `bucket/path/to/input_dir`
- *output_path*: `bucket/path/to/output_dir`
- *field_path*: `bucket/path/to/fields.shp`
- *skip_pixel*: `True`
- *MINIO_ACCESS_KEY*: `minio`
- *MINIO_SECRET_KEY*: `minio123`
- *MINIO_ENDPOINT_URL*: `http://localhost:9000`

We can run the module as follows:
```bash
docker run -v path/to/jsons:/app/resources -it \
alexdarancio7/stelar_image2ts resources/input.json resources/output.json
```
where `input.json` is a JSON file formatted as follows:
```json
{
    {
    "docket_image": "alexdarancio7/stelar_image2ts:latest",
    "input": [{
        "path": "bucket/path/to/input_dir",
        "name": "Directory of input TIF files"
    }],
    "parameters": {
        "output_path": "bucket/path/to/output_dir",
        "extension": "TIF"
    },
    "minio": {
        "id": "minio",
        "key": "minio123",
        "endpoint_url": "http://localhost:9000"
    },
    "tags": []
}
}
```
and the `output.json` will look something like this:
```json
{
    "message": "Time series data has been created successfully.",
    "output": [
        {
            "path": "bucket/path/to/output_dir",
            "type": "Directory containing the time series data."
        }
    ],
    "metrics": {
        "number_of_images": 1,
        "image_width": 1128,
        "image_height": 1128,
        "total_runtime": 7.46787166595459,
        "partial_runtimes": [
            {
                "step": "Unpacking RAS files",
                "runtime": 3.8297972679138184
            },
            {
                "step": "Combining the images into eopatches",
                "runtime": 0.02138805389404297
            },
            {
                "step": "Creating pixel-level time series",
                "runtime": 3.614686965942383
            }
        ]
    }
}
```

## License & Acknowledgements
This module is part of the STELAR project, which is funded by the European Union’s Europe research and innovation programme under grant agreement No 101070122.
The module is licensed under the MIT License (see [LICENSE](LICENSE) for details).
