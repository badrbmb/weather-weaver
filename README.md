# Weather-weaver
Python package to fetch, process and store numercial weather predictions from different providers.

##  Overview

`weather-weaver` is a python package designed to help download weather predictions, process them using dask, pandas and xarray libraries, and store the processed data in Zarr and parquet format.

The project is structured following an (attempt of) hexagonal architecture pattern, with the core functionality of the project housed under `weather_weaver/services`, interfaces defined under `weather_weaver/models` and i/o and external actors defined under `weather_weaver/inputs` and `weather_weaver/outputs` respectively.

Inputs from NWP data providers currently implemented under `weather_weaver/inputs` are from ECMWF, covering both `HRES` and `ensemble forecasts` (expired and true forecasts - from the open-data initiative) as well as historical `ERA5 reanalysis` results (from the  Copernicus Data Store).

---

##  Repository Structure

```sh
└── weather-weaver/
    ├── notebooks
    ├── pyproject.toml
    └── weather_weaver
        ├── config.py
        ├── constants.py
        ├── inputs
        │   └── ecmwf
        │       ├── cds
        │       │   ├── constants.py
        │       │   ├── fetcher.py
        │       │   ├── processor.py
        │       │   └── request.py
        │       ├── constants.py
        │       ├── open_data
        │       │   ├── constants.py
        │       │   ├── fetcher.py
        │       │   └── request.py
        │       └── processor.py
        ├── main.py
        ├── models
        │   ├── fetcher.py
        │   ├── geo.py
        │   ├── processor.py
        │   ├── request.py
        │   └── storage.py
        ├── outputs
        │   └── localfs
        │       └── client.py
        ├── services
        │   └── service.py
        └── utils.py
```

---

##  Main Modules

<details closed><summary>weather_weaver</summary>

| File                                                                                              | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ---                                                                                               | ---                                                                                        
| [main.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/main.py)           | The `main.py` file in the `weather_weaver` directory is the entry point for the Weather Weaver application. It defines the command-line interface (CLI) method `download_datasets` for downloading weather datasets. The method takes various parameters such as the start date, data source, geographical area, storage location, and cluster type. It uses other modules and classes in the codebase to fetch, process, and store the datasets. The method also starts a local Dask cluster to parallelize the data download process. |

</details>

<details closed><summary>weather_weaver.models</summary>

| File                                                                                                     | Summary                                                                                                                                                                                                                                                                                                                                                                                                         |
| ---                                                                                                      | ---                                                                                                                                                                                                                                                                                                                                                                                                             |
| [geo.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/models/geo.py)             | The `geo.py` code snippet in the `weather-weaver` repository is responsible for handling geographical data. It includes functions for downloading world countries' boundaries, loading them into a geodataframe, filtering datasets based on longitude, and filtering dask dataframes. Additionally, it provides methods for creating a `GeoFilterModel` instance using a bounding box or a list of ISO3 codes. |
| [request.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/models/request.py)     | The code snippet in weather_weaver/models/request.py defines abstract base classes and methods for building requests. It provides a base request model and a request builder that can create default or closest requests based on specified criteria. This code plays a crucial role in generating requests for fetching weather data in the parent repository's architecture.                                  |
| [processor.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/models/processor.py) | The code snippet in `processor.py` is part of the `weather-weaver` repository. It defines the `BaseProcessor` abstract class with a `transform` method that processes a raw file and optionally applies a GeoFilterModel to tag countries.                                                                                                                                                                      |
| [storage.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/models/storage.py)     | The `StorageInterface` in `storage.py` defines a generic interface for storing fetched NWP (Numerical Weather Prediction) data. It provides methods to check if a file exists, validate file size, list available files for requests, store datasets, and delete records.                                                                                                                                       |
| [fetcher.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/models/fetcher.py)     | The code in fetcher.py in the models directory of the weather-weaver repository provides an interface for fetching and converting NWP data from external sources. It includes functions for listing available files and downloading a single file.                                                                                                                                                              |

</details>

<details closed><summary>weather_weaver.services</summary>

| File                                                                                                   | Summary                                                                                                                                                                                                                                                                                             |
| ---                                                                                                    | ---                                                                                                                                                                                                                                                                                                 |
| [service.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/services/service.py) | The code snippet is a service class for downloading and processing weather datasets. It implements methods for building requests, filtering new requests, downloading, processing, and storing the datasets. The code uses Dask for parallel processing and provides logging for tracking progress. |

</details>

<details closed><summary>weather_weaver.outputs.localfs</summary>

| File                                                                                                        | Summary                                                                                                                                                                                                                                                                       |
| ---                                                                                                         | ---                                                                                                                                                                                                                                                                           |
| [client.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/outputs/localfs/client.py) | This code snippet in `weather_weaver/outputs/localfs/client.py` is responsible for handling storage and retrieval of data in a local file system. It provides functionalities to check file existence and validity, list available files, store datasets, and delete objects. |

</details>

<details closed><summary>weather_weaver.inputs.ecmwf</summary>

| File                                                                                                           | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ---                                                                                                            | ---                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| [constants.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/inputs/ecmwf/constants.py) | This code snippet defines constants and file paths related to the ECMWF data source in the weather-weaver repository. It specifies the data source, parameters to download, forecast steps, and output directories. It also includes a coordinate allow list for the raw datasets.                                                                                                                                                      |
| [processor.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/inputs/ecmwf/processor.py) | This code snippet is part of the weather-weaver repository and is located in the `weather_weaver/inputs/ecmwf/processor.py` file. It contains the `ECMWFProcessor` class, which is responsible for loading and processing raw weather data from ECMWF. It provides methods for loading, pre-processing, merging, and post-processing datasets. It also has a `transform` method for transforming the raw data into a dask GeoDataFrame. |

</details>

<details closed><summary>weather_weaver.inputs.ecmwf.open_data</summary>

| File                                                                                                                     | Summary                                                                                                                                                                                                                                                                               |
| ---                                                                                                                      | ---                                                                                                                                                                                                                                                                                   |
| [constants.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/inputs/ecmwf/open_data/constants.py) | The code snippet in the file `weather_weaver/inputs/ecmwf/open_data/constants.py` defines constants related to downloading ECMWF open data. It specifies the data source, a list of parameters to download, and the forecast steps.                                                   |
| [request.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/inputs/ecmwf/open_data/request.py)     | The code snippet in `weather_weaver/inputs/ecmwf/open_data/request.py` defines request objects and request builder for retrieving data from ECMWF Open Data. It allows building default requests for a given run date and generating requests closest to the desired target run date. |
| [fetcher.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/inputs/ecmwf/open_data/fetcher.py)     | This code snippet is a fetcher module for the weather-weaver repository. It handles fetching raw weather data from an ECMWF open data source. The module lists all raw files matching a request and downloads them to a specified directory.                                          |

</details>

<details closed><summary>weather_weaver.inputs.ecmwf.cds</summary>

| File                                                                                                               | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ---                                                                                                                | ---                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| [constants.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/inputs/ecmwf/cds/constants.py) | The code snippet in `constants.py` defines constants and lists used in the ECMWF module of the weather-weaver repository, such as dataset names, NWP parameters, default months, days, times, and allowed coordinate names.                                                                                                                                                                                                                                                                                                     |
| [request.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/inputs/ecmwf/cds/request.py)     | This code snippet defines the `ECMWFCDSRequest` class and `ECMWFCDSRequestBuilder` class. The `ECMWFCDSRequest` class represents a request to the ECMWF CDS API for weather data, with properties and methods for creating a request compatible with the API. The `ECMWFCDSRequestBuilder` class provides methods for building default and closest requests based on certain criteria. These classes play a critical role in handling requests for weather data from the ECMWF CDS API in the parent repository's architecture. |
| [processor.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/inputs/ecmwf/cds/processor.py) | This code snippet is a part of the larger weather-weaver repository. It contains the implementation of the EMCWFCDSProcessor class, which provides methods for loading, pre-processing, processing, and post-processing weather datasets. It also includes a transform method for processing raw files and returning a dask_geopandas GeoDataFrame.                                                                                                                                                                             |
| [fetcher.py](https://github.com/badrbmb/weather-weaver/blob/master/weather_weaver/inputs/ecmwf/cds/fetcher.py)     | The `ECMWFCDSFetcher` class in the file `fetcher.py` is responsible for downloading raw weather data files from the Copernicus CDS server. It handles checking if a file already exists and is of valid size before downloading. The downloaded files are stored in a specified directory.                                                                                                                                                                                                                                      |

</details>

---

##  Getting Started


###  Installation

Make sure you're ideally working on a new virtual env with Poetry installed. Install the dependencies with:

```sh
poetry install
```

###  Running weather-weaver

If you're using vs code, you might find some usefull launch scripts in `.vscode/launch.json`, otherwise feel free to have a look at `weather_weaver/main.py` help running:

```sh
python weather_weaver/main.py --help
```
###  Tests

To execute tests, run:

```sh
pytest
```