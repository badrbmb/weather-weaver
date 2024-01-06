from weather_weaver.config import DATA_DIR

# Data source to download ECMWF open data from
DATA_SOURCE = "azure"

# list of parameters to download limited by what is available in the open-data catalogue
# read more about request parameters https://github.com/ecmwf/ecmwf-opendata?tab=readme-ov-file#parameters-and-levels
NWP_PARAMETERS = [
    "2t",
    "tp",
    "10u",
    "10v",
]

# limit to 90 hourly steps by default
FORECAST_STEPS = [3 * i for i in range(31)]

# Output directories
ECMWF_DIR = DATA_DIR / "ecmwf"
ECMWF_DIR.mkdir(exist_ok=True)

RAW_DIR = ECMWF_DIR / "raw"
RAW_DIR.mkdir(exist_ok=True)

PROCESSED_DIR = ECMWF_DIR / "processed"
PROCESSED_DIR.mkdir(exist_ok=True)

# list of coordinates to keep from raw datasets
COORDINATE_ALLOW_LIST: list[str] = ["time", "step", "latitude", "longitude"]
