from weather_weaver.config import DATA_DIR

DATA_SOURCE = "azure"

NWP_PARAMETERS = [
    "2t",
    "tp",
    "10u",
    "10v",
]  # list of parameters to download limited by what is available in the open-data catalogue
# read more about request parameters https://github.com/ecmwf/ecmwf-opendata?tab=readme-ov-file#parameters-and-levels
FORECAST_STEPS = [3 * i for i in range(31)]  # limit to 90 hourly steps by default

ECMWF_DIR = DATA_DIR / "ecmwf"
ECMWF_DIR.mkdir(exist_ok=True)

RAW_DIR = ECMWF_DIR / "raw"
RAW_DIR.mkdir(exist_ok=True)

PROCESSED_DIR = ECMWF_DIR / "processed"
PROCESSED_DIR.mkdir(exist_ok=True)
