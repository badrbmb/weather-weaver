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
