DATASET_NAME = "reanalysis-era5-single-levels"

NWP_PARAMETERS = [
    "2m_temperature",
    "total_precipitation",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
]

DEFAULT_MONTHS = [str(t) for t in range(1, 13)]

DEFAULT_DAYS = [str(t) for t in range(1, 32)]

DEFAULT_TIMES = [f"{str(t).zfill(2)}:00" for t in range(24)]

COORDINATE_ALLOW_LIST: list[str] = ["valid_time", "latitude", "longitude"]
