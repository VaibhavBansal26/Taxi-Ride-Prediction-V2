import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..")))
from pathlib import Path
import requests
from config.config import RAW_DATA_DIR


def fetch_raw_data(year: int, month: int) -> Path:
    URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02}.parquet"
    response = requests.get(URL)
    if response.status_code == 200:
        path = RAW_DATA_DIR / f"rides_{year}_{month:02}.parquet"
        open(path, "wb").write(response.content)
        return path
    else:
        raise Exception(f"{URL} is not available")
