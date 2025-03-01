import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..")))
import pandas as pd
from typing import List, Optional
from config.config import RAW_DATA_DIR
from .fetch_raw_data import fetch_raw_data
from .filter_data import filter_data
from src.utils.data_utils.process_zone_data import process_zone_data

def load_and_process_data(
    year: int, months: Optional[List[int]] = None
) -> pd.DataFrame:
   
    if months is None:
        months = list(range(1, 13))

    monthly_rides = []

    for month in months:
        file_path = RAW_DATA_DIR / f"rides_{year}_{month:02}.parquet"

        try:
            if not file_path.exists():
                print(f"Downloading data for {year}-{month:02}...")
                fetch_raw_data(year, month)
                print(f"Successfully downloaded data for {year}-{month:02}.")
            else:
                print(f"File already exists for {year}-{month:02}.")

            print(f"Loading data for {year}-{month:02}...")
            rides = pd.read_parquet(file_path, engine="pyarrow")

            rides = filter_data(rides, year, month)
            print(f"Successfully processed data for {year}-{month:02}.")

            zones = process_zone_data()
            rides = pd.merge(rides,zones, how="left", on="pickup_location_id")

            monthly_rides.append(rides)

        except FileNotFoundError:
            print(f"File not found for {year}-{month:02}. Skipping...")
        except Exception as e:
            print(f"Error processing data for {year}-{month:02}: {str(e)}")
            continue

    # Combine all monthly data
    if not monthly_rides:
        raise Exception(
            f"No data could be loaded for the year {year} and specified months: {months}"
        )

    print("Combining all monthly data...")
    combined_rides = pd.concat(monthly_rides, ignore_index=True)
    print("Data loading and processing complete!")

    return combined_rides