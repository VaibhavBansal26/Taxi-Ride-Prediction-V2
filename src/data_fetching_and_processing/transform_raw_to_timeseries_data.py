import pandas as pd
from .fill_missing_ride_data import fill_missing_ride_data


def transform_raw_to_timeseries_data(rides: pd.DataFrame) -> pd.DataFrame:
  
    rides["pickup_hour"] = rides["pickup_datetime"].dt.floor("h")

    agg_rides = (
        rides.groupby(["pickup_hour", "pickup_location_id", "zone"])
        .size()
        .reset_index(name="rides")
    )

    agg_rides_all_slots = (
        fill_missing_ride_data(
            agg_rides, "pickup_hour", "pickup_location_id","zone","rides"
        )
        .sort_values(["pickup_location_id", "pickup_hour"])
        .reset_index(drop=True)
    )

    # important
    agg_rides_all_slots = agg_rides_all_slots.astype(
        {"pickup_location_id": "int16", "rides": "int16","zone":"str"}
    )

    return agg_rides_all_slots
