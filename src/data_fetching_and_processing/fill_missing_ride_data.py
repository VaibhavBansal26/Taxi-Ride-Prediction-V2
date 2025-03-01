import pandas as pd

def fill_missing_ride_data(df, hour_col, location_col, zone_col, rides_col):
   
    df[hour_col] = pd.to_datetime(df[hour_col])

    full_hours = pd.date_range(
        start=df[hour_col].min(), end=df[hour_col].max(), freq="h"
    )

    all_locations = df[location_col].unique()

    full_combinations = pd.DataFrame(
        [(hour, location, df.loc[df[location_col] == location, zone_col].values[0]) for hour in full_hours for location in all_locations],
        columns=[hour_col, location_col, zone_col],
    )

    merged_df = pd.merge(full_combinations, df, on=[hour_col, location_col, zone_col], how="left")

    merged_df[rides_col] = merged_df[rides_col].fillna(0).astype(int)

    return merged_df