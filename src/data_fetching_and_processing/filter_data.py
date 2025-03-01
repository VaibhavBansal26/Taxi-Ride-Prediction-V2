
import pandas as pd

def filter_data(rides: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
  
    if not (1 <= month <= 12):
        raise ValueError("Month must be between 1 and 12.")
    if not isinstance(year, int) or not isinstance(month, int):
        raise ValueError("Year and month must be integers.")

    start_date = pd.Timestamp(year=year, month=month, day=1)
    end_date = pd.Timestamp(year=year + (month // 12), month=(month % 12) + 1, day=1)

    # Ride Duration
    rides["duration"] = rides["tpep_dropoff_datetime"] - rides["tpep_pickup_datetime"]

    # Define filters (Ride duration within 5 hours, total amount greater than 0 and less than 99.9th percentile,
    # NYC location, and within the specified date range)
    duration_filter = (rides["duration"] > pd.Timedelta(0)) & (
        rides["duration"] <= pd.Timedelta(hours=5)
    )
    total_amount_filter = (rides["total_amount"] > 0) & (
        rides["total_amount"] <= rides["total_amount"].quantile(0.999)
    )
    nyc_location_filter = ~rides["PULocationID"].isin((1, 264, 265))
    date_range_filter = (rides["tpep_pickup_datetime"] >= start_date) & (
        rides["tpep_pickup_datetime"] < end_date
    )

    # Combining filters
    final_filter = (
        duration_filter & total_amount_filter & nyc_location_filter & date_range_filter
    )

    # Calculate dropped records
    total_records = len(rides)
    valid_records = final_filter.sum()
    records_dropped = total_records - valid_records
    percent_dropped = (records_dropped / total_records) * 100

    print(f"Total records: {total_records:,}")
    print(f"Valid records: {valid_records:,}")
    print(f"Records dropped: {records_dropped:,} ({percent_dropped:.2f}%)")

    # Filter the DataFrame
    validated_rides = rides[final_filter]
    validated_rides = validated_rides[["tpep_pickup_datetime", "PULocationID"]]
    validated_rides.rename(
        columns={
            "tpep_pickup_datetime": "pickup_datetime",
            "PULocationID": "pickup_location_id",
        },
        inplace=True,
    )

    # Verify the valid rides
    if validated_rides.empty:
        raise ValueError(f"No valid rides found for {year}-{month:02} after filtering.")

    return validated_rides