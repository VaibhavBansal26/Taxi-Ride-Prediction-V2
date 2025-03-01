import pandas as pd
from datetime import datetime, timedelta
from typing import Union
from .load_and_process_data import load_and_process_data

def fetch_batch_raw_data(
    from_date: Union[datetime, str], to_date: Union[datetime, str]
) -> pd.DataFrame:
    
    if isinstance(from_date, str):
        from_date = datetime.fromisoformat(from_date)
    if isinstance(to_date, str):
        to_date = datetime.fromisoformat(to_date)

    if not isinstance(from_date, datetime) or not isinstance(to_date, datetime):
        raise ValueError(
            "Both 'from_date' and 'to_date' must be datetime objects or valid ISO format strings."
        )
    if from_date >= to_date:
        raise ValueError("'from_date' must be earlier than 'to_date'.")

    # Shift dates back by 52 weeks (1 year)
    historical_from_date = from_date - timedelta(weeks=52)
    historical_to_date = to_date 
    # - timedelta(weeks=52)

    # Load historical data
    rides_from = load_and_process_data(
        year=historical_from_date.year, months=[historical_from_date.month]
    )
    rides_from = rides_from[
        rides_from.pickup_datetime >= historical_from_date.to_datetime64()
    ]

    if historical_to_date.month != historical_from_date.month:
        rides_to = load_and_process_data(
            year=historical_to_date.year, months=[historical_to_date.month]
        )
        rides_to = rides_to[
            rides_to.pickup_datetime < historical_to_date.to_datetime64()
        ]
        rides = pd.concat([rides_from, rides_to], ignore_index=True)
    else:
        rides = rides_from
   
    rides["pickup_datetime"] += timedelta(weeks=52)

    rides.sort_values(by=["pickup_location_id", "pickup_datetime"], inplace=True)

    return rides