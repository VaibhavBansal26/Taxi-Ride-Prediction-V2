import pandas as pd
from datetime import datetime, timedelta, timezone
import config.config as config
from src.utils.inference_utils.inference_utils import get_feature_store

def fetch_days_data(days):
    current_date = pd.to_datetime(datetime.now(timezone.utc))
    fetch_data_from = current_date - timedelta(days=(365 + days))
    fetch_data_to = current_date 
    # - timedelta(days=365)
    print(fetch_data_from, fetch_data_to)
    fs = get_feature_store()
    fg = fs.get_feature_group(name=config.FEATURE_GROUP_NAME, version=1)

    query = fg.select_all()
    # query = query.filter((fg.pickup_hour >= fetch_data_from))
    df = query.read()
    cond = (df["pickup_hour"] >= fetch_data_from) & (df["pickup_hour"] <= fetch_data_to)
    return df[cond]