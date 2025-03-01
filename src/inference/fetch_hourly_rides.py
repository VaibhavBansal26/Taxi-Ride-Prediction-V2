import pandas as pd
from datetime import timedelta
import config.config as config
from src.utils.inference_utils.inference_utils import get_feature_store

def fetch_hourly_rides(hours):
    current_hour = (pd.Timestamp.now(tz="Etc/UTC") - timedelta(hours=hours)).floor("h")

    fs = get_feature_store()
    fg = fs.get_feature_group(name=config.FEATURE_GROUP_NAME, version=1)

    query = fg.select_all()
    query = query.filter(fg.pickup_hour >= current_hour)

    return query.read()