import pandas as pd
from datetime import timedelta
import config.config as config
from src.utils.inference_utils.inference_utils import get_feature_store

def fetch_predictions(hours):
    current_hour = (pd.Timestamp.now(tz="Etc/UTC") - timedelta(hours=hours)).floor("h")

    fs = get_feature_store()
    fg = fs.get_feature_group(name=config.FEATURE_GROUP_MODEL_PREDICTION, version=1)

    df = fg.filter((fg.pickup_hour >= current_hour)).read()

    return df