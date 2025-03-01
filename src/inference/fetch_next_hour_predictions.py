from datetime import datetime, timedelta, timezone
import config.config as config
from src.utils.inference_utils.inference_utils import get_feature_store

def fetch_next_hour_predictions():
    # Get current UTC time and round up to next hour
    now = datetime.now(timezone.utc)
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

    fs = get_feature_store()
    fg = fs.get_feature_group(name=config.FEATURE_GROUP_MODEL_PREDICTION, version=1)
    df = fg.read()
    # Then filter for next hour in the DataFrame
    df = df[df["pickup_hour"] == next_hour]

    print(f"Current UTC time: {now}")
    print(f"Next hour: {next_hour}")
    print(f"Found {len(df)} records")
    return df