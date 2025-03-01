from datetime import datetime, timedelta, timezone

import hopsworks
import numpy as np
import pandas as pd
from hsfs.feature_store import FeatureStore

import config.config as config
from src.data_fetching_and_processing.transform_ts_data_to_features_and_target import transform_ts_data_info_features


def get_hopsworks_project() -> hopsworks.project.Project:
    return hopsworks.login(
        project=config.HOPSWORKS_PROJECT_NAME, api_key_value=config.HOPSWORKS_API_KEY
    )


def get_feature_store() -> FeatureStore:
    project = get_hopsworks_project()
    return project.get_feature_store()


def get_model_predictions(model, features: pd.DataFrame) -> pd.DataFrame:
    # past_rides_columns = [c for c in features.columns if c.startswith('rides_')]
    predictions = model.predict(features)

    results = pd.DataFrame()
    results["pickup_location_id"] = features["pickup_location_id"].values
    results["predicted_demand"] = predictions.round(0)

    return results


def load_batch_of_features_from_store(
    current_date: datetime,
) -> pd.DataFrame:
    feature_store = get_feature_store()

    # read time-series data from the feature store
    fetch_data_to = current_date - timedelta(hours=1)
    fetch_data_from = current_date - timedelta(days=29)
    print(f"Fetching data from {fetch_data_from} to {fetch_data_to}")
    feature_view = feature_store.get_feature_view(
        name=config.FEATURE_VIEW_NAME, version=config.FEATURE_VIEW_VERSION
    )

    ts_data = feature_view.get_batch_data(
        start_time=(fetch_data_from - timedelta(days=1)),
        end_time=(fetch_data_to + timedelta(days=1)),
    )
    ts_data = ts_data[ts_data.pickup_hour.between(fetch_data_from, fetch_data_to)]

    # Sort data by location and time
    ts_data.sort_values(by=["pickup_location_id", "pickup_hour"], inplace=True)

    features = transform_ts_data_info_features(
        ts_data, window_size=24 * 28, step_size=23
    )

    return features


def load_model_from_registry(version=None):
    from pathlib import Path

    import joblib

    from src.utils.pipeline_utils.pipeline_utils import (  # Import custom classes/functions
        TemporalFeatureEngineer,
        average_rides_last_4_weeks,
    )

    project = get_hopsworks_project()
    model_registry = project.get_model_registry()

    models = model_registry.get_models(name=config.MODEL_NAME)
    model = max(models, key=lambda model: model.version)
    model_dir = model.download()
    model = joblib.load(Path(model_dir) / "lgb_model.pkl")

    return model


def load_metrics_from_registry(version=None):

    project = get_hopsworks_project()
    model_registry = project.get_model_registry()

    models = model_registry.get_models(name=config.MODEL_NAME)
    model = max(models, key=lambda model: model.version)

    return model.training_metrics
