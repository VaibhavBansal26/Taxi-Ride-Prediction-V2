from datetime import timedelta
import pandas as pd
import plotly.express as px

def plot_prediction(features: pd.DataFrame, prediction: int):
    # Identify time series columns (e.g., historical ride counts)
    time_series_columns = [
        col for col in features.columns if col.startswith("rides_t-")
    ]
    time_series_values = [
        features[col].iloc[0] for col in time_series_columns
    ] + prediction["predicted_demand"].to_list()

    # Convert pickup_hour Series to single timestamp
    pickup_hour = pd.Timestamp(features["pickup_hour"].iloc[0])

    # Generate corresponding timestamps for the time series
    time_series_dates = pd.date_range(
        start=pickup_hour - timedelta(hours=len(time_series_columns)),
        end=pickup_hour,
        freq="h",
    )

    # Create a DataFrame for the historical data
    historical_df = pd.DataFrame(
        {"datetime": time_series_dates, "rides": time_series_values}
    )

    # Create the plot title with relevant metadata
    title = f"Pickup Hour: {pickup_hour}, Location ID: {features['pickup_location_id'].iloc[0]}"

    # Create the base line plot
    fig = px.line(
        historical_df,
        x="datetime",
        y="rides",
        template="plotly_white",
        markers=True,
        title=title,
        labels={"datetime": "Time", "rides": "Ride Counts"},
    )

    # Add prediction point
    fig.add_scatter(
        x=[pickup_hour],  # Last timestamp
        y=prediction["predicted_demand"].to_list(),
        line_color="red",
        mode="markers",
        marker_symbol="x",
        marker_size=10,
        name="Prediction",
    )

    return fig
