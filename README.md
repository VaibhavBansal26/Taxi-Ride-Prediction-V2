# Taxi-Ride-Prediction-V2


Hopsworks - Hopsworks is the flexible and modular AI Lakehouse with a feature store that provides seamless integration for existing pipelines, superior performance

Cron Jobs

Pipeline
1. Feature Pipeline
2. Model Training Pipeline
3. Inference Pipeline

Models Used
1. XG Boost
2. LightGBM
3. Fourier Transform
4. ARIMA
5. ARMA
6. Seasonal ARMA
7. Neural Networks

Front End Application

Streamlit

Objective - 
1. Ride Next Hour Prediction
2. Model Monitoring App


Github Workflows
1. Feature Pipeline - Fetch New Data From Feature Pipeline and Load Data to Hopsworks (Cron Job runs every hour - feature_pipeline.yml)
2. Training Pipeline - (training_pipeline.yml) 
3. Inference Pipeline - (inference_pipeline.yml)


Workflow
1. Data Ingestion
   a. Fetch Raw Data From NYC taxi ride API
   b. Filter Data (Include Only Valid Data for specific date range)