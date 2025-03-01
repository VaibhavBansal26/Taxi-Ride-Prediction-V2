from pathlib import Path
import pandas as pd

def fetch_zone_data() -> str:
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    df = pd.read_csv(url)
    path = Path("..") / "data" / "raw" / "rides_zones.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine='pyarrow', index=False)
    
    print(f"Successfully saved as Parquet: {str(path)}")
    return str(path)