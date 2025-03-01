import pyarrow.parquet as pq
from .fetch_zone_data import fetch_zone_data

def process_zone_data(): 
    zone_path = fetch_zone_data()
    zf = pq.read_table(zone_path)
    zones = zf.to_pandas()
    zones["Zone"] = zones['Zone'].str.strip() + ', ' + zones['Borough'].str.strip()
    zones.rename(columns={"LocationID":"pickup_location_id","Zone":"zone"},inplace=True)
    zones.drop(columns=['service_zone','Borough'],inplace=True)
    return zones