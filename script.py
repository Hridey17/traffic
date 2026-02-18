import requests
import pandas as pd
from datetime import datetime
import time
import os

API_KEY = os.getenv("TOMTOM_API_KEY")
FLOW_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

segments = [
    {"segment_id": "Segment 1", "name": "World Trade Centre", "lat": 25.2262, "lon": 55.2870},
    {"segment_id": "Segment 2", "name": "DIFC", "lat": 25.2136, "lon": 55.2796},
    {"segment_id": "Segment 3", "name": "Business Bay", "lat": 25.1872, "lon": 55.2644},
    {"segment_id": "Segment 4", "name": "MOE", "lat": 25.1182, "lon": 55.2006}
]

FILE_NAME = "szr_traffic_data.csv"

def get_flow_data(segment):
    try:
        params = {"point": f"{segment['lat']},{segment['lon']}", "key": API_KEY}
        data = requests.get(FLOW_URL, params=params, timeout=30).json()
        fsd = data.get("flowSegmentData", {})
        return {
            "timestamp": datetime.utcnow(),
            "segment_id": segment["segment_id"],
            "segment_name": segment["name"],
            "latitude": segment["lat"],
            "longitude": segment["lon"],
            "current_speed_kmh": fsd.get("currentSpeed"),
            "free_flow_speed_kmh": fsd.get("freeFlowSpeed"),
            "current_travel_time_sec": fsd.get("currentTravelTime"),
            "free_flow_travel_time_sec": fsd.get("freeFlowTravelTime"),
            "confidence": fsd.get("confidence")
        }
    except Exception as e:
        print(f"Error fetching {segment['name']}: {e}")
        return None

print("🚦 SZR Traffic Data Collection Triggered")

records = []

for seg in segments:
    print(f"Fetching {seg['name']} at {datetime.utcnow()}")
    data = get_flow_data(seg)
    if data:
        records.append(data)
    time.sleep(1)

if records:
    df = pd.DataFrame(records)
    if os.path.isfile(FILE_NAME):
        df.to_csv(FILE_NAME, mode="a", header=False, index=False)
    else:
        df.to_csv(FILE_NAME, index=False)
    print(f"✅ Data appended to {FILE_NAME}")
else:
    print("⚠️ No data collected")
