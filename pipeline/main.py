import requests
import pandas as pd

"""The Bronze Stage"""

def dataIngestion():
    """Ingesting the Uber Dataset"""

    try:
        df = pd.read_csv("uber_trips_dataset_50k.csv")
        print(df.head())
        return df
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    
def transform(file):
    url = (
        "https://historical-forecast-api.open-meteo.com/v1/forecast"
        "?latitude=-26.20227"
        "&longitude=28.04363"
        "&start_date=2026-02-25"
        "&end_date=2026-03-11"
        "&hourly=temperature_2m,apparent_temperature,precipitation,"
        "precipitation_probability,weathercode,cloud_cover,wind_speed_10m,"
        "relative_humidity_2m,is_day"
        "&timezone=Africa/Johannesburg"
    )

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()

       
        data = response.json()
        weather_results = data["hourly"]

        df = pd.DataFrame(weather_results)
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return
        

if __name__ ==  "__main__":
  print(extract())