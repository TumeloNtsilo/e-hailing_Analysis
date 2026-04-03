import requests
import pandas as pd

"""The Bronze Stage"""

def dataIngestion():
    """Ingesting the Uber Dataset"""

    try:
        df = pd.read_csv("uber_trips_dataset_50k.csv")
        return df
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    
def transform_csv(df):
    
    df = df.drop_duplicates().dropna(subset=['pickup_lat', 'pickup_lng', 'distance_km','fare_amount'])
    df['pickup_lat'] = df['pickup_lat'].round(2)
    df['pickup_lng'] = df['pickup_lng'].round(2)
    df['pickup_time'] = pd.to_datetime(df['pickup_time'])
    df['date'] = df['pickup_time'].dt.date

    new_df = df.drop(['driver_id', 'rider_id', 'drop_lat', 'drop_lng', 'pickup_time', 'drop_time'], axis=1)
    new_df = new_df.head(100)
    print(new_df.head())
   
    return new_df

def weather_request(new_df):

    weather_points = new_df[['pickup_lat', 'pickup_lng', 'date']].drop_duplicates()

    print(weather_points.head())
    return weather_points

def get_weather_data(points):

    weather_data = []

    for index, row in points.iterrows():
        lat = row['pickup_lat']
        lng = row['pickup_lng']
        date = row['date']
        
        url = (
            f"https://historical-forecast-api.open-meteo.com/v1/forecast"
            f"?latitude={lat}"
            f"&longitude={lng}"
            f"&start_date={date}"
            f"&end_date={date}"
            f"&daily=weather_code,temperature_2m_max,rain_sum,snowfall_sum,"
            f"precipitation_sum&timezone=auto"
        )

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            weather_data.append({
                'pickup_lat' : lat,
                'pickup_lng': lng,
                'date' : date,
                'temperature': data['daily']['temperature_2m_max'][0],
                'precipitation': data['daily']['precipitation_sum'][0]
            })

        except requests.RequestException as e:
            print(f"Error for {lat}, {lng, {date}}: {e}")
            
        
    weather_data = pd.DataFrame(weather_data)
    print(weather_data.head())
    return weather_data

def merge_data(df, weather_data):
    df_merged = pd.merge(df, weather_data, on=["pickup_lat", "pickup_lng", "date"], how="left")
    print(df_merged.head())
    return df_merged


if __name__ ==  "__main__":
  df = dataIngestion()
  new_df = transform_csv(df)
  points = weather_request(new_df)
  weather_data = get_weather_data(points)
  df_merged = merge_data(new_df, weather_data)


