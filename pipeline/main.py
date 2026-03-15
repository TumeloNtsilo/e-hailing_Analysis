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

    df['pickup_lat'] = df['pickup_lat'].round(2)
    df['pickup_lng'] = df['pickup_lng'].round(2)
    df['pickup_time'] = pd.to_datetime(df['pickup_time'])
    df['date'] = df['pickup_time'].dt.date

    new_df = df.drop(['driver_id', 'rider_id', 'drop_lat', 'drop_lng'], axis=1)

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
            f"&daily=temperature_2m,apparent_temperature,precipitation,"
            f"precipitation_probability,weathercode,cloud_cover,wind_speed_10m,"
            f"relative_humidity_2m,is_day"
            f"&timezone=auto"
        )

        
        response = requests.get(url, timeout=15)
    
        data = response.json()

        weather_data.append({
            'pickup_lat' : lat,
            'pickup_lng': lng,
            'date' : date,
            'temperature': data['daily']['temperature_2m_max'][0],
            'precipitation': data['daily']['precipitation_sum'][0]
        })
            
        

    weather_data = pd.DataFrame(weather_data)
    return weather_data



if __name__ ==  "__main__":
  df = dataIngestion()
  new_df = transform_csv(df)
#   points = weather_request(new_df)
#   get_weather_data(points)
