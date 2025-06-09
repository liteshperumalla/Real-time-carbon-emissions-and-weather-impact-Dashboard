import requests
import pandas as pd
from datetime import datetime
import time

# OpenWeatherMap API configuration
OWM_API_KEY = 'b508d3e46cd69f6a7469187dbcddd3d7'
OWM_BASE_URL = 'https://api.openweathermap.org/data/2.5/'

# Carbon Intensity API configuration
CI_BASE_URL = 'https://api.carbonintensity.org.uk'

# Region data
REGIONS = {
    1: "Inverness",
    2: "Edinburgh",
    3: "Manchester",
    4: "Newcastle upon Tyne",
    5: "Sheffield",
    6: "Liverpool",
    7: "Cardiff",
    8: "Birmingham",
    9: "Nottingham",
    10: "Cambridge",
    11: "Bristol",
    12: "Southampton",
    13: "London",
    14: "Brighton",
    15: "Leeds",
    16: "Glasgow",
    17: "Swansea",
}

CARBON_REGIONS = {
    1: "North Scotland",
    2: "South Scotland",
    3: "North West England",
    4: "North East England",
    5: "South Yorkshire",
    6: "North Wales, Merseyside and Cheshire",
    7: "South Wales",
    8: "West Midlands",
    9: "East Midlands",
    10: "East England",
    11: "South West England",
    12: "South England",
    13: "London",
    14: "South East England",
    15: "England",
    16: "Scotland",
    17: "Wales",
}

def fetch_current_weather(city):
    endpoint = f'weather?q={city}&appid={OWM_API_KEY}&units=metric'
    url = OWM_BASE_URL + endpoint
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def fetch_city_coordinates(city, country_code="GB"):
    endpoint = f'weather?q={city},{country_code}&appid={OWM_API_KEY}'
    url = OWM_BASE_URL + endpoint
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['coord']['lat'], data['coord']['lon']
    else:
        return None, None

def fetch_regional_carbon_intensity(region_id):
    url = f"{CI_BASE_URL}/regional/regionid/{region_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def fetch_national_carbon_intensity():
    url = f"{CI_BASE_URL}/intensity"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def combine_and_save_data(city, region_id, filename='weather_data.csv'):
    weather_data = fetch_current_weather(city)
    carbon_data = fetch_regional_carbon_intensity(region_id)
    latitude, longitude = fetch_city_coordinates(city)

    if weather_data:
        weather_info = {
            'city': city,
            'temperature': weather_data['main']['temp'],
            'weather_description': weather_data['weather'][0]['description'],
            'humidity': weather_data['main']['humidity'],
            'wind_speed': weather_data['wind']['speed'],
            'weather_datetime': datetime.utcfromtimestamp(weather_data['dt']).strftime('%Y-%m-%d %H:%M:%S'),
            'latitude': latitude,
            'longitude': longitude
        }
    else:
        print(f"Failed to fetch weather data for {city}")
        return

    if carbon_data and 'data' in carbon_data and len(carbon_data['data']) > 0:
        region_data = carbon_data['data'][0]
        intensity_data = region_data.get('intensity', {})
        if 'forecast' in intensity_data:
            carbon_info = {
                'region_id': region_id,
                'region_name': region_data['dnoregion'],
                'carbon_intensity': intensity_data['forecast'],
                'carbon_forecast': intensity_data['forecast'],
                'carbon_average': intensity_data.get('index', 'N/A'),
                'carbon_datetime': region_data.get('from', '')
            }
        else:
            national_data = fetch_national_carbon_intensity()
            intensity = national_data['data'][0]['intensity'] if national_data else {}
            carbon_info = {
                'region_id': region_id,
                'region_name': 'National',
                'carbon_intensity': intensity.get('actual', 'N/A'),
                'carbon_forecast': intensity.get('forecast', 'N/A'),
                'carbon_average': intensity.get('index', 'N/A'),
                'carbon_datetime': national_data['data'][0].get('from', '') if national_data else ''
            }
    else:
        print(f"No carbon data for region {region_id}")
        return

    combined_data = {**weather_info, **carbon_info}

    try:
        df_existing = pd.read_csv(filename)
    except FileNotFoundError:
        df_existing = pd.DataFrame()

    df_new = pd.DataFrame([combined_data])
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    df_combined.to_csv(filename, index=False)
    print(f"Data for {city} saved to {filename}")

if __name__ == "__main__":
    for region_id, region_name in REGIONS.items():
        city = region_name
        combine_and_save_data(city, region_id)