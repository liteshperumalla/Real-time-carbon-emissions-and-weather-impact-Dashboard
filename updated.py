import requests
import pandas as pd
from datetime import datetime
import time

# OpenWeatherMap API configuration
OWM_API_KEY = 'b508d3e46cd69f6a7469187dbcddd3d7'  # Replace with your API key
OWM_BASE_URL = 'https://api.openweathermap.org/data/2.5/'

# Carbon Intensity API configuration
CI_BASE_URL = 'https://api.carbonintensity.org.uk'

# Region data for OpenWeatherMap and Carbon Intensity API
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

# Functions for OpenWeatherMap API
def fetch_current_weather(city):
    """Fetch current weather data for a city."""
    endpoint = f'weather?q={city}&appid={OWM_API_KEY}&units=metric'
    url = OWM_BASE_URL + endpoint
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching weather data for {city}: {response.status_code}")
        return None

def fetch_weather_forecast(city, days=5):
    """Fetch weather forecast for the next 'days' for a city."""
    endpoint = f'forecast?q={city}&appid={OWM_API_KEY}&units=metric'
    url = OWM_BASE_URL + endpoint
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching weather forecast for {city}: {response.status_code}")
        return None

# Functions for Carbon Intensity API
def fetch_regional_carbon_intensity(region_id):
    """Fetch current carbon intensity for a specific region."""
    endpoint = f'/regional/regionid/{region_id}'
    url = CI_BASE_URL + endpoint
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching carbon intensity for region ID {region_id}: {response.status_code}")
        return None

def fetch_national_carbon_intensity():
    """Fetch national carbon intensity data."""
    endpoint = '/intensity'
    url = CI_BASE_URL + endpoint
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching national carbon intensity data:", response.status_code)
        return None

def fetch_carbon_intensity_forecast():
    """Fetch national carbon intensity forecast."""
    endpoint = '/intensity/forecast'
    url = CI_BASE_URL + endpoint
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching carbon intensity forecast:", response.status_code)
        return None
def fetch_city_coordinates(city, country_code="GB"):
    """Fetch latitude and longitude for a city."""
    endpoint = f'weather?q={city},{country_code}&appid={OWM_API_KEY}'
    url = OWM_BASE_URL + endpoint
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['coord']['lat'], data['coord']['lon']
    else:
        print(f"Error fetching coordinates for {city}: {response.status_code}")
        return None, None
def fetch_carbon_intensity(region_id):
    # Example API endpoint (update with your actual endpoint)
    api_url = f"https://api.carbonintensity.org.uk/intensity/{region_id}"
    response = requests.get(api_url)
    
    if response.status_code == 200:
        return response.json()
    else:
        return None
import pandas as pd
from datetime import datetime

def combine_and_save_data(city, region_id, filename='combined_data.csv'):
    """Combine weather and carbon intensity data, and save to CSV."""
    weather_data = fetch_current_weather(city)
    carbon_data = fetch_regional_carbon_intensity(region_id)
    latitude, longitude = fetch_city_coordinates(city)

    # Check if weather data is fetched successfully
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
        print(f"Failed to fetch weather data for {city}.")
        return  # Exit the function if weather data is not available

    # Check if carbon data is fetched successfully
    if carbon_data:
        if 'data' in carbon_data and len(carbon_data['data']) > 0:
            region_data = carbon_data['data'][0]
            intensity_data = region_data.get('intensity', None)
            
            if intensity_data and 'forecast' in intensity_data:
                forecast_intensity = intensity_data['forecast']
                carbon_info = {
                    'region_id': region_id,
                    'region_name': region_data['dnoregion'],
                    'carbon_intensity': forecast_intensity,
                    'carbon_forecast': forecast_intensity,
                    'carbon_average': intensity_data.get('index', 'N/A'),
                    'carbon_datetime': region_data['data'][0]['from']
                }

            else:
                print(f"Intensity forecast data missing for region {region_id}.")
                carbon_data = fetch_national_carbon_intensity()
                if carbon_data:
                    carbon_info = {
                        'region_id': region_id,
                        'region_name': 'National',
                        'carbon_intensity': carbon_data['data'][0].get('intensity', {}).get('actual', 'N/A'),
                        'carbon_forecast': carbon_data['data'][0].get('intensity', {}).get('forecast', 'N/A'),
                        'carbon_average': carbon_data['data'][0].get('intensity', {}).get('index', 'N/A'),
                        'carbon_datetime': carbon_data['data'][0].get('from', '')
                    }

        else:
            print(f"No data available for region {region_id}.")
            return  # Exit the function if region data is missing

    else:
        print(f"Failed to fetch carbon intensity data for region {region_id}.")
        return  # Exit the function if carbon intensity data is not available

    # Combine the data
    combined_data = {**weather_info, **carbon_info}

    # Load existing CSV or create a new one
    try:
        df_existing = pd.read_csv(filename)
    except FileNotFoundError:
        df_existing = pd.DataFrame()

    # Append new data
    df_new = pd.DataFrame([combined_data])
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)

    # Save to CSV
    df_combined.to_csv(filename, index=False)
    print(f"Data for {city} (Region: {carbon_info['region_name']}) saved to {filename}")
    
# Automation function for all regions
def automate_data_fetching_for_regions(interval_minutes=60):
    """Automate data fetching for all regions at regular intervals."""
    while True:
        for region_id, region_name in REGIONS.items():
            city = region_name  # Using region name as city name
            combine_and_save_data(city, region_id)
        print(f"Waiting for {interval_minutes} minutes before the next fetch...")
        time.sleep(interval_minutes * 60)

# Example usage
if __name__ == "__main__":
    # Start automation
    automate_data_fetching_for_regions(interval_minutes=60)

