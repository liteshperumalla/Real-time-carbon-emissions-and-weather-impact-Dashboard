import requests
import pandas as pd
from datetime import datetime
import os

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
    10: "East of England",
    11: "South West England",
    12: "South England",
    13: "London",
    14: "South East England",
    15: "Yorkshire",
    16: "Central Scotland",
    17: "South West Scotland",
}

def fetch_weather_data(city):
    url = f"{OWM_BASE_URL}weather?q={city}&appid={OWM_API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None

def fetch_carbon_data(region_id):
    url = f"{CI_BASE_URL}/regional/regionid/{region_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None

def combine_and_save_data(city, region_id, filename="weather_data.csv"):
    weather_data = fetch_weather_data(city)
    carbon_data = fetch_carbon_data(region_id)

    if not weather_data or not carbon_data:
        print(f"Missing data for {city} or region ID {region_id}")
        return

    weather_info = {
        "city": city,
        "temperature": weather_data["main"]["temp"],
        "humidity": weather_data["main"]["humidity"],
        "pressure": weather_data["main"]["pressure"],
        "weather": weather_data["weather"][0]["description"],
        "datetime": datetime.now().isoformat()
    }

    carbon_region = carbon_data["data"][0]["region"]
    carbon_info = {
        "region_name": carbon_region["shortname"],
        "carbon_intensity": carbon_region["intensity"]["forecast"],
        "intensity_index": carbon_region["intensity"]["index"],
        "timestamp": carbon_data["data"][0].get("from", "")
    }

    combined_data = {**weather_info, **carbon_info}

    try:
        df_existing = pd.read_csv(filename)
    except FileNotFoundError:
        df_existing = pd.DataFrame()

    df_new = pd.DataFrame([combined_data])
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    df_combined.to_csv(filename, index=False)
    print(f"Data for {city} (Region: {carbon_info['region_name']}) saved to {filename}")

if __name__ == "__main__":
    for region_id, region_name in REGIONS.items():
        city = region_name
        combine_and_save_data(city, region_id)