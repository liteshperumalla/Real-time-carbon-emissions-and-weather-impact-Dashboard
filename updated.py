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
    """Fetch current weather data for a city"""
    endpoint = f'weather?q={city}&appid={OWM_API_KEY}&units=metric'
    url = OWM_BASE_URL + endpoint
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Weather API error for {city}: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Weather API request failed for {city}: {e}")
        return None

def fetch_city_coordinates(city, country_code="GB"):
    """Fetch coordinates for a city"""
    endpoint = f'weather?q={city},{country_code}&appid={OWM_API_KEY}'
    url = OWM_BASE_URL + endpoint
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data['coord']['lat'], data['coord']['lon']
        else:
            print(f"Coordinates API error for {city}: {response.status_code}")
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"Coordinates API request failed for {city}: {e}")
        return None, None

def fetch_regional_carbon_intensity(region_id):
    """Fetch regional carbon intensity data"""
    url = f"{CI_BASE_URL}/regional/regionid/{region_id}"
    try:
        response = requests.get(url, timeout=10)
        print(f"Carbon API response for region {region_id}: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Carbon data structure: {data}")
            return data
        else:
            print(f"Carbon API error for region {region_id}: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Carbon API request failed for region {region_id}: {e}")
        return None

def fetch_national_carbon_intensity():
    """Fetch national carbon intensity data as fallback"""
    url = f"{CI_BASE_URL}/intensity"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"National carbon data: {data}")
            return data
        else:
            print(f"National carbon API error: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"National carbon API request failed: {e}")
        return None

def extract_carbon_intensity(carbon_data):
    """Extract carbon intensity values from API response"""
    if not carbon_data or 'data' not in carbon_data:
        return None, None, None, None
    
    data_list = carbon_data['data']
    if not data_list or len(data_list) == 0:
        return None, None, None, None
    
    # Get the first (current) data point
    current_data = data_list[0]
    
    # Extract region info
    region_name = current_data.get('dnoregion', 'Unknown')
    carbon_datetime = current_data.get('from', '')
    
    # Extract intensity data
    intensity_data = current_data.get('intensity', {})
    
    # Try to get actual, forecast, or index values
    carbon_actual = intensity_data.get('actual')
    carbon_forecast = intensity_data.get('forecast') 
    carbon_index = intensity_data.get('index', 'N/A')
    
    # Use forecast if actual is not available
    carbon_intensity = carbon_actual if carbon_actual is not None else carbon_forecast
    
    print(f"Extracted carbon data - Actual: {carbon_actual}, Forecast: {carbon_forecast}, Index: {carbon_index}")
    
    return carbon_intensity, carbon_forecast, carbon_index, region_name, carbon_datetime

def combine_and_save_data(city, region_id, filename='weather_data.csv'):
    """Combine weather and carbon data and save to CSV"""
    print(f"\n--- Processing {city} (Region {region_id}) ---")
    
    # Fetch weather data
    weather_data = fetch_current_weather(city)
    if not weather_data:
        print(f"Failed to fetch weather data for {city}")
        return
    
    # Fetch coordinates
    latitude, longitude = fetch_city_coordinates(city)
    
    # Prepare weather info
    weather_info = {
        'city': city,
        'temperature': weather_data['main']['temp'],
        'weather_description': weather_data['weather'][0]['description'],
        'humidity': weather_data['main']['humidity'],
        'wind_speed': weather_data['wind'].get('speed', 0),
        'weather_datetime': datetime.utcfromtimestamp(weather_data['dt']).strftime('%Y-%m-%d %H:%M:%S'),
        'latitude': latitude,
        'longitude': longitude
    }
    
    # Fetch carbon data
    carbon_data = fetch_regional_carbon_intensity(region_id)
    carbon_intensity, carbon_forecast, carbon_index, region_name, carbon_datetime = extract_carbon_intensity(carbon_data)
    
    # If regional data failed or returned null, try national data
    if carbon_intensity is None:
        print(f"Regional carbon data unavailable for region {region_id}, trying national data...")
        national_data = fetch_national_carbon_intensity()
        if national_data and 'data' in national_data and len(national_data['data']) > 0:
            intensity_data = national_data['data'][0].get('intensity', {})
            carbon_intensity = intensity_data.get('actual') or intensity_data.get('forecast')
            carbon_forecast = intensity_data.get('forecast', 'N/A')
            carbon_index = intensity_data.get('index', 'N/A')
            region_name = 'National'
            carbon_datetime = national_data['data'][0].get('from', '')
            print(f"Using national carbon data: {carbon_intensity}")
    
    # Prepare carbon info
    carbon_info = {
        'region_id': region_id,
        'region_name': region_name or CARBON_REGIONS.get(region_id, 'Unknown'),
        'carbon_intensity': carbon_intensity,
        'carbon_forecast': carbon_forecast,
        'carbon_index': carbon_index,
        'carbon_datetime': carbon_datetime
    }
    
    # Combine all data
    combined_data = {**weather_info, **carbon_info}
    
    # Add timestamp for when this data was collected
    combined_data['data_collected_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"Final combined data for {city}: {combined_data}")
    
    # Load existing data or create new DataFrame
    try:
        df_existing = pd.read_csv(filename)
    except FileNotFoundError:
        df_existing = pd.DataFrame()
    
    # Add new data
    df_new = pd.DataFrame([combined_data])
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    
    # Save to CSV
    df_combined.to_csv(filename, index=False)
    print(f"Data for {city} saved to {filename}")
    
    # Small delay to avoid rate limiting
    time.sleep(1)

if __name__ == "__main__":
    print("Starting weather and carbon data collection...")
    
    for region_id, region_name in REGIONS.items():
        city = region_name
        try:
            combine_and_save_data(city, region_id)
        except Exception as e:
            print(f"Error processing {city}: {e}")
            continue
    
    print("\nData collection completed!")