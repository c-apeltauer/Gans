#!/usr/bin/env python
import requests
import pandas as pd
import re
import sys
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import pytz

# get configuration data
config_json = open('config.json')
config_data = json.loads(config_json.read())

# read configuration data into individual variables
api_key_openweather = config_data['api_key_openweather']
api_key_rapidapi = config_data['api_key_rapidapi']
schema = "gans"
host = config_data['mysql_host']
user = "root"
password = config_data['mysql_passwd']
port = 3306
connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'

# get all currently available weather data at (lon, lat)
def get_weather_data(lon, lat):
    url = "https://api.openweathermap.org/data/2.5/forecast"
    header={"X-Api-Key": api_key_openweather}
    querystring = {"lat" : str(lat), "lon" : str(lon), 'units': 'metric'}
    weather_request = requests.request("GET",
                                        url,
                                        headers=header,
                                        params=querystring)
    weather_json = weather_request.json()
    if weather_json['cod'] != '200':
        return []
    return weather_json['list']

# extract weather data for a single time period (of 3 h)
# returns a list of relevant data
def extract_data_single(forecast):
    # 'rain' only available if it is actually raining
    if 'rain' in forecast.keys():
        rain = forecast['rain']['3h']
    else:
        rain = 0.0
    timestamp = forecast['dt_txt']
    temp = forecast['main']['temp']
    feeled_temp = forecast['main']['feels_like']
    humidity = forecast['main']['humidity']
    general = forecast['weather'][0]['description']
    clouds = forecast['clouds']['all']
    wind = forecast['wind']['speed']
    visibility = forecast['visibility']
    return [timestamp, temp, feeled_temp, humidity, general, clouds, wind, rain, visibility]

"""
    openweathermap.org gives us a forecast for the next 5 days in steps of 3 hours (starting at next multiple of 3 hours)
    Depending on our use case it doesn't make sense to accumulate all these forecasts
    so we limit it to the first upto forecasts (i. e. to the weather at the next hour divisible by 3 and the next 3*(upto-1) hours)
    Default is to use all forecasts provided
"""
def extract_data(forecasts, upto=-1):
    weather_df = pd.DataFrame(
        [],
        columns=['timestamp', 'temperature', 'feeled_temperature', 'humidity', 'overall', 'clouds', 'windspeed', 'rain', 'visibility']
    )
    if -1 != upto:
        # -1: extract all data
        forecasts = forecasts[:upto]
    for forecast in forecasts:
        weather_df.loc[len(weather_df)] = extract_data_single(forecast)
    return weather_df

# update the population for the city in row 'row' of the cities data table
def update_population_for_city(row):
    soup = get_wikipedia(row['city'])
    population_df = pd.DataFrame({'city_id': [], 'population': [], 'year_retrieved': []})
    new_row = {'city_id': row['city_id'], 'population': get_population(soup), 'year_retrieved': datetime.now().year}
    population_df.loc[len(population_df)] = new_row
    population_df.to_sql('population',
                            if_exists='append',
                            con=connection_string,
                            index=False)

# update the population for all cities in the cities data table
def update_population():
    cities_from_sql = pd.read_sql("cities", con=connection_string)
    cities_from_sql.apply(update_population_for_city, axis=1)

# download Wikipedia data on 'city'
def get_wikipedia(city):
    url = "https://en.wikipedia.org/wiki/" + city
    response = requests.get(url)
    return BeautifulSoup(response.content, 'html.parser')

# extract population data from Wikipedia
def get_population(soup_wiki):
    population = -1
    for infobox_label in soup_wiki.find_all(class_='infobox-label'):
        if (infobox_label.get_text().startswith('Population')):
            population = int(infobox_label.find_next(class_='infobox-data').get_text().replace(',', ''))
    # if nothing was found, try another approach
    if (-1 == population):
        for infobox_header in soup_wiki.find_all(class_='infobox-header'):
            if (infobox_header.get_text().startswith('Population')):
                population = int(infobox_header.find_next(class_='infobox-data').get_text().replace(',', ''))
    return population

# add a new city to the cities data table
def add_city(city):
    cities_from_sql = pd.read_sql("cities", con=connection_string)
    if city in cities_from_sql.loc[:, 'city'].values:
        return

    df = pd.DataFrame({'city': [], 'longitude': [], 'latitude': [], 'country': [], 'population': [], 'tz': []})
    soup_wiki = get_wikipedia(city)
    # Turn a°b'c'' into a decimal number
    match = re.search(r'(\d+)°(\d+)′((\d+)″)?(\w)', soup_wiki.find(class_='latitude').get_text())
    lat = float(match.group(1)) + float(match.group(2))/60
    # seconds not always present
    if match.group(4):
        lat += float(match.group(4))/3600
    if 'N' != match.group(5):
        lat = -lat;
    match = re.search(r'(\d+)°(\d+)′((\d+)″)?(\w)', soup_wiki.find(class_='longitude').get_text())
    long = float(match.group(1)) + float(match.group(2))/60
    if match.group(4):
        long += float(match.group(4))/3600
    if 'E' != match.group(5):
        long = -long;

    for infobox_label in soup_wiki.find_all(class_='infobox-label'):
        # Mostly the state of the city is 'Country', but for UK it's Sovereign state, while country is 'England'
        if (infobox_label.get_text() == 'Sovereign state'):
            country = infobox_label.find_next(class_='infobox-data').get_text().strip()
            break
        elif (infobox_label.get_text() == 'Country'):
            country = infobox_label.find_next(class_='infobox-data').get_text().strip()

    population = get_population(soup_wiki)

    # get airports and time zones from API
    airport_list, tz = get_airports(lon=long, lat=lat)

    # add a new row to our local pandas frame
    new_row = {'city': city,
                'longitude': long,
                'latitude': lat,
                'country': country,
                'population': population,
                'tz': tz
                }
    df.loc[len(df)] = new_row

    # extract just the 'city' column and send it to the 'cities' data table in SQL
    df.loc[:, 'city'].to_sql('cities',
                                if_exists='append',
                                con=connection_string,
                                index=False)

    # read back 'cities' data table to get the city_id of the newly added city
    cities_from_sql = pd.read_sql("cities", con=connection_string)

    # merge city_id into our local data frame
    merged_df = df.merge(cities_from_sql, on = 'city', how='left')

    # population data table just needs city_id, population and the year of retrival
    population_df = merged_df.drop(columns=['city', 'longitude', 'latitude', 'country', 'tz'])
    # fill column with the current year
    population_df['year_retrieved']=[2025 for i in range(0, merged_df.shape[0])]
    population_df.to_sql('population',
                            if_exists='append',
                            con=connection_string,
                            index=False)

    # geo doesn't need city or population
    merged_df.drop(columns=['city', 'population']).to_sql('geo',
                                                            if_exists='append',
                                                            con=connection_string,
                                                            index=False)

    # if airports were found, add them to the table
    if len(airport_list) != 0:
        city_id = cities_from_sql.loc[cities_from_sql.loc[:, 'city'] == city, 'city_id'].iloc[0]
        # same city_id for all airports
        airport_df = pd.DataFrame({'city_id': [city_id for i in range(0, len(airport_list))],
                                    'icao': airport_list})
        airport_df.to_sql('airports', if_exists='append', con=connection_string, index=False)
        
# update cities
def update_tables(cities):
    cities_from_sql = pd.read_sql("cities", con=connection_string)
    geo = pd.read_sql('geo', con=connection_string)
    for city in cities:
        print(f"Processing {city}");
        # if new city, add it
        if not city in cities_from_sql.loc[:, 'city'].values:
            try:
                add_city(city)
                cities_from_sql = pd.read_sql("cities", con=connection_string)
                geo = pd.read_sql('geo', con=connection_string)
            except Exception:
                """
                    Do nothing
                """
        # ignore cities, you cannot webscrape
        if not city in cities_from_sql.loc[:, 'city'].values:
            print(f"{city} not in DB, update DB not possible")
            continue
        city_id = cities_from_sql.loc[cities_from_sql.loc[:, 'city'] == city, 'city_id'].iloc[0]
        lat = geo.loc[geo.loc[:, 'city_id']==city_id, 'latitude'].iloc[0]
        lon = geo.loc[geo.loc[:, 'city_id']==city_id, 'longitude'].iloc[0]
        wjson = get_weather_data(lat=lat, lon=lon)
        wdf = extract_data(wjson, 2)
        wdf['city_id']=[city_id for i in range(0, wdf.shape[0])]
        wdf.to_sql('weather',
                    if_exists='append',
                    con=connection_string,
                    index=False)
        airports_from_sql = pd.read_sql('airports', con=connection_string)
        airports_from_sql = airports_from_sql.loc[airports_from_sql.loc[:, 'city_id']==city_id]
        for ts in wdf.loc[:, 'timestamp']:
            format = '%Y-%m-%d %H:%M:%S'
            query_time = datetime.strptime(ts, format).replace(tzinfo=pytz.utc)
            for airport in airports_from_sql.loc[:, 'icao']:
                tz = geo.loc[geo.loc[:, 'city_id']==city_id, 'tz'].iloc[0]
                get_flights(airport, query_time, tz)

def get_flights(airport, utc_time, timezone):
    start_local = utc_time.astimezone(pytz.timezone(timezone)).strftime("%Y-%m-%dT%H:%M")
    end_utc = utc_time + timedelta(hours=3)
    end_local = end_utc.astimezone(pytz.timezone(timezone)).strftime("%Y-%m-%dT%H:%M")
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{airport}/{start_local}/{end_local}"
    querystring = {"direction":"Arrival","withCancelled":"false","withCargo":"false","withPrivate":"false","withLocation":"false"}

    headers = {"x-rapidapi-host": "aerodatabox.p.rapidapi.com",
        'x-rapidapi-key': '687292277emsh6620811a3972b04p1a4ee9jsn8c02f9bc139b'
    }
    response = requests.get(url, headers=headers, params=querystring)

    if 200 == response.status_code:
        json=response.json()

        format = '%Y-%m-%d %H:%MZ'
        froms = [ arrival['movement']['airport']['name'] for arrival in json['arrivals']]
        arrivals = [ datetime.strptime(arrival['movement']['scheduledTime']['utc'], format) for arrival in json['arrivals']]
        flights_df = pd.DataFrame({'icao': [airport for i in range(0, len(arrivals))],
                                    'arrival': arrivals,
                                    'fromWhere': froms})
        flights_df.to_sql('flights',
                    if_exists='append',
                    con=connection_string,
                    index=False)


def get_airports(lon, lat):
    url = "https://aerodatabox.p.rapidapi.com/airports/search/location"
    querystring = {"lat":str(lat),"lon":str(lon),"radiusKm":"50","limit":"10","withFlightInfoOnly":"true"}
    headers = {"x-rapidapi-host": "aerodatabox.p.rapidapi.com",
                'x-rapidapi-key': api_key_rapidapi
    }

    response = requests.get(url, headers=headers, params=querystring)
    airports = response.json()['items']
    return [ airport['icao'] for airport in airports ], airports[0]['timeZone']

if '_population' == sys.argv[1]:
    update_population()
else:
    update_tables(sys.argv[1:])
