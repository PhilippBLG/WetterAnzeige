from flask import Flask, request, render_template, Response, jsonify
from flask_scss import Scss
from flask_sqlalchemy import SQLAlchemy
import requests
import time
import pandas as pd
import numpy as np
from functools import lru_cache
app = Flask(__name__)

# Function to calculate distances using Haversine formula
def haversine(lat1, lon1, lat2, lon2):
    """Calculate the great-circle distance between two points on the Earth."""
    R = 6371.0  # Radius of the Earth in kilometers
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c


def replace_nan_with_none(obj):
    """
    Ersetzt in einem verschachtelten Objekt (dict, list) alle NaN-Werte durch None.
    """
    if isinstance(obj, dict):
        return {k: replace_nan_with_none(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_nan_with_none(item) for item in obj]
    elif isinstance(obj, (float, np.floating)) and np.isnan(obj):
        return None
    else:
        return obj

@lru_cache(maxsize=1)
def read_ghcnd_stations(url):
    """
    Liest und parst die GHCND Inventory-Datei (ghcnd-inventory.txt) von der angegebenen URL.

    Dateiformat (Fixed Width):
      - ID        : Spalten 1-11   (Character)
      - LATITUDE  : Spalten 13-20  (Real)
      - LONGITUDE : Spalten 22-30  (Real)
      - ELEMENT   : Spalten 32-35  (Character)
      - FIRSTYEAR : Spalten 37-40  (Integer)
      - LASTYEAR  : Spalten 42-45  (Integer)
    """
    app.logger.info("Lade Inventory-Datei von URL (Cache miss)")

    # Definiere die Spaltenbereiche (Python-indiziert: 0-basiert)
    colspecs = [
        (0, 11),  # ID: Spalte 1-11
        (12, 20),  # LATITUDE: Spalte 13-20
        (21, 30),  # LONGITUDE: Spalte 22-30
        (31, 35),  # ELEMENT: Spalte 32-35
        (36, 40),  # FIRSTYEAR: Spalte 37-40
        (41, 45)  # LASTYEAR: Spalte 42-45
    ]
    names = ['ID', 'LATITUDE', 'LONGITUDE', 'ELEMENT', 'FIRSTYEAR', 'LASTYEAR']

    # Lese die Fixed-Width-Datei von der URL
    df = pd.read_fwf(url, colspecs=colspecs, header=None, names=names)

    # Konvertiere numerische Spalten in die richtigen Typen
    df['LATITUDE'] = pd.to_numeric(df['LATITUDE'], errors='coerce')
    df['LONGITUDE'] = pd.to_numeric(df['LONGITUDE'], errors='coerce')
    df['FIRSTYEAR'] = pd.to_numeric(df['FIRSTYEAR'], errors='coerce')
    df['LASTYEAR'] = pd.to_numeric(df['LASTYEAR'], errors='coerce')

    # Entferne Zeilen mit fehlenden Werten in den essentiellen Spalten
    df = df.dropna(subset=['ID', 'LATITUDE', 'LONGITUDE'])

    # Falls ein Stationseintrag mehrfach vorkommt (für unterschiedliche ELEMENTE),
    # wählen wir einen Eintrag pro Station (z.B. den ersten Eintrag)
    df_unique = df.drop_duplicates(subset=['ID'])
    app.logger.info(f"Es wurden {len(df_unique)} eindeutige Stationen aus dem Inventory geladen")

    return df_unique


@lru_cache(maxsize=1)
def read_station_cities(csv_url_city):
    """
    Reads a fixed-width file containing station metadata and returns a dictionary
    mapping Station ID to its NAME.

    Expected format:
        ID            1-11   Character
        LATITUDE     13-20   Real
        LONGITUDE    22-30   Real
        ELEVATION    32-37   Real
        STATE        39-40   Character
        NAME         42-71   Character
        GSN FLAG     73-75   Character
        HCN/CRN FLAG 77-79   Character
        WMO ID       81-85   Character
    """
    import pandas as pd
    colspecs = [
        (0, 11),  # ID: columns 1-11 (0-indexed: 0 to 11)
        (12, 20),  # LATITUDE: columns 13-20
        (21, 30),  # LONGITUDE: columns 22-30
        (31, 37),  # ELEVATION: columns 32-37
        (38, 40),  # STATE: columns 39-40
        (41, 71),  # NAME: columns 42-71
        (72, 75),  # GSN FLAG: columns 73-75
        (76, 79),  # HCN/CRN FLAG: columns 77-79
        (80, 85)  # WMO ID: columns 81-85
    ]
    names = ["ID", "LATITUDE", "LONGITUDE", "ELEVATION", "STATE", "NAME", "GSN_FLAG", "HCN_CRN_FLAG", "WMO_ID"]
    df = pd.read_fwf(csv_url_city, colspecs=colspecs, header=None, names=names)
    # Create mapping from ID to NAME, trimming any extra whitespace
    mapping = dict(zip(df["ID"].str.strip(), df["NAME"].str.strip()))
    return mapping


def find_stations_within_radius(inventory_url, lat, lon, max_dist_km, max_stations, firstyear, lastyear):
    """
    Sucht die nächstgelegenen Stationen innerhalb eines bestimmten Radius,
    die außerdem im angegebenen Zeitraum (start_year bis end_year) Daten liefern.
    """
    # Lese die Stationendaten (wird dank lru_cache nur einmal geladen)
    stations_df = read_ghcnd_stations(inventory_url)

    # Filtere nach Zeitraum: Die Station muss Daten ab dem start_year und bis zum end_year haben
    stations_df = stations_df[(stations_df['FIRSTYEAR'] <= firstyear) & (stations_df['LASTYEAR'] >= lastyear)].copy()

    # Berechne die Entfernungen für alle Stationen (vektorisiert)
    stations_df['distance_km'] = haversine(
        lat,
        lon,
        stations_df['LATITUDE'].values,
        stations_df['LONGITUDE'].values
    )

    # Filtere Stationen, die innerhalb des angegebenen Radius liegen, und sortiere sie
    nearby_stations = (stations_df[stations_df['distance_km'] <= max_dist_km]
                       .sort_values('distance_km')
                       .head(max_stations))

    # Erstelle eine Liste von Dictionaries
    stations_list = [
        {
            "station_id": str(row['ID']),
            "latitude": float(row['LATITUDE']),
            "longitude": float(row['LONGITUDE']),
            "distance_km": float(row['distance_km']),
            "firstyear": int(row['FIRSTYEAR']),
            "lastyear": int(row['LASTYEAR'])
        }
        for _, row in nearby_stations.iterrows()
    ]

    return stations_list


@lru_cache(maxsize=100)  # Cache up to 100 different station results
def process_station_data(station_id: str, firstyear: int, lastyear: int, station_lat: float) -> tuple:
    """Process and cache weather data for a specific station,
    including overall yearly statistics and seasonal (Winter, Spring, Summer, Autumn) max/min temperatures.
    Only returns data for years between firstyear and lastyear (inclusive).
    The season names are switched if station_lat is negative (Southern Hemisphere)."""
    app.logger.info(f"Processing weather data for station {station_id} - cache miss")

    station_data_url = f'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_station/{station_id}.csv.gz'
    import gzip

    response = requests.get(station_data_url, stream=True)
    response.raise_for_status()

    with gzip.GzipFile(fileobj=response.raw) as f:
        data = pd.read_csv(
            f,
            header=None,
            names=['ID', 'DATE', 'ELEMENT', 'VALUE', 'M-FLAG', 'Q-FLAG', 'S-FLAG', 'OBS-TIME'],
            dtype={
                'ID': str,
                'DATE': str,
                'ELEMENT': str,
                'VALUE': float,
                'M-FLAG': str,
                'Q-FLAG': str,
                'S-FLAG': str,
                'OBS-TIME': str
            },
            low_memory=False
        )

    # Convert DATE to datetime and extract YEAR as string
    data['DATE'] = pd.to_datetime(data['DATE'], format='%Y%m%d', errors='coerce')
    data['YEAR'] = data['DATE'].dt.year.astype(str)

    # Filter only relevant elements
    filtered_data = data[data['ELEMENT'].isin(['TMAX', 'TMIN', 'PRCP'])].copy()
    filtered_data['VALUE'] = pd.to_numeric(filtered_data['VALUE'], errors='coerce')
    # Convert TMAX and TMIN from tenths of °C to °C
    filtered_data.loc[filtered_data['ELEMENT'].isin(['TMAX', 'TMIN']), 'VALUE'] = \
        filtered_data.loc[filtered_data['ELEMENT'].isin(['TMAX', 'TMIN']), 'VALUE'] / 10

    # --- Yearly Statistics ---
    temp_data = filtered_data[filtered_data['ELEMENT'].isin(['TMAX', 'TMIN'])]
    temp_summary = temp_data.groupby(['YEAR', 'ELEMENT'])['VALUE'].agg(['max', 'min', 'mean']).unstack()
    max_temps = temp_summary['max']['TMAX'] if 'TMAX' in temp_summary['max'] else None
    min_temps = temp_summary['min']['TMIN'] if 'TMIN' in temp_summary['min'] else None
    overall_avg_temp = temp_data.groupby('YEAR')['VALUE'].mean()

    rain_data = filtered_data[filtered_data['ELEMENT'] == 'PRCP']
    avg_rain_per_year = rain_data.groupby('YEAR')['VALUE'].mean()

    yearly_result = pd.DataFrame({
        'Max_Temperature (°C)': max_temps,
        'Min_Temperature (°C)': min_temps,
        'Year_Avg_Temperature (°C)': overall_avg_temp,
        'Year_Avg_Rain (mm)': avg_rain_per_year
    }).fillna(0)

    # --- Seasonal Statistics ---
    season_data = temp_data.copy()
    season_data['month'] = season_data['DATE'].dt.month
    season_data['year_int'] = season_data['DATE'].dt.year

    def get_season(month: int) -> str:
        # For Northern Hemisphere
        if station_lat >= 0:
            if month in [3, 4, 5]:
                return 'Spring'
            elif month in [6, 7, 8]:
                return 'Summer'
            elif month in [9, 10, 11]:
                return 'Autumn'
            else:
                return 'Winter'
        else:
            # Southern Hemisphere: switch the seasons
            if month in [3, 4, 5]:
                return 'Autumn'
            elif month in [6, 7, 8]:
                return 'Winter'
            elif month in [9, 10, 11]:
                return 'Spring'
            else:
                return 'Summer'

    season_data['season'] = season_data['month'].apply(get_season)
    # For grouping Winter correctly, shift December to the next year (this applies for both hemispheres)
    season_data['season_year'] = season_data['year_int']
    season_data.loc[season_data['month'] == 12, 'season_year'] += 1

    tmax_data = season_data[season_data['ELEMENT'] == 'TMAX']
    tmin_data = season_data[season_data['ELEMENT'] == 'TMIN']

    seasonal_max = tmax_data.groupby(['season_year', 'season'])['VALUE'].max().unstack()
    seasonal_min = tmin_data.groupby(['season_year', 'season'])['VALUE'].min().unstack()

    seasonal_summary = {}
    for season_year in sorted(season_data['season_year'].unique()):
        season_year_str = str(season_year)
        seasonal_summary[season_year_str] = {}
        for season in ['Winter', 'Spring', 'Summer', 'Autumn']:
            max_val = seasonal_max.loc[season_year, season] if season in seasonal_max.columns and season_year in seasonal_max.index else None
            min_val = seasonal_min.loc[season_year, season] if season in seasonal_min.columns and season_year in seasonal_min.index else None
            seasonal_summary[season_year_str][season] = {
                'Max_Temperature (°C)': max_val if max_val is not None else 0,
                'Min_Temperature (°C)': min_val if min_val is not None else 0
            }

    # Filter: Only include years between firstyear and lastyear
    yearly_filtered = {year: data for year, data in yearly_result.to_dict(orient='index').items()
                       if firstyear <= int(year) <= lastyear}
    seasonal_filtered = {year: data for year, data in seasonal_summary.items()
                         if firstyear <= int(year) <= lastyear}

    from flask import jsonify
    final_result = {
        'yearly_summary': yearly_filtered,
        'seasonal_summary': seasonal_filtered
    }

    # Replace any NaN values (using numpy version)
    final_result = replace_nan_with_none(final_result)

    return jsonify(final_result), 200


@app.route('/api/station_data', methods=['GET'])
def get_station_weather_data():
    station_id = request.args.get('station_id')
    if not station_id:
        return {"error": "Missing station_id"}, 400
    try:
        firstyear = int(request.args.get('firstyear', 1900))
        lastyear = int(request.args.get('lastyear', 2100))
        station_lat = float(request.args.get('station_lat', 0))  # default 0 (Equator)
        app.logger.info(f"Fetching data for station {station_id} for years between {firstyear} and {lastyear}, lat: {station_lat}")
        return process_station_data(station_id, firstyear, lastyear, station_lat)
    except Exception as e:
        app.logger.error(f"Error processing station data: {e}")
        return {"error": str(e)}, 500



@app.route('/api/find_stations', methods=['GET'])
def find_stations():
    import json

    # Use the GHCN inventory file for stations
    inventory_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"
    # Use your fixed-width city file (mapping ID to NAME)
    csv_url_city = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"  # Replace with your actual URL

    lat = float(request.args.get("lat", 48.060711110885094))
    lon = float(request.args.get("lon", 8.533784762385885))
    max_dist_km = float(request.args.get("max_dist_km", 50.0))
    max_stations = int(request.args.get("max_stations", 5))
    firstyear = int(request.args.get("firstyear", 2010))
    lastyear = int(request.args.get("lastyear", 2015))

    try:
        stations = find_stations_within_radius(
            inventory_url=inventory_url,
            lat=lat,
            lon=lon,
            max_dist_km=max_dist_km,
            max_stations=max_stations,
            firstyear=firstyear,
            lastyear=lastyear
        )

        # Build the mapping from Station ID to Station Name (city)
        station_names = read_station_cities(csv_url_city)

        # For each station, add the city info based on its ID
        for station in stations:
            station_id = station.get("station_id")
            station["city"] = station_names.get(station_id, "Unknown")

        def generate():
            for station in stations:
                yield f"data: {json.dumps(station)}\n\n"
            time.sleep(1)
            yield "data: finished\n\n"

    except Exception as e:
        app.logger.error(f"Fehler bei der Stationssuche: {e}")
        return {"error": str(e)}, 500

    return Response(
        generate(),
        content_type='text/event-stream',
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )

@app.route("/")
def index():
    return render_template('index.html')

if __name__ == '__main__':
    with app.app_context():
        try:
            app.logger.info("Preloading station data...")
            csv_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"
            csv_url_city = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
            read_ghcnd_stations(csv_url) # This will store the data in cache
            read_station_cities(csv_url_city) # This will store the data in cache
            app.logger.info("Station data preloaded successfully.")
        except Exception as e:
            app.logger.error(f"Error preloading station data: {e}")

    app.run(host='0.0.0.0', port=8090)

