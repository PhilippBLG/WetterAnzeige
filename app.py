import time
import json
import gzip
from functools import lru_cache
import numpy as np
import pandas as pd
import requests
from flask import Flask, request, render_template, Response, jsonify
import logging
import os

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# Konstanten für Fixed-Width-Dateien
GHCND_COLSPECS = [
    (0, 11),   # ID: Spalte 1-11
    (12, 20),  # LATITUDE: Spalte 13-20
    (21, 30),  # LONGITUDE: Spalte 22-30
    (31, 35),  # ELEMENT: Spalte 32-35
    (36, 40),  # FIRSTYEAR: Spalte 37-40
    (41, 45)   # LASTYEAR: Spalte 42-45
]
GHCND_NAMES = ['ID', 'LATITUDE', 'LONGITUDE', 'ELEMENT', 'FIRSTYEAR', 'LASTYEAR']

STATION_CITY_COLSPECS = [
    (0, 11),   # ID: Spalte 1-11
    (12, 20),  # LATITUDE: Spalte 13-20
    (21, 30),  # LONGITUDE: Spalte 22-30
    (31, 37),  # ELEVATION: Spalte 32-37
    (38, 40),  # STATE: Spalte 39-40
    (41, 71),  # NAME: Spalte 42-71
    (72, 75),  # GSN FLAG: Spalte 73-75
    (76, 79),  # HCN/CRN FLAG: Spalte 77-79
    (80, 85)   # WMO ID: Spalte 81-85
]
STATION_CITY_NAMES = ["ID", "LATITUDE", "LONGITUDE", "ELEVATION", "STATE", "NAME", "GSN_FLAG", "HCN_CRN_FLAG", "WMO_ID"]


# Hilfsfunktionen
def haversine(lat1, lon1, lat2, lon2):
    """Berechnet die Großkreisentfernung zwischen zwei Punkten auf der Erde."""
    R = 6371.0  # Erdradius in Kilometern
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
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
    return obj


@lru_cache(maxsize=1)
def read_ghcnd_stations(url: str) -> pd.DataFrame:
    """
    Reads and parses the GHCND Inventory file from the given URL,
    and only keeps records with the ELEMENT 'TMIN' or 'TMAX'.
    """
    app.logger.info("Lade Inventory-Datei von URL (Cache miss)")
    df = pd.read_fwf(url, colspecs=GHCND_COLSPECS, header=None, names=GHCND_NAMES)

    # Filter rows: keep only rows where ELEMENT is 'TMIN' or 'TMAX'
    df = df[df['ELEMENT'].isin(['TMIN', 'TMAX'])]

    # Convert columns to numeric types
    for col in ['LATITUDE', 'LONGITUDE', 'FIRSTYEAR', 'LASTYEAR']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Remove rows with missing essential values
    df = df.dropna(subset=['ID', 'LATITUDE', 'LONGITUDE'])

    # Remove duplicate station entries (only one per station)
    df_unique = df.drop_duplicates(subset=['ID'])
    app.logger.info(f"Es wurden {len(df_unique)} eindeutige Stationen geladen")
    return df_unique


@lru_cache(maxsize=1)
def read_station_cities(csv_url_city: str) -> dict:
    """
    Liest eine Fixed-Width-Datei mit Stationsmetadaten und erstellt ein Mapping von Station ID zu NAME.
    """
    df = pd.read_fwf(csv_url_city, colspecs=STATION_CITY_COLSPECS, header=None, names=STATION_CITY_NAMES)
    mapping = dict(zip(df["ID"].str.strip(), df["NAME"].str.strip()))
    return mapping


def find_stations_within_radius(inventory_url: str, lat: float, lon: float,
                                max_dist_km: float, max_stations: int,
                                firstyear: int, lastyear: int) -> list:
    """
    Sucht die nächstgelegenen Stationen innerhalb eines bestimmten Radius,
    die im angegebenen Zeitraum Daten liefern.
    """
    stations_df = read_ghcnd_stations(inventory_url)
    stations_df = stations_df[(stations_df['FIRSTYEAR'] <= firstyear) & (stations_df['LASTYEAR'] >= lastyear)].copy()
    stations_df['distance_km'] = haversine(lat, lon, stations_df['LATITUDE'].values, stations_df['LONGITUDE'].values)
    nearby = stations_df[stations_df['distance_km'] <= max_dist_km].sort_values('distance_km').head(max_stations)
    return [
        {
            "station_id": str(row['ID']),
            "latitude": float(row['LATITUDE']),
            "longitude": float(row['LONGITUDE']),
            "distance_km": float(row['distance_km']),
            "firstyear": int(row['FIRSTYEAR']),
            "lastyear": int(row['LASTYEAR'])
        }
        for _, row in nearby.iterrows()
    ]


def get_season(month: int, station_lat: float) -> str:
    """
    Bestimmt die Jahreszeit basierend auf dem Monat und der geografischen Breite.
    """
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
        if month in [3, 4, 5]:
            return 'Autumn'
        elif month in [6, 7, 8]:
            return 'Winter'
        elif month in [9, 10, 11]:
            return 'Spring'
        else:
            return 'Summer'


@lru_cache(maxsize=100)
def process_station_data(station_id: str, firstyear: int, lastyear: int, station_lat: float) -> tuple:
    """
    Verarbeitet und cached Wetterdaten für eine Station.
    """
    app.logger.info(f"Processing weather data for station {station_id} - cache miss")
    station_data_url = f'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_station/{station_id}.csv.gz'
    response = requests.get(station_data_url, stream=True)
    response.raise_for_status()

    with gzip.GzipFile(fileobj=response.raw) as f:
        data = pd.read_csv(
            f,
            header=None,
            names=['ID', 'DATE', 'ELEMENT', 'VALUE', 'M-FLAG', 'Q-FLAG', 'S-FLAG', 'OBS-TIME'],
            dtype={'ID': str, 'DATE': str, 'ELEMENT': str, 'VALUE': float,
                   'M-FLAG': str, 'Q-FLAG': str, 'S-FLAG': str, 'OBS-TIME': str},
            low_memory=False
        )

    data['DATE'] = pd.to_datetime(data['DATE'], format='%Y%m%d', errors='coerce')
    data['YEAR'] = data['DATE'].dt.year.astype(str)
    filtered = data[data['ELEMENT'].isin(['TMAX', 'TMIN'])].copy()
    filtered['VALUE'] = pd.to_numeric(filtered['VALUE'], errors='coerce')
    filtered.loc[filtered['ELEMENT'].isin(['TMAX', 'TMIN']), 'VALUE'] /= 10

    # Yearly Statistics based on daily averages
    # Filter only for TMAX and TMIN values
    temp_data = filtered[filtered['ELEMENT'].isin(['TMAX', 'TMIN'])].copy()

    # Pivot the data so that each date has its TMAX and TMIN in columns
    # (Assuming each day has both TMAX and TMIN)
    daily = temp_data.pivot_table(index='DATE', columns='ELEMENT', values='VALUE', aggfunc='mean')

    # Compute the daily average temperature as the mean of TMAX and TMIN
    daily['avg'] = daily[['TMAX', 'TMIN']].mean(axis=1)

    # Extract the year from the date index
    daily['YEAR'] = daily.index.year.astype(str)

    # Now, group by year and compute:
    #   - the maximum daily average,
    #   - the minimum daily average,
    #   - and the overall average daily average.
    yearly_max_avg = daily.groupby('YEAR')['avg'].max()
    yearly_min_avg = daily.groupby('YEAR')['avg'].min()
    yearly_overall_avg = daily.groupby('YEAR')['avg'].mean()

    yearly_result = pd.DataFrame({
        'Max_Temperature (°C)': yearly_max_avg,
        'Min_Temperature (°C)': yearly_min_avg,
        'Year_Avg_Temperature (°C)': yearly_overall_avg
    }).fillna(0)

    # Seasonal Statistics based on daily averages (without overall average)
    season_data = temp_data.copy()
    season_data['month'] = season_data['DATE'].dt.month
    season_data['year_int'] = season_data['DATE'].dt.year
    season_data['season'] = season_data['month'].apply(lambda m: get_season(m, station_lat))
    # For proper grouping of winter, assign December to the following year:
    season_data['season_year'] = season_data['year_int']
    season_data.loc[season_data['month'] == 12, 'season_year'] += 1

    # Pivot the data by date, season, and season_year
    daily_season = season_data.pivot_table(
        index=['DATE', 'season', 'season_year'],
        columns='ELEMENT',
        values='VALUE',
        aggfunc='mean'
    )

    # Compute the daily average as the mean of TMAX and TMIN for each day
    daily_season['avg'] = daily_season[['TMAX', 'TMIN']].mean(axis=1)

    # Group by season_year and season, then compute the maximum and minimum of the daily averages.
    seasonal_group = daily_season.groupby(['season_year', 'season'])['avg']
    seasonal_max_avg = seasonal_group.max().unstack()  # Maximum daily average per season
    seasonal_min_avg = seasonal_group.min().unstack()  # Minimum daily average per season

    # Build a dictionary for the seasonal summary (only max and min)
    seasonal_summary = {}
    for sy in sorted(season_data['season_year'].unique()):
        sy_str = str(sy)
        seasonal_summary[sy_str] = {}
        for season in ['Winter', 'Spring', 'Summer', 'Autumn']:
            max_val = seasonal_max_avg.loc[
                sy, season] if season in seasonal_max_avg.columns and sy in seasonal_max_avg.index else None
            min_val = seasonal_min_avg.loc[
                sy, season] if season in seasonal_min_avg.columns and sy in seasonal_min_avg.index else None
            seasonal_summary[sy_str][season] = {
                'Max_Temperature (°C)': max_val if max_val is not None else 0,
                'Min_Temperature (°C)': min_val if min_val is not None else 0,
            }

    yearly_filtered = {year: val for year, val in yearly_result.to_dict(orient='index').items()
                       if firstyear <= int(year) <= lastyear}
    seasonal_filtered = {year: val for year, val in seasonal_summary.items()
                         if firstyear <= int(year) <= lastyear}

    result = {
        'yearly_summary': replace_nan_with_none(yearly_filtered),
        'seasonal_summary': replace_nan_with_none(seasonal_filtered)
    }
    return jsonify(result), 200


# Flask Endpoints
@app.route('/api/station_data', methods=['GET'])
def get_station_weather_data():
    station_id = request.args.get('station_id')
    if not station_id:
        return jsonify({"error": "Missing station_id"}), 400
    try:
        firstyear = int(request.args.get('firstyear', 1900))
        lastyear = int(request.args.get('lastyear', 2100))
        station_lat = float(request.args.get('station_lat', 0))
        app.logger.info(f"Fetching data for station {station_id} for years {firstyear}-{lastyear}, lat: {station_lat}")
        return process_station_data(station_id, firstyear, lastyear, station_lat)
    except Exception as e:
        app.logger.error(f"Error processing station data: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/find_stations', methods=['GET'])
def find_stations():
    inventory_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"
    csv_url_city = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
    try:
        lat = float(request.args.get("lat", 48.060711110885094))
        lon = float(request.args.get("lon", 8.533784762385885))
        max_dist_km = float(request.args.get("max_dist_km", 50.0))
        max_stations = int(request.args.get("max_stations", 5))
        firstyear = int(request.args.get("firstyear", 2010))
        lastyear = int(request.args.get("lastyear", 2015))

        stations = find_stations_within_radius(
            inventory_url, lat, lon, max_dist_km, max_stations, firstyear, lastyear
        )
        station_names = read_station_cities(csv_url_city)
        for station in stations:
            station["city"] = station_names.get(station.get("station_id"), "Unknown")

        def generate():
            for station in stations:
                yield f"data: {json.dumps(station)}\n\n"
            time.sleep(1)
            yield "data: finished\n\n"

    except Exception as e:
        app.logger.error(f"Fehler bei der Stationssuche: {e}")
        return jsonify({"error": str(e)}), 500

    return Response(
        generate(),
        content_type='text/event-stream',
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
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
            read_ghcnd_stations(csv_url)
            read_station_cities(csv_url_city)
            app.logger.info("Station data preloaded successfully.")
        except Exception as e:
            app.logger.error(f"Error preloading station data: {e}")
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)