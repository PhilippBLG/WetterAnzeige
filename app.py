from flask import Flask, request, render_template
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

@lru_cache(maxsize=1)
def load_and_cache_csv(csv_url):
    response = requests.get(csv_url)
    response.raise_for_status()
    return response.text.splitlines()


def read_ghcnd_stations(url):
    """Reads and parses the GHCND stations file from a given URL."""
    # Read the file into a pandas DataFrame
    stations_pd = pd.read_csv(url, header=None, on_bad_lines='skip', encoding='utf-8', dtype=str)

    data = []
    for line in stations_pd.values:
        # Ensure each field is extracted properly and converted to string
        line_str = ''.join(line)  # Convert array to a single string
        if len(line_str.strip()) == 0:
            continue  # Skip empty lines

        # Extract fields using fixed-width slicing
        fields = [
            line_str[0:11].strip(),  # Station ID
            line_str[12:20].strip(),  # Latitude
            line_str[21:30].strip(),  # Longitude
            line_str[30:50].strip()  # Add any other fields as needed
        ]

        try:
            data.append({
                'ID': fields[0],
                'LATITUDE': float(fields[1]),
                'LONGITUDE': float(fields[2])
            })
        except ValueError:
            # Handle cases where LATITUDE or LONGITUDE cannot be converted to float
            continue

    df = pd.DataFrame(data)
    return df


def find_stations_within_radius(csv_url, lat, lon, max_dist_km, max_stations):
    """Finds stations within a specified radius and yields them immediately using pandas for performance."""
    # Read the station data using the fixed-width parser
    stations_df = read_ghcnd_stations(csv_url)

    # Coordinates of the given location
    given_lat, given_lon = lat, lon
    count = 0

    # Iterate through stations and calculate distance on-the-fly
    for _, row in stations_df.iterrows():
        station_lat = row['LATITUDE']
        station_lon = row['LONGITUDE']
        dist_km = haversine(given_lat, given_lon, station_lat, station_lon)

        # If the station is within the maximum distance, yield it immediately
        if dist_km <= max_dist_km:
            yield {
                "station_id": str(row['ID']),
                "latitude": station_lat,
                "longitude": station_lon,
                "distance_km": dist_km,
            }
            count += 1

            # Stop if we've yielded the maximum number of stations
            if count >= max_stations:
                break

@app.route('/api/station_data', methods=['GET'])
def get_station_weather_data():
    station_id = request.args.get('station_id')
    if not station_id:
        return {"error": "Missing station_id"}, 400
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
                'ID': str,  # Station ID is likely a string
                'DATE': str,  # Date is a string
                'ELEMENT': str,  # Element type is a string (e.g., TMAX, TMIN)
                'VALUE': float,  # Values are decimal numbers
                'M-FLAG': str,
                'Q-FLAG': str,
                'S-FLAG': str,
                'OBS-TIME': str  # Observation time is probably a string
            },
            low_memory=False
        )

    # Jahr aus Datum extrahieren

    data['YEAR'] = data['DATE'].astype(str).str[:4]

    # Filter relevant elements
    filtered_data = data[data['ELEMENT'].isin(['TMAX', 'TMIN', 'PRCP'])].copy()

    # Clean and convert the values
    filtered_data.loc[:, 'VALUE'] = pd.to_numeric(filtered_data['VALUE'], errors='coerce')

    # Scale the values to correct units (°C for temperature, mm for precipitation)
    filtered_data.loc[:, 'VALUE'] = filtered_data.apply(
        lambda row: row['VALUE'] / 10 if row['ELEMENT'] in ['TMAX', 'TMIN'] else row['VALUE'], axis=1
    )

    # Group temperature data by year and element to calculate max, min, and averages
    temp_data = filtered_data[filtered_data['ELEMENT'].isin(['TMAX', 'TMIN'])]
    temp_summary = temp_data.groupby(['YEAR', 'ELEMENT'])['VALUE'].agg(['max', 'min', 'mean']).unstack()

    # Extract temperature data
    max_temps = temp_summary['max']['TMAX'] if 'TMAX' in temp_summary['max'] else None
    min_temps = temp_summary['min']['TMIN'] if 'TMIN' in temp_summary['min'] else None

    # Calculate the overall average temperature for the year
    overall_avg_temp = temp_data.groupby('YEAR')['VALUE'].mean()

    # Process rain data (`PRCP`)
    rain_data = filtered_data[filtered_data['ELEMENT'] == 'PRCP']

    # Calculate yearly average rain
    avg_rain_per_year = rain_data.groupby('YEAR')['VALUE'].mean()

    # Calculate overall average rain (`Avg_Rain`)

    # Combine results into a single DataFrame
    result = pd.DataFrame({
        'Max_Temperature (°C)': max_temps,
        'Min_Temperature (°C)': min_temps,
        'Year_Avg_Temperature (°C)': overall_avg_temp,
        'Year_Avg_Rain (mm)': avg_rain_per_year
    })

    # Handle missing values (if any years are incomplete)
    result = result.fillna(0)

    # Return the result as JSON
    return result.to_json(orient='index'), 200

@app.route('/api/find_stations', methods=['GET'])
def find_stations():
    from flask import Response

    csv_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.csv"
    lat = float(request.args.get("lat", 48.060711110885094))
    lon = float(request.args.get("lon", 8.533784762385885))
    max_dist_km = float(request.args.get("max_dist_km", 50.0))
    max_stations = int(request.args.get("max_stations", 5))

    def generate():
        try:
            for station in find_stations_within_radius(
                    csv_url=csv_url, lat=lat, lon=lon, max_dist_km=max_dist_km, max_stations=max_stations
            ):
                import json
                yield f"data: {json.dumps(station)}\n\n"
            time.sleep(1)  # Keep the connection alive briefly before closing
            yield "data: finished\n\n"
        except GeneratorExit:
            app.logger.info("Client disconnected.")
        except Exception as e:
            app.logger.error(f"Error during SSE: {e}")

    response = Response(generate(), content_type='text/event-stream',
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",  # Ensure the connection remains open
        },
    )
    response.status_code = 200
    return response

@app.route("/")
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8090)
