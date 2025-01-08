from flask import Flask, request, jsonify
import csv
import requests
from geopy.distance import geodesic

app = Flask(__name__)

def find_stations_within_radius(csv_url, lat, lon, max_dist_km, max_stations):
    """
    Lädt die CSV-Datei von csv_url und gibt eine Liste von Stationen zurück,
    die innerhalb von max_dist_km liegen, bis max_stations erreicht ist.
    """
    stations = []

    # CSV aus dem Web laden
    response = requests.get(csv_url)
    response.raise_for_status()
    content = response.text.splitlines()
    reader = csv.reader(content)

    # Jede Zeile im CSV verarbeiten
    for row in reader:
        try:
            station_id = row[0].strip()
            station_lat = float(row[1])
            station_lon = float(row[2])

            # Entfernung berechnen
            dist_km = geodesic((lat, lon), (station_lat, station_lon)).kilometers

            if dist_km <= max_dist_km:
                stations.append({
                    "station_id": station_id,
                    "latitude": station_lat,
                    "longitude": station_lon,
                    "distance_km": dist_km
                })

                # Maximalanzahl an Stationen beachten
                if len(stations) >= max_stations:
                    break
        except (ValueError, IndexError):
            continue

    return stations

@app.route('/api/find_stations', methods=['GET'])
def find_stations():
    """
    API-Endpoint, um Wetterstationen basierend auf den übergebenen Parametern zu finden.
    Erwartet:
      - csv_url (Pfad zur CSV)
      - lat (Breitengrad)
      - lon (Längengrad)
      - max_dist_km (Suchradius)
      - max_stations (Maximale Anzahl an Ergebnissen)
    """
    # Standardwerte
    csv_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.csv"
    lat = float(request.args.get("lat", 48.060711110885094))  # Default: Beispiel-Koordinaten
    lon = float(request.args.get("lon", 8.533784762385885))
    max_dist_km = float(request.args.get("max_dist_km", 50.0))
    max_stations = int(request.args.get("max_stations", 5))

    # Ergebnisse berechnen
    try:
        stations = find_stations_within_radius(csv_url, lat, lon, max_dist_km, max_stations)
        return jsonify({"stations": stations})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/form', methods=['GET'])
def form():
    """
    Gibt ein HTML-Formular aus, um die Variablen zu setzen.
    """
    return """
    <html>
    <body>
        <h1>Station Finder</h1>
        <form action="/api/find_stations" method="get">
            Latitude: <input type="text" name="lat" value="48.060711110885094"><br>
            Longitude: <input type="text" name="lon" value="8.533784762385885"><br>
            Max Distance (km): <input type="text" name="max_dist_km" value="50"><br>
            Max Stations: <input type="text" name="max_stations" value="5"><br>
            <input type="submit" value="Find Stations">
        </form>
    </body>
    </html>
    """

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8090)