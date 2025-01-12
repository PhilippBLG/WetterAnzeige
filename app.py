from flask import Flask, request, jsonify
import csv
import requests
from geopy.distance import geodesic

app = Flask(__name__, static_folder='static')


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
        except Exception as e:
            print(f"Error processing row {row}: {e}")

            continue
        except Exception as e:
            print(f"Error processing row {row}: {e}")

    stations = sorted(stations, key=lambda k: k['distance_km'])
    for station in stations:
        station["station_id"] = str(station["station_id"])
        station["latitude"] = float(station["latitude"])
        station["longitude"] = float(station["longitude"])
        station["distance_km"] = float(station["distance_km"])

    stations = sorted(stations, key=lambda k: k['distance_km'])
    for station in stations:
        station["station_id"] = str(station["station_id"])
        station["latitude"] = float(station["latitude"])
        station["longitude"] = float(station["longitude"])
        station["distance_km"] = float(station["distance_km"])
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
    # Standardwerte für `/api/find_stations`
    csv_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.csv"
    lat = float(request.args.get("lat", 48.060711110885094))  # Default: Beispiel-Koordinaten
    lon = float(request.args.get("lon", 8.533784762385885))
    max_dist_km = float(request.args.get("max_dist_km", 50.0))
    max_stations = int(request.args.get("max_stations", 5))

    # Ergebnisse berechnen
    try:
        stations = find_stations_within_radius(csv_url, lat, lon, max_dist_km, max_stations)
        if not stations:
            print("No stations found.")
            return jsonify([]), 200
        print(f"Stations found: {stations}")
        return jsonify(stations), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Standardwerte für `/api/find_stations`
    csv_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.csv"
    lat = float(request.args.get("lat", 48.060711110885094))  # Default: Beispiel-Koordinaten
    lon = float(request.args.get("lon", 8.533784762385885))
    max_dist_km = float(request.args.get("max_dist_km", 50.0))
    max_stations = int(request.args.get("max_stations", 5))

    # Ergebnisse berechnen
    try:
        stations = find_stations_within_radius(csv_url, lat, lon, max_dist_km, max_stations)
        return jsonify(stations), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/', methods=['GET'])
def render_form():
    """
    HTML-Seite mit Formular und Google Maps anzeigen.
    """
    lat = request.args.get("lat", 48.060711110885094)  # Default-Werte
    lon = request.args.get("lon", 8.533784762385885)
    max_dist_km = request.args.get("max_dist_km", 50.0)
    max_stations = request.args.get("max_stations", 5)
    html_output = f"""
    <html>
        <head>
            <script src="https://maps.googleapis.com/maps/api/js?key=AIzaSyAxOzRDNqtyLTKUK83-j7auXehzWmoCoaY&libraries=marker">
            </script>
            <script>
                async function fetchStations() {{
                    const lat = document.getElementById('lat').value;
                    const lon = document.getElementById('lon').value;
                    const max_dist_km = document.getElementById('max_dist_km').value;
                    const max_stations = document.getElementById('max_stations').value;
                    const response = await fetch(`/api/find_stations?lat=${{lat}}&lon=${{lon}}&max_dist_km=${{max_dist_km}}&max_stations=${{max_stations}}`);
                    const data = await response.json();
                    const map = new google.maps.Map(document.getElementById("map"), {{
                        zoom: 10,
                        center: {{ lat: parseFloat(lat), lng: parseFloat(lon) }},
                        mapId: "Map1",
                    }});
                    const maik = document.createElement("img");
                    maik.src = "/static/Subject.png";
                    maik.width = 44;
                    const marker2 = new google.maps.marker.AdvancedMarkerElement({{
                                map: map,
                                position: {{ lat: parseFloat(lat), lng: parseFloat(lon) }},
                                content: maik,
                                title: `Home`,
                                gmpClickable: true,
                            }});
                    data.forEach(station => {{
                        const WetterStationImg = document.createElement("img");
                        WetterStationImg.src = "https://cdn-icons-png.flaticon.com/512/1809/1809492.png";
                        WetterStationImg.width = 44;
                        WetterStationImg.height = 44;
                        const marker = new google.maps.marker.AdvancedMarkerElement({{
                            map: map,
                            position: {{
                                lat: station.latitude,
                                lng: station.longitude
                            }},
                            content: WetterStationImg,
                            title: `Station ID: ${{station.station_id}}`,
                        }});
                    }});
                    // Add a click listener for each marker, and set up the info window.
                    marker2.addListener("click", ({{ domEvent, latLng }}) => {{
                      maik.style.transition = "width 0.5s ease-in-out";
                      maik.width = 200;
                      const {{ target }} = domEvent;
                    
                      infoWindow.close();
                      infoWindow.setContent(marker2.title);
                      infoWindow.open(marker2.map, marker2);
                    }});
                }}
            </script>
        </head>
        <body>
            <h1>Station Finder</h1>
            <form onsubmit="event.preventDefault(); fetchStations();">
                Latitude: <input id="lat" type="text" value="{lat}"><br>
                Longitude: <input id="lon" type="text" value="{lon}"><br>
                Max Distance (km): <input id="max_dist_km" type="text" value="{max_dist_km}"><br>
                Max Stations: <input id="max_stations" type="text" value="{max_stations}"><br>
                <input type="submit" value="Find Stations">
            </form>
            <div id="map" style="height: 600px; width: 100%; margin-top:20px;"></div>
        </body>
    </html>
    """
    return html_output, 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8090)