from flask import Flask, request, jsonify
import csv
import requests
from geopy.distance import geodesic

app = Flask(__name__, static_folder='static')


def find_stations_within_radius(csv_url, lat, lon, max_dist_km):
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
                    "distance_km": dist_km,
                    "processed": True
                })

                # Maximalanzahl an Stationen beachten
                pass  # Remove max_stations from helper
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

    return sorted(stations, key=lambda k: k['distance_km'])
    return stations

@app.route('/api/find_stations', methods=['GET'])
def find_stations():
    csv_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.csv"
    lat = float(request.args.get("lat", 48.060711110885094))
    lon = float(request.args.get("lon", 8.533784762385885))
    max_dist_km = float(request.args.get("max_dist_km", 50.0))
    max_stations = int(request.args.get("max_stations", 5))

    try:
        raw_stations = find_stations_within_radius(csv_url, lat, lon, max_dist_km)
        stations = raw_stations[:max_stations]  # Apply max_stations filter here
        if not stations:
            return jsonify([]), 200
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
                    const map = new google.maps.Map(document.getElementById("map"), {{
                        zoom: 10,
                        center: {{ lat: parseFloat(document.getElementById('lat').value), lng: parseFloat(document.getElementById('lon').value) }},
                        mapId: "Map1",
                    }});
                    const lat = document.getElementById('lat').value;
                    const lon = document.getElementById('lon').value;
                    const max_dist_km = document.getElementById('max_dist_km').value;
                    const max_stations = document.getElementById('max_stations').value;
                    const response = await fetch(`/api/find_stations?lat={lat}&lon={lon}&max_dist_km={max_dist_km}&max_stations={max_stations}`);
                    const reader = response.body.getReader();
                    const decoder = new TextDecoder("utf-8");
                    let stationsChunk = "";
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
                    marker2.addListener("click", ({{ domEvent, latLng }}) => {{
                        maik.style.transition = "width 0.5s ease-in-out";
                        maik.width = maik.width === 44 ? 400 : 44;
                    }});
                    while (true) {{
                        const {{ value, done }} = await reader.read();
                        if (done) break;
                        stationsChunk += decoder.decode(value, {{stream: true}});
                        const stations = JSON.parse(stationsChunk);
                        for (const station of stations) {{
                            const WetterStationImg = document.createElement("img");
                            WetterStationImg.src = "https://cdn-icons-png.flaticon.com/512/1809/1809492.png";
                            WetterStationImg.width = 44;
                            WetterStationImg.height = 44;
                            const marker = new google.maps.marker.AdvancedMarkerElement({{
                                map: map,
                                position: {{ lat: station.latitude, lng: station.longitude }},
                                gmpClickable: true,
                                content: WetterStationImg,
                                title: `Station ID: ${{station.station_id}}`
                            }});
                            const infoWindow = new google.maps.InfoWindow();
                            marker.addListener("click", ({{ domEvent, latLng }}) => {{
                                infoWindow.close();
                                infoWindow.setContent(`
                                    <div>
                                        <h3>${{marker.title}}</h3>
                                        <p>Additional information can go here!</p>
                                    </div>
                                `);
                                infoWindow.open(marker.map, marker);
                            }});
                        }};
                    }};
                }}
                window.onload = async () => {{ await fetchStations(); }};
            </script>
        </head>
        <body>
            <h1>Station Finder</h1>
            <form onsubmit="event.preventDefault(); fetchStations();">
                Latitude: <input id="lat" type="number" value="{lat}"><br>
                Longitude: <input id="lon" type="number" value="{lon}"><br>
                Max Distance (km): <input id="max_dist_km" type="number" value="{max_dist_km}"><br>
                Max Stations: <input id="max_stations" type="number" value="{max_stations}"><br>
                <input type="submit" value="Find Stations">
            </form>
            <div id="map" style="height: 75vh; width: 100%; margin-top:20px;"></div>
        </body>
    </html>
    """
    return html_output, 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8090)