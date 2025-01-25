from flask import Flask, request
import csv
import requests
from geopy.distance import geodesic
import time
import pandas as pd
from functools import lru_cache
app = Flask(__name__, static_folder='static')

@lru_cache(maxsize=1)
def load_and_cache_csv(csv_url):
    response = requests.get(csv_url)
    response.raise_for_status()
    return response.text.splitlines()

def find_stations_within_radius(csv_url, lat, lon, max_dist_km, max_stations):
    # Load cached CSV content
    content = load_and_cache_csv(csv_url)
    reader = csv.reader(content)

    count = 0  # Track the number of stations yielded
    for row in reader:
        if count >= max_stations:
            break
        station_id = row[0].strip()
        station_lat = float(row[1])
        station_lon = float(row[2])
        dist_km = geodesic((lat, lon), (station_lat, station_lon)).kilometers

        if dist_km <= max_dist_km:
            # Prepare the station dictionary
            station_info = {
                "station_id": station_id,
                "latitude": station_lat,
                "longitude": station_lon,
                "distance_km": dist_km
            }

            # Add average Tmin if available
            station_data_url = f"https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_station/{station_id}.csv.gz"
            avg_tmin = 5
            station_info["average_tmin"] = avg_tmin

            count += 1  # Increment the count of yielded stations
            print(count, max_stations)
            yield station_info  # Yield the station as soon as it is ready


def calculate_average_tmin(station_data_url):
    import gzip
    response = requests.get(station_data_url)
    response.raise_for_status()
    with gzip.GzipFile(fileobj=response.raw) as f:
        csv_content = f.read().decode("utf-8")
    reader = csv.reader(csv_content.splitlines())
    tmin_values = []
    for row in reader:
        try:
            if row[2] == "TMIN":
                tmin_values.append(float(row[3]) / 10.0)  # Convert to Celsius
        except (IndexError, ValueError):
            continue
    if tmin_values:
        print(sum(tmin_values) / len(tmin_values))
        return sum(tmin_values) / len(tmin_values)
    return None

@app.route('/api/find_stations', methods=['GET'])
def find_stations():
    from flask import Response

    csv_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.csv"
    lat = float(request.args.get("lat", 48.060711110885094))
    lon = float(request.args.get("lon", 8.533784762385885))
    max_dist_km = float(request.args.get("max_dist_km", 50.0))
    max_stations = int(request.args.get("max_stations", 5))

    def generate_stream():
        for station in find_stations_within_radius(
                csv_url=csv_url, lat=lat, lon=lon, max_dist_km=max_dist_km, max_stations=max_stations
        ):
            import json
            print(f"Streaming station: {station['station_id']}")
            yield f"data: {json.dumps(station)}\n\n"
        yield "event: end\n"
        yield "data: complete\n\n"
        time.sleep(1)  # Keep the connection alive briefly before closing

    response = Response(generate_stream(), content_type='text/event-stream')
    return response

@app.route('/', methods=['GET'])
def render_form():
    """
    HTML-Seite mit Formular und Google Maps anzeigen.
    """
    lat = request.args.get("lat", 48.060711110885094)  # Default-Werte
    lon = request.args.get("lon", 8.533784762385885)
    max_dist_km = request.args.get("max_dist_km", 50.0)
    max_stations = request.args.get("max_stations", 1)
    html_output = f"""
    <html>
        <head>
            <script src="https://maps.googleapis.com/maps/api/js?key=AIzaSyAxOzRDNqtyLTKUK83-j7auXehzWmoCoaY&libraries=marker">
            </script>
        <script>
            let markers = [];
            let map = null;
            let eventSource = null;
            async function fetchStations() {{
                const lat = document.getElementById('lat').value;
                const lon = document.getElementById('lon').value;
                const max_dist_km = document.getElementById('max_dist_km').value;
                const max_stations = document.getElementById('max_stations').value;
                console.log("Calling fetchStations()...");
                if (eventSource) {{
                    console.log("Closing existing EventSource connection.");
                    eventSource.close();
                }}
                console.log("Creating new EventSource connection.");
                eventSource = new EventSource(`/api/find_stations?lat=${{lat}}&lon=${{lon}}&max_dist_km=${{max_dist_km}}&max_stations=${{max_stations}}`);
                console.log("New EventSource created:", eventSource);
                markers.forEach(marker => marker.setMap(null));
                markers = [];
                if (!map) {{
                    map = new google.maps.Map(document.getElementById("map"), {{
                        zoom: 10,
                        center: {{ lat: parseFloat(lat), lng: parseFloat(lon) }},
                        mapId: "Map1"
                    }});
                }} else {{
                    map.setCenter({{ lat: parseFloat(lat), lng: parseFloat(lon) }});
                }}
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
                // Add a click listener for each marker, and set up the info window.
                marker2.addListener("click", ({{ domEvent, latLng }}) => {{
                    maik.style.transition = "width 0.5s ease-in-out";
                    maik.width = maik.width === 44 ? 400 : 44;
                    const {{ target }} = domEvent;
                }});
                eventSource.onmessage = (event) => {{
                    try {{
                        const station = JSON.parse(event.data);
                        console.log("Station data received:", station);
                        const WetterStationImg = document.createElement("img");
                        WetterStationImg.src = "https://cdn-icons-png.flaticon.com/512/1809/1809492.png";
                        WetterStationImg.width = 44;
                        WetterStationImg.height = 44;
                        let marker = new google.maps.marker.AdvancedMarkerElement({{
                            map: map,
                            position: {{
                                lat: station.latitude,
                                lng: station.longitude
                            }},
                            gmpClickable: true,
                            content: WetterStationImg,
                            title: `Station ID: ${{station.station_id}}`
                        }});
                        markers.push(marker);
                        const infoWindow = new google.maps.InfoWindow();
                        marker.addListener("click", ({{ domEvent, latLng }}) => {{
                           const {{ target }} = domEvent;
                           infoWindow.close();
                           infoWindow.setContent(`
                              <div>
                                <h3>${{marker.title}}</h3>
                                <p>Average Temperature ${{station.average_tmin ?? "N/A"}}</p>
                              </div>
                            `);
                           infoWindow.open(marker.map, marker);
                        }});
                    }} catch (e) {{
                        console.error("Error parsing station data:", e);
                    }}
                }}
            }};
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
            <div id="map" style="height: 75vh; width: 100%; margin-top:20px;"></div>
        </body>
    </html>
    """
    return html_output, 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8090)


#window.onload = async () => {{ await fetchStations(); }};