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
    """Finds stations within a specified radius and yields them immediately."""
    # Read the station data using the fixed-width parser
    stations_df = read_ghcnd_stations(csv_url)

    # Coordinates of the given location
    given_coords = (lat, lon)
    count = 0  # Track how many stations have been yielded

    # Iterate over stations and calculate distance
    for _, row in stations_df.iterrows():
        station_lat = row['LATITUDE']
        station_lon = row['LONGITUDE']
        dist_km = geodesic(given_coords, (station_lat, station_lon)).kilometers

        # If the station is within the maximum distance, yield it immediately
        if dist_km <= max_dist_km:
            yield {
                "station_id": str(row['ID']),
                "latitude": station_lat,
                "longitude": station_lon,
                "distance_km": dist_km
            }
            count += 1

            # Stop if we've yielded the maximum number of stations
            if count >= max_stations:
                break

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

    def generate():
        try:
            for station in find_stations_within_radius(
                    csv_url=csv_url, lat=lat, lon=lon, max_dist_km=max_dist_km, max_stations=max_stations
            ):
                import json
                #print(f"Streaming station: {station['station_id']}")
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
            "X-Accel-Buffering": "no"  # Disable buffering if using reverse proxy
        },
    )
    response.status_code = 200
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
                console.log("Creating new EventSource connection.");
                eventSource = new EventSource(`/api/find_stations?lat=${{lat}}&lon=${{lon}}&max_dist_km=${{max_dist_km}}&max_stations=${{max_stations}}`);
                eventSource.addEventListener('message', function(e) {{
                    console.log(e);
                    var data = e.data;
                    if (data === 'finished') {{
                        console.log('closing connection')
                        eventSource.close()
                }}
                }});
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
                eventSource.onerror = () => {{
                    console.error("EventSource connection failed.", event);
                    alert("Verbindung unterbrochen. Bitte erneut versuchen.");
                }};

                eventSource.onclose = () => {{
                    console.log("EventSource connection closed.");
                }};
                eventSource.onmessage = (event) => {{
                    try {{
                        if (event.data.includes('finished')) {{
                            console.log("Stream finished:", event.data);
                            return; // Exit if it's the end marker
                        }}
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
