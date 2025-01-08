import requests
import csv
from geopy.distance import geodesic

def find_stations_within_radius(csv_url, my_lat, my_lon, max_dist_km):
    """
    Lädt die CSV-Datei von csv_url und gibt jede Station zurück,
    die innerhalb max_dist_km Kilometern von (my_lat, my_lon) entfernt ist.

    Die Rückgabe erfolgt "direkt" (per Generator-Yield),
    sodass du die Ergebnisse zeilenweise verarbeiten kannst.

    Erwartete Spaltenbelegung:
        row[0] = StationID
        row[1] = Breitengrad (Lat)
        row[2] = Längengrad (Lon)
    """
    # CSV aus dem Web laden
    response = requests.get(csv_url)
    response.raise_for_status()  # Bei HTTP-Fehlern Exception

    # Inhalt in Zeilen zerlegen
    content = response.text.splitlines()
    reader = csv.reader(content)

    # Jede Zeile im CSV verarbeiten
    for row in reader:
        try:
            station_id = row[0].strip()
            station_lat = float(row[1])
            station_lon = float(row[2])

            # Entfernung mit geodesic berechnen (in km)
            dist = geodesic((my_lat, my_lon), (station_lat, station_lon)).kilometers

            # Wenn Distanz <= max_dist_km, direkt "yield" zurückgeben
            if dist <= max_dist_km:
                # Du kannst beliebig viele Infos übergeben, z. B. auch dist
                yield {
                    "station_id": station_id,
                    "latitude": station_lat,
                    "longitude": station_lon,
                    "distance_km": dist
                }

        except (ValueError, IndexError):
            # Falls eine Zeile nicht passt (z. B. fehlende Spalten),
            # wird sie übersprungen.
            continue

# Beispielaufruf
if __name__ == "__main__":
    csv_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.csv"
    my_lat, my_lon = 48.060711110885094, 8.533784762385885
    max_distance_km = 50.0  # Suchradius in km
    max_stations = 5.0  # Maximalanzahl an Wetterstationen
    current_stations = 0

    # Generator aufrufen
    stations_generator = find_stations_within_radius(csv_url, my_lat, my_lon, max_distance_km)

    # Ergebnisschleife: jede Station wird sofort ausgegeben
    for station_info in stations_generator:
        print(
            f"Station {station_info['station_id']} "
            f"bei ({station_info['latitude']}, {station_info['longitude']}) "
            f"ist {station_info['distance_km']:.2f} km entfernt."
        )
        current_stations += 1
        if current_stations >= max_stations:
            break
