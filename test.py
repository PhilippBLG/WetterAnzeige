import csv
import logging
from typing import Dict, Iterator
import requests
from geopy.distance import geodesic


# Konfiguration eines Loggers (optional, aber hilfreich)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def find_stations_within_radius(
    csv_url: str,
    lat: float,
    lon: float,
    max_dist_km: float
) -> Iterator[Dict[str, float]]:
    """
    Lädt die CSV-Datei von `csv_url` und gibt jede Station zurück,
    die innerhalb `max_dist_km` Kilometern von (lat, lon) entfernt ist.

    Die Rückgabe erfolgt "direkt" (per Generator), sodass du die Ergebnisse
    zeilenweise verarbeiten kannst.

    Erwartete Spaltenbelegung der CSV:
      - row[0] = StationID (str)
      - row[1] = Breitengrad (Lat, float)
      - row[2] = Längengrad (Lon, float)

    :param csv_url:       URL zu einer CSV-Datei im Web
    :param lat:           Breitengrad (Latitude) des Suchpunkts
    :param lon:           Längengrad (Longitude) des Suchpunkts
    :param max_dist_km:   Suchradius in Kilometern
    :return:              Generator, der Dictionaries mit Station-Infos yieldet
    """

    # CSV aus dem Web laden (Kontextmanager für sauberes Handling)
    with requests.get(csv_url) as response:
        response.raise_for_status()

        # CSV-Inhalt in Zeilen zerlegen
        content = response.text.splitlines()
        reader = csv.reader(content)

        # Jede Zeile im CSV verarbeiten
        for row in reader:
            # Hier kann man z.B. mit if len(row) < 3: continue vorab checken
            try:
                station_id = row[0].strip()
                station_lat = float(row[1])
                station_lon = float(row[2])

                # Entfernung mit geodesic berechnen (in km)
                dist_km = geodesic((lat, lon), (station_lat, station_lon)).kilometers

                # Wenn Distanz <= max_dist_km, direkt yielden
                if dist_km <= max_dist_km:
                    yield {
                        "station_id": station_id,
                        "latitude": station_lat,
                        "longitude": station_lon,
                        "distance_km": dist_km,
                    }

            except (ValueError, IndexError) as e:
                # Loggen, damit du erfährst, wenn etwas schiefläuft
                logger.debug("Zeile kann nicht verarbeitet werden: %s | Error: %s", row, e)
                # Überspringe fehlerhafte Zeilen
                continue


def main() -> None:
    """
    Beispielhafte Hauptfunktion, die das Skript ausführt.
    """
    csv_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.csv"
    my_lat = 48.060711110885094
    my_lon = 8.533784762385885
    max_distance_km = 50.0  # Suchradius in km
    max_stations = 5        # Maximale Anzahl Stationen, die wir ausgeben wollen
    current_stations = 0

    logger.info("Starte Suche nach Stationen innerhalb von %s km ...", max_distance_km)

    # Generator aufrufen
    stations_generator = find_stations_within_radius(csv_url, my_lat, my_lon, max_distance_km)

    # Ergebnisschleife: Jede Station wird sofort ausgegeben
    for station_info in stations_generator:
        print(
            f"Station {station_info['station_id']} "
            f"bei ({station_info['latitude']}, {station_info['longitude']}) "
            f"ist {station_info['distance_km']:.2f} km entfernt."
        )
        current_stations += 1
        if current_stations >= max_stations:
            logger.info("Maximale Anzahl gefundener Stationen erreicht (%d).", max_stations)
            break


if __name__ == "__main__":
    main()