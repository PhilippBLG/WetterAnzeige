import requests
from bs4 import BeautifulSoup
import gzip

def unzip_gz_file(gz_file_path, output_file_path):
    """
    Entpackt die gz-Datei gz_file_path in die output_file_path.
    """
    with gzip.open(gz_file_path, 'rb') as f_in:
        with open(output_file_path, 'wb') as f_out:
            f_out.write(f_in.read())

url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_station/"
response = requests.get(url)
response.raise_for_status()

soup = BeautifulSoup(response.text, "html.parser")

# Alle <a>-Tags mit 'href' durchgehen und CSV-Links finden
csv_links = []
for link in soup.find_all('a'):
    href = link.get('href')
    if href and href.endswith(".csv.gz"):
        csv_links.append(href)

print("Gefundene CSV-Dateien:", csv_links)

# Jetzt könntest du jede CSV-Datei einzeln herunterladen
for csv_file in csv_links:
    file_url = url + csv_file  # ggf. auf Pfadtrennzeichen achten
    r = requests.get(file_url)
    # r.content oder r.text weiterverarbeiten, lokal speichern etc.