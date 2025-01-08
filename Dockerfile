# 1. Verwende ein passendes Basis-Image
FROM python:3.9-slim

# 2. Arbeitsverzeichnis im Container erstellen und setzen
WORKDIR jetbrains://pycharm/navigate/reference?project=WetterAnzeige&path=

# 3. Anforderungen (Dependencies) installieren
#    Falls du ein requirements.txt hast:
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Anschließend deinen Code kopieren
COPY . .

# 5. Port freigeben, auf dem Flask läuft
EXPOSE 5000

# 6. Flask starten
CMD [ "python", "app.py" ]