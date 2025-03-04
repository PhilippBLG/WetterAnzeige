# 1. Verwende ein passendes Basis-Image
FROM python:3.9-slim

WORKDIR /app
COPY . .

# 3. Anforderungen (Dependencies) installieren
#    Falls du ein requirements.txt hast:
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# 5. Port freigeben, auf dem Flask läuft
EXPOSE 8080
ENV PORT 8080

# 6. Flask starten
CMD ["python","-u","app.py"]