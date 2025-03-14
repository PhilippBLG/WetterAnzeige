import unittest
import os
import tempfile
import io
import gzip
import json
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock

from app import (
    app,
    haversine,
    replace_nan_with_none,
    find_stations_within_radius,
    process_station_data,
    get_season,
    read_station_cities,
    read_ghcnd_stations
)


STATION_CITY_COLSPECS = [
    (0, 11),   # ID: columns 1-11
    (12, 20),  # LATITUDE: columns 13-20
    (21, 30),  # LONGITUDE: columns 22-30
    (31, 37),  # ELEVATION: columns 32-37
    (38, 40),  # STATE: columns 39-40
    (41, 71),  # NAME: columns 42-71
    (72, 75),  # GSN FLAG: columns 73-75
    (76, 79),  # HCN/CRN FLAG: columns 77-79
    (80, 85)   # WMO ID: columns 81-85
]
STATION_CITY_NAMES = ["ID", "LATITUDE", "LONGITUDE", "ELEVATION", "STATE", "NAME", "GSN_FLAG", "HCN_CRN_FLAG", "WMO_ID"]

# Konstanten für Fixed-Width-Dateien
GHCND_COLSPECS = [
    (0, 11),   # ID: Spalte 1-11
    (12, 20),  # LATITUDE: Spalte 13-20
    (21, 30),  # LONGITUDE: Spalte 22-30
    (31, 35),  # ELEMENT: Spalte 32-35
    (36, 40),  # FIRSTYEAR: Spalte 37-40
    (41, 45)   # LASTYEAR: Spalte 42-45
]
GHCND_NAMES = ['ID', 'LATITUDE', 'LONGITUDE', 'ELEMENT', 'FIRSTYEAR', 'LASTYEAR']

###############################################################################
# Utility Functions Tests
###############################################################################
class TestUtilityFunctions(unittest.TestCase):
    def test_haversine(self):
        # When both points are the same, the distance should be 0.
        self.assertAlmostEqual(haversine(0, 0, 0, 0), 0)

        # Test with known values.
        # Example: New York (40.7128, -74.0060) and London (51.5074, -0.1278)
        distance = haversine(40.7128, -74.0060, 51.5074, -0.1278)
        # Approximate distance is around 5570 km.
        self.assertGreater(distance, 5500)
        self.assertLess(distance, 6000)

    def test_replace_nan_with_none(self):
        test_obj = {
            'a': np.nan,
            'b': [1, 2, np.nan, {'c': np.nan}],
            'd': 'value'
        }
        expected = {
            'a': None,
            'b': [1, 2, None, {'c': None}],
            'd': 'value'
        }
        self.assertEqual(replace_nan_with_none(test_obj), expected)

    def test_get_season(self):
        # Northern Hemisphere (station_lat >= 0)
        self.assertEqual(get_season(3, 45), 'Spring')
        self.assertEqual(get_season(4, 45), 'Spring')
        self.assertEqual(get_season(5, 45), 'Spring')
        self.assertEqual(get_season(6, 45), 'Summer')
        self.assertEqual(get_season(7, 45), 'Summer')
        self.assertEqual(get_season(8, 45), 'Summer')
        self.assertEqual(get_season(9, 45), 'Autumn')
        self.assertEqual(get_season(10, 45), 'Autumn')
        self.assertEqual(get_season(11, 45), 'Autumn')
        self.assertEqual(get_season(1, 45), 'Winter')
        self.assertEqual(get_season(2, 45), 'Winter')
        self.assertEqual(get_season(12, 45), 'Winter')

        # Southern Hemisphere (station_lat < 0)
        self.assertEqual(get_season(3, -45), 'Autumn')
        self.assertEqual(get_season(4, -45), 'Autumn')
        self.assertEqual(get_season(5, -45), 'Autumn')
        self.assertEqual(get_season(6, -45), 'Winter')
        self.assertEqual(get_season(7, -45), 'Winter')
        self.assertEqual(get_season(8, -45), 'Winter')
        self.assertEqual(get_season(9, -45), 'Spring')
        self.assertEqual(get_season(10, -45), 'Spring')
        self.assertEqual(get_season(11, -45), 'Spring')
        self.assertEqual(get_season(1, -45), 'Summer')
        self.assertEqual(get_season(2, -45), 'Summer')
        self.assertEqual(get_season(12, -45), 'Summer')

    def test_read_station_cities(self):
        # Create a sample fixed-width file content.
        # We'll create two rows. Each field is padded to the required width.
        #
        # Field widths (based on STATION_CITY_COLSPECS):
        # ID: 11, LATITUDE: 8, LONGITUDE: 9, ELEVATION: 6, STATE: 2,
        # NAME: 30, GSN_FLAG: 3, HCN_CRN_FLAG: 3, WMO_ID: 5
        #
        # We'll separate fields with a single space between fields.
        row1 = (
            f"{'GME00102404':<11}" +
            f" {'48.1234':<8}" +
            f" {'8.1234':<9}" +
            f" {'100':<6}" +
            f" {'XX':<2}" +
            f" {'Test City':<30}" +
            f" {'_':<3}" +
            f" {'_':<3}" +
            f" {'_':<5}"
        )
        row2 = (
            f"{'GME00102405':<11}" +
            f" {'49.5678':<8}" +
            f" {'9.5678':<9}" +
            f" {'200':<6}" +
            f" {'YY':<2}" +
            f" {'Another City':<30}" +
            f" {'_':<3}" +
            f" {'_':<3}" +
            f" {'_':<5}"
        )
        sample_content = row1 + "\n" + row2 + "\n"

        # Create a temporary file containing our sample data.
        with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
            tmp.write(sample_content)
            tmp_path = tmp.name

        try:
            # Call the function with the path to our temporary file.
            mapping = read_station_cities(tmp_path)
            # Expected mapping: IDs mapped to the station NAME (trimmed).
            expected = {
                "GME00102404": "Test City",
                "GME00102405": "Another City"
            }
            self.assertEqual(mapping, expected)
        finally:
            os.unlink(tmp_path)

    def test_read_ghcnd_stations(self):
        # Create sample fixed-width data:
        # We simulate three rows:
        # Row 1: A TMIN record for STATION001.
        # Row 2: A TMAX record for STATION001 (duplicate ID; should be dropped).
        # Row 3: A PRCP record for STATION002 (should be filtered out).
        #
        # Each field is padded to the width specified in TEST_GHCND_COLSPECS.
        row1 = (
            f"{'STATION001':<13}" +  # ID (11 chars)
            f"{'48.1234':<9}" +      # LATITUDE (8 chars)
            f"{' 8.12345':<9}" +      # LONGITUDE (9 chars)
            f"{'TMIN':<5}" +         # ELEMENT (4 chars)
            f"{'2000':<5}" +         # FIRSTYEAR (4 chars)
            f"{'2020':<5}"           # LASTYEAR (4 chars)
        )
        row2 = (
            f"{'STATION001':<13}" +
            f"{'48.1234':<9}" +
            f"{' 8.12345':<9}" +
            f"{'TMAX':<5}" +
            f"{'2000':<5}" +
            f"{'2020':<5}"
        )
        row3 = (
            f"{'STATION002':<13}" +
            f"{'49.0000':<9}" +
            f"{' 9.00000':<9}" +
            f"{'PRCP':<5}" +
            f"{'2001':<5}" +
            f"{'2021':<5}"
        )
        sample_data = row1 + "\n" + row2 + "\n" + row3 + "\n"
        print(sample_data)

        # Write sample_data to a temporary file.
        with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
            tmp.write(sample_data)
            tmp_path = tmp.name

        try:
            df = read_ghcnd_stations(tmp_path)
            # Since row3 should be filtered out (ELEMENT not in ['TMIN','TMAX'])
            # and row2 is a duplicate of row1, we expect exactly 1 unique station.
            self.assertEqual(len(df), 1)
            row = df.iloc[0]
            self.assertEqual(row["ID"].strip(), "STATION001")
            # Verify that numeric columns are converted correctly.
            self.assertAlmostEqual(row["LATITUDE"], 48.1234)
            self.assertAlmostEqual(row["LONGITUDE"], 8.12345)
            # Check that the ELEMENT is either 'TMIN' or 'TMAX'
            self.assertIn(row["ELEMENT"], ['TMIN', 'TMAX'])
        finally:
            os.unlink(tmp_path)

###############################################################################
# Testing find_stations_within_radius
###############################################################################
class TestFindStationsWithinRadius(unittest.TestCase):
    @patch('app.read_ghcnd_stations')
    def test_find_stations_within_radius(self, mock_read):
        # Create a sample DataFrame that read_ghcnd_stations should return.
        df = pd.DataFrame({
            'ID': ['ST001', 'ST002', 'ST003'],
            'LATITUDE': [48.06, 48.07, 49.0],
            'LONGITUDE': [8.53, 8.54, 8.0],
            'ELEMENT': ['TMAX', 'TMIN', 'TMAX'],
            'FIRSTYEAR': [2000, 2000, 2000],
            'LASTYEAR': [2020, 2020, 2020]
        })
        mock_read.return_value = df

        # Call the function with parameters that pass the time filter.
        stations = find_stations_within_radius(
            inventory_url="dummy_url",
            lat=48.06,
            lon=8.53,
            max_dist_km=100,  # Adjust max distance to include our dummy stations.
            max_stations=5,
            firstyear=2000,
            lastyear=2020
        )

        # Ensure that each returned station dict has the expected keys.
        self.assertIsInstance(stations, list)
        for station in stations:
            self.assertIn('station_id', station)
            self.assertIn('latitude', station)
            self.assertIn('longitude', station)
            self.assertIn('distance_km', station)
            self.assertIn('firstyear', station)
            self.assertIn('lastyear', station)

###############################################################################
# Testing process_station_data
###############################################################################
class TestProcessStationData(unittest.TestCase):
    @patch('app.requests.get')
    def test_process_station_data(self, mock_get):
        # Prepare sample CSV content.
        # Note: Ensure 8 comma-separated columns per row as expected by pd.read_csv.
        csv_content = (
            "ST001,20210101,TMAX,250,,,,\n"
            "ST001,20210101,TMIN,50,,,,\n"
            "ST001,20210102,TMAX,260,,,,\n"
            "ST001,20210102,TMIN,60,,,,\n"
        )

        # Compress the CSV content with gzip.
        gzipped_csv = io.BytesIO()
        with gzip.GzipFile(fileobj=gzipped_csv, mode='w') as f:
            f.write(csv_content.encode('utf-8'))
        gzipped_csv.seek(0)

        # Create a mock response with a .raw attribute returning our BytesIO stream.
        mock_response = MagicMock()
        mock_response.raw = gzipped_csv
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        # Process station data within a Flask app context.
        with app.app_context():
            response, status = process_station_data("ST001", 2021, 2021, 45.0)
            self.assertEqual(status, 200)

            # Convert the Flask Response to a Python dict.
            data = json.loads(response.get_data(as_text=True))
            self.assertIn('yearly_summary', data)
            self.assertIn('seasonal_summary', data)

###############################################################################
# Testing Flask Endpoints
###############################################################################
class TestFlaskRoutes(unittest.TestCase):
    def setUp(self):
        # Create a test client for the Flask application.
        self.app = app.test_client()
        self.app.testing = True

    @patch('app.process_station_data')
    def test_get_station_weather_data_missing_id(self, mock_process):
        # Test the API without providing station_id.
        response = self.app.get('/api/station_data')
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.get_data(as_text=True))
        self.assertIn('error', data)

    @patch('app.process_station_data')
    def test_get_station_weather_data_valid(self, mock_process):
        # Set up a dummy response for process_station_data.
        dummy_response = app.response_class(
            response=json.dumps({"dummy": "data"}),
            status=200,
            mimetype='application/json'
        )
        mock_process.return_value = (dummy_response, 200)

        # Test a valid API call.
        response = self.app.get(
            '/api/station_data?station_id=ST001&firstyear=2021&lastyear=2021&station_lat=45'
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.get_data(as_text=True))
        self.assertEqual(data, {"dummy": "data"})

    @patch('app.find_stations_within_radius')
    @patch('app.read_station_cities')
    def test_find_stations_endpoint(self, mock_read_cities, mock_find_stations):
        # Set up dummy stations and a station-to-city mapping.
        dummy_stations = [
            {
                "station_id": "ST001",
                "latitude": 48.06,
                "longitude": 8.53,
                "distance_km": 10,
                "firstyear": 2010,
                "lastyear": 2015
            }
        ]
        dummy_cities = {"ST001": "Test City"}
        mock_find_stations.return_value = dummy_stations
        mock_read_cities.return_value = dummy_cities

        response = self.app.get(
            '/api/find_stations?lat=48.06&lon=8.53&max_dist_km=50&max_stations=5&firstyear=2010&lastyear=2015'
        )
        self.assertEqual(response.status_code, 200)
        # Since the endpoint returns an event-stream, check the content type.
        self.assertEqual(response.content_type, 'text/event-stream')
        response_data = response.get_data(as_text=True)
        self.assertIn("Test City", response_data)

    def test_index(self):
        # Patch render_template so that we don't require a real template file.
        with patch('app.render_template', return_value="Index Page"):
            response = self.app.get('/')
            self.assertEqual(response.status_code, 200)
            self.assertIn("Index Page", response.get_data(as_text=True))

###############################################################################
# Testing Integration
###############################################################################
class IntegrationTests(unittest.TestCase):
    def setUp(self):
        # Create a test client using the Flask application configured for testing.
        self.client = app.test_client()
        self.client.testing = True

    def create_gzipped_csv(self, csv_data: str) -> io.BytesIO:
        """
        Helper function that creates a gzip-compressed in-memory CSV.
        """
        gzipped_csv = io.BytesIO()
        with gzip.GzipFile(fileobj=gzipped_csv, mode='w') as gz:
            gz.write(csv_data.encode('utf-8'))
        gzipped_csv.seek(0)
        return gzipped_csv

    @patch('app.requests.get')
    def test_api_station_data_integration(self, mock_get):
        """
        Integration test for the /api/station_data endpoint.

        This test patches the requests.get call to simulate fetching a gzip-compressed CSV file,
        then verifies that the endpoint returns JSON containing the expected keys.
        """
        csv_data = (
            "TEST,20210101,TMAX,250,,,,\n"
            "TEST,20210101,TMIN,50,,,,\n"
            "TEST,20210102,TMAX,260,,,,\n"
            "TEST,20210102,TMIN,60,,,,\n"
        )
        gzipped_csv = self.create_gzipped_csv(csv_data)

        fake_response = MagicMock()
        fake_response.raw = gzipped_csv
        fake_response.raise_for_status = MagicMock()
        mock_get.return_value = fake_response

        response = self.client.get(
            '/api/station_data?station_id=TEST&firstyear=2021&lastyear=2021&station_lat=45'
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertIn("yearly_summary", data)
        self.assertIn("seasonal_summary", data)

    @patch('app.requests.get')
    def test_api_station_data_external_error(self, mock_get):
        """
        Simulate an external error (e.g. network failure) during the call to requests.get.
        The endpoint should catch the exception and return an error with status code 500.
        """
        mock_get.side_effect = Exception("Simulated external failure")
        response = self.client.get(
            '/api/station_data?station_id=TEST&firstyear=2021&lastyear=2021&station_lat=45'
        )
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        self.assertIn("error", data)
        self.assertIn("Simulated external failure", data["error"])

    @patch('app.find_stations_within_radius')
    def test_api_find_stations_error(self, mock_find_stations):
        """
        Simulate an error in the station search logic.
        The /api/find_stations endpoint should catch the exception and return an error with status code 500.
        """
        mock_find_stations.side_effect = Exception("Simulated station search failure")
        response = self.client.get(
            '/api/find_stations?lat=48&lon=8&max_dist_km=10&max_stations=5&firstyear=2000&lastyear=2020'
        )
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        self.assertIn("error", data)
        self.assertIn("Simulated station search failure", data["error"])

if __name__ == '__main__':
    unittest.main()