import unittest
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
)

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

if __name__ == '__main__':
    unittest.main()