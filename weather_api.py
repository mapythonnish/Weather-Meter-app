import os
import json
from flask import Flask, request, jsonify
import requests
import redis
from datetime import datetime, timedelta
import re

app = Flask(__name__)
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Define the base URL for METAR data retrieval
METAR_BASE_URL = "http://tgftp.nws.noaa.gov/data/observations/metar/stations/"

# Regular expression pattern to parse METAR data
metar_pattern = r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2}) (\w{4}) (\d{6}Z) (\d{5}KT) (\d{4}) ([A-Z]+)(\d{3}) ([A-Z]+)(\d{3}) (\d{2})/(\d{2}) Q(\d{4}) ([A-Z]+)"

# Function to fetch METAR data for a station code
def fetch_metar_data(station_code):
    url = METAR_BASE_URL + station_code + ".TXT"
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    return None

# Function to parse METAR data and extract required information
def parse_metar_data(metar_text):
    match = re.match(metar_pattern, metar_text)
    if match:
        timestamp = match.group(1)
        station = match.group(2)
        wind_direction = match.group(3)
        wind_speed = match.group(4)
        visibility = match.group(5)
        cloud1_type = match.group(6)
        cloud1_altitude = match.group(7)
        cloud2_type = match.group(8)
        cloud2_altitude = match.group(9)
        temperature = match.group(10)
        dewpoint = match.group(11)
        pressure = match.group(12)
        remarks = match.group(13)

        # Create a JSON object
        metar_json = {
            "timestamp": timestamp,
            "station": station,
            "wind": {
                "direction": wind_direction,
                "speed": wind_speed,
            },
            "visibility": visibility,
            "clouds": [
                {
                    "type": cloud1_type,
                    "altitude": cloud1_altitude,
                },
                {
                    "type": cloud2_type,
                    "altitude": cloud2_altitude,
                },
            ],
            "temperature": temperature,
            "dewpoint": dewpoint,
            "pressure": pressure,
            "remarks": remarks,
        }

        return metar_json
    else:
        return None

# Function to get weather info from cache or fetch fresh data
def get_weather_info(station_code, nocache):
    cached_data = redis_client.get(station_code)

    if nocache or not cached_data:
        metar_data = fetch_metar_data(station_code)
        if metar_data:
            weather_info = parse_metar_data(metar_data)
            if weather_info:
                redis_client.setex(station_code, 300, json.dumps(weather_info))
                return weather_info
    else:
        return json.loads(cached_data)

    return None

@app.route("/metar/ping", methods=["GET"])
def ping():
    return jsonify({"data": "pong"})

@app.route("/metar/info", methods=["GET"])
def get_weather():
    station_code = request.args.get("scode")
    nocache = request.args.get("nocache")
    
    if station_code:
        nocache = bool(int(nocache)) if nocache else False
        weather_info = get_weather_info(station_code, nocache)

        if weather_info:
            response_data = {
                "station": station_code,
                "last_observation": weather_info["timestamp"],
                "weather_data": weather_info,
            }
            return jsonify({"data": response_data})
        else:
            return jsonify({"error": "Weather information not available for the station."}), 404
    else:
        return jsonify({"error": "Invalid request. Please provide a station code."}), 400

if __name__ == "__main__":
    app.run(host='localhost', port=8080)
