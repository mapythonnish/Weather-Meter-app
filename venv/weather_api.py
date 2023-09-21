import os
import re
import json
from flask import Flask, request, jsonify
import requests
import redis
from datetime import datetime, timedelta

app = Flask(__name__)
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Define the base URL for METAR data retrieval
METAR_BASE_URL = "http://tgftp.nws.noaa.gov/data/observations/metar/stations/"

# Function to fetch METAR data for a station code
def fetch_metar_data(station_code):
    url = METAR_BASE_URL + station_code + ".TXT"
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    return None

# Function to parse METAR data and extract required information
def parse_metar_data(metar_text):
    lines = metar_text.split("\n")
    if len(lines) < 2:
        return None

    observation_time = lines[0]
    temperature = None
    wind = None

    for line in lines[1:]:
        if "TEMP" in line:
            continue
        if "KT" in line:
            wind = line.strip()
        else:
            # Attempt to extract temperature in various formats
            temperature_match = re.search(r'(M?\d+/\d+|M?\d+M?\d+)', line)
            if temperature_match:
                temperature = temperature_match.group(0)

    return {
        "observation_time": observation_time,
        "temperature": temperature,
        "wind": wind,
    }


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
                "last_observation": weather_info["observation_time"],
                "temperature": weather_info["temperature"],
                "wind": weather_info["wind"],
            }
            return jsonify({"data": response_data})
        else:
            return jsonify({"error": "Weather information not available for the station."}), 404
    else:
        return jsonify({"error": "Invalid request. Please provide a station code."}), 400

if __name__ == "__main__":
    app.run(host='localhost', port=8080)
