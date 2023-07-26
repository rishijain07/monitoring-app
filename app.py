import csv
import os
import random
import pandas as pd
from flask import Flask, request, jsonify, send_file
from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil.parser import parse
from pytz import timezone, utc

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = './uploads'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/loop_monitor'
app.secret_key = 'secret_key'

# Utility functions

def get_utc_timestamp():
    return datetime.now(utc)

def convert_to_local_time(utc_timestamp, timezone_str):
    tz = timezone(timezone_str)
    local_time = utc_timestamp.astimezone(tz)
    return local_time


def interpolate_data(start_time, end_time, observations, interval):
    current_time = start_time.replace(tzinfo=utc)
    interpolated_data = []
    observations = sorted(observations, key=lambda x: parse(x['timestamp_utc']))

    for i in range(len(observations) - 1):
        current_observation = observations[i]
        next_observation = observations[i + 1]

        current_obs_time = parse(current_observation['timestamp_utc']).replace(tzinfo=utc)
        next_obs_time = parse(next_observation['timestamp_utc']).replace(tzinfo=utc)
        time_diff = next_obs_time - current_obs_time

        # Calculate the proportion of uptime and downtime between the current and next observation
        uptime_proportion = 1 if current_observation['status'] == 'active' else 0
        downtime_proportion = 1 if current_observation['status'] == 'inactive' else 0

        if time_diff.total_seconds() > 0:
            uptime_proportion = (uptime_proportion * interval.total_seconds()) / time_diff.total_seconds()
            downtime_proportion = (downtime_proportion * interval.total_seconds()) / time_diff.total_seconds()

        # Extrapolate the uptime and downtime for the entire time interval
        while current_time < next_obs_time:
            current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
            interpolated_data.append({'timestamp_utc': current_time_str, 'status': current_observation['status']})
            current_time += interval

    # Add the last observation
    last_observation = observations[-1]
    while current_time <= end_time:
        current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
        interpolated_data.append({'timestamp_utc': current_time_str, 'status': last_observation['status']})
        current_time += interval

    return interpolated_data

# ...




# MongoDB Setup

client = MongoClient(app.config['MONGO_URI'])
db = client['loop_monitor']
stores_collection = db['stores']
business_hours_collection = db['business_hours']
timezones_collection = db['timezones']

# Store data from CSVs into MongoDB

def load_csv_data_into_db():
    # Load Store data
    with open('stores_data.csv', 'r') as file:
        reader = csv.DictReader(file)
        stores_data = [entry for entry in reader]

    stores_collection.insert_many(stores_data)

    # Load Business Hours data
    with open('business_hours_data.csv', 'r') as file:
        reader = csv.DictReader(file)
        business_hours_data = [entry for entry in reader]

    business_hours_collection.insert_many(business_hours_data)

    # Load Timezone data
    with open('timzones_data.csv', 'r') as file:
        reader = csv.DictReader(file)
        timezones_data = [entry for entry in reader]

    timezones_collection.insert_many(timezones_data)

# Generate the report based on the provided data

# Generate the report based on the provided data

def generate_report():
    # Retrieve all store data
    stores_data = stores_collection.find()
    records = list(stores_collection.find())
    print(len(records))

    report = []
    count = 0

    # Convert latest_timestamp to a datetime object
    latest_timestamp_entry = stores_collection.find_one(sort=[('timestamp_utc', -1)])
    latest_timestamp_str = latest_timestamp_entry['timestamp_utc']
    latest_timestamp = parse(latest_timestamp_str).replace(tzinfo=utc)  # Convert to datetime with timezone info

    # Loop through each store
    for store in stores_data:
        # print('hi')
        count+=1
        store_id = store['store_id']
        status = store['status']

        # Retrieve business hours for the store
        business_hours = business_hours_collection.find_one({'store_id': store_id})
        if not business_hours:
            # If no business hours found, assume it is open 24*7
            start_time = datetime.min.time()
            end_time = datetime.max.time()
        else:
            # day_of_week = int(business_hours['dayOfWeek'])
            start_time = datetime.strptime(business_hours['start_time_local'], '%H:%M:%S').time()
            end_time = datetime.strptime(business_hours['end_time_local'], '%H:%M:%S').time()

        # Retrieve timezone for the store
        timezone_data = timezones_collection.find_one({'store_id': store_id})
        timezone_str = timezone_data['timezone_str'] if timezone_data else 'America/Chicago'

        # Extrapolate uptime and downtime based on periodic polls
        observations = stores_collection.find({'store_id': store_id})
        interpolated_data = interpolate_data(latest_timestamp - timedelta(weeks=1), latest_timestamp, observations, timedelta(hours=1))

        # Calculate uptime and downtime for the last hour, last day, and last week
        uptime_last_hour = 0
        downtime_last_hour = 0
        uptime_last_day = 0
        downtime_last_day = 0
        uptime_last_week = 0
        downtime_last_week = 0

        for entry in interpolated_data:
            entry_time = parse(entry['timestamp_utc']).replace(tzinfo=utc)  # Convert to datetime with timezone info

            if entry_time >= latest_timestamp - timedelta(hours=1):
                if status == 'active':
                    uptime_last_hour += 1
                else:
                    downtime_last_hour += 1

            if entry_time >= latest_timestamp - timedelta(hours=24):
                if status == 'active':
                    uptime_last_day += 1
                else:
                    downtime_last_day += 1

            if entry_time >= latest_timestamp - timedelta(weeks=1):
                if status == 'active':
                    uptime_last_week += 1
                else:
                    downtime_last_week += 1

        # Calculate the total business hours in the last day and last week
        total_business_hours_last_day = (end_time.hour - start_time.hour) + (end_time.minute - start_time.minute) / 60
        total_business_hours_last_week = total_business_hours_last_day * 7

        # Calculate uptime and downtime in hours
        uptime_last_hour *= 60
        uptime_last_day = (uptime_last_day / len(interpolated_data)) * total_business_hours_last_day
        uptime_last_week = (uptime_last_week / len(interpolated_data)) * total_business_hours_last_week
        downtime_last_hour *= 60
        downtime_last_day = (downtime_last_day / len(interpolated_data)) * total_business_hours_last_day
        downtime_last_week = (downtime_last_week / len(interpolated_data)) * total_business_hours_last_week

        report.append({
            'store_id': store_id,
            'uptime_last_hour': uptime_last_hour,
            'uptime_last_day': uptime_last_day,
            'uptime_last_week': uptime_last_week,
            'downtime_last_hour': downtime_last_hour,
            'downtime_last_day': downtime_last_day,
            'downtime_last_week': downtime_last_week
        })
        if count % 100 == 0:
            print(f"Progress: {count} stores processed")

    return report


# API Endpoints

@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    # Start report generation
    report_id = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=10))
    report = generate_report()
    with open(f'{app.config["UPLOAD_FOLDER"]}/{report_id}.csv', 'w', newline='') as file:
        fieldnames = report[0].keys()
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report)
    return jsonify({'report_id': report_id})

@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id') 
    if not report_id:
        return jsonify({'error': 'Missing report_id parameter'}), 400

    report_file_path = f'{app.config["UPLOAD_FOLDER"]}/{report_id}.csv'
    if os.path.exists(report_file_path):
        return send_file(report_file_path, as_attachment=True)
    else:
        return jsonify({'status': 'Running'})

@app.route('/load', methods=['POST'])
def load():
    load_csv_data_into_db()
    return jsonify({'result': 'success'}), 200


if __name__ == '__main__':
    # load_csv_data_into_db()
    app.run(debug=True)
