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


def interpolate_data(start_time_local, end_time_local, observations, interval,timezone_str):
    tz = timezone(timezone_str)  # Get the timezone from the store's data
    current_time = tz.localize(datetime.combine(datetime.today(), start_time_local))  # Convert to offset-aware
    interpolated_data = []
    observations = sorted(observations, key=lambda x: parse(x['timestamp_utc']))

    for i in range(len(observations) - 1):
        current_observation = observations[i]
        next_observation = observations[i + 1]

        current_obs_time = parse(current_observation['timestamp_utc']).replace(tzinfo=utc)  # Convert to UTC
        next_obs_time = parse(next_observation['timestamp_utc']).replace(tzinfo=utc)  # Convert to UTC
        time_diff = next_obs_time - current_obs_time

        # Calculate the proportion of uptime and downtime between the current and next observation
        uptime_proportion = 1 if current_observation['status'] == 'active' else 0
        downtime_proportion = 1 if current_observation['status'] == 'inactive' else 0

        if time_diff.total_seconds() > 0:
            uptime_proportion = (uptime_proportion * interval.total_seconds()) / time_diff.total_seconds()
            downtime_proportion = (downtime_proportion * interval.total_seconds()) / time_diff.total_seconds()

        # Extrapolate the uptime and downtime for each hour within the time interval
        while current_time < next_obs_time:
            current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
            status = 'active' if uptime_proportion > 0 else 'inactive'
            interpolated_data.append({'timestamp_utc': current_time_str, 'status': status})
            current_time += interval
            uptime_proportion -= 1
            downtime_proportion -= 1

    # Add the last observation
    last_observation = observations[-1]
    while current_time <= tz.localize(datetime.combine(datetime.today(), end_time_local)):  # Convert to offset-aware
        current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
        status = last_observation['status']
        interpolated_data.append({'timestamp_utc': current_time_str, 'status': status})
        current_time += interval

    return interpolated_data

# ...




# MongoDB Setup

client = MongoClient(app.config['MONGO_URI'])
db = client['monitor']
stores_collection = db['stores']
business_hours_collection = db['business_hours']
timezones_collection = db['timezones']
reports_collection = db['reports'] 

# Store data from CSVs into MongoDB

def load_csv_data_into_db():
    # Load Store data
    with open('store.csv', 'r') as file:
        reader = csv.DictReader(file)
        stores_data = [entry for entry in reader]

    stores_collection.insert_many(stores_data)

    # Load Business Hours data
    with open('business_hours.csv', 'r') as file:
        reader = csv.DictReader(file)
        business_hours_data = [entry for entry in reader]

    business_hours_collection.insert_many(business_hours_data)

    # Load Timezone data
    with open('timezones.csv', 'r') as file:
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
    latest_timestamp_utc = parse(latest_timestamp_str).replace(tzinfo=utc)  # Convert to datetime with timezone info

    # Loop through each store
    for store in stores_data:
        count += 1
        store_id = store['store_id']
        status = store['status']

        # Retrieve business hours for the store
        business_hours = business_hours_collection.find_one({'store_id': store_id})
        if not business_hours:
            # If no business hours found, assume it is open 24*7
            start_time_local = datetime.min.time()
            end_time_local = datetime.max.time()
        else:
            start_time_local = datetime.strptime(business_hours['start_time_local'], '%H:%M:%S').time()
            end_time_local = datetime.strptime(business_hours['end_time_local'], '%H:%M:%S').time()

        # Retrieve timezone for the store
        timezone_data = timezones_collection.find_one({'store_id': store_id})
        timezone_str = timezone_data['timezone_str'] if timezone_data else 'America/Chicago'
        tz = timezone(timezone_str)

        # Convert latest_timestamp from UTC to store's local time
        latest_timestamp_local = latest_timestamp_utc.astimezone(tz)

        # Extrapolate uptime and downtime based on periodic polls
        observations = stores_collection.find({'store_id': store_id})
        interpolated_data = interpolate_data(
            start_time_local,
            end_time_local,
            observations,
            timedelta(hours=1),
            timezone_str
        )

        # Calculate uptime and downtime for the last hour, last day, and last week
        uptime_last_hour = 0
        downtime_last_hour = 0
        uptime_last_day = 0
        downtime_last_day = 0
        uptime_last_week = 0
        downtime_last_week = 0

        for entry in interpolated_data:
            entry_time_utc = parse(entry['timestamp_utc']).replace(tzinfo=utc)  # Convert to datetime with UTC timezone
            entry_time_local = entry_time_utc.astimezone(tz)  # Convert to store's local time

            if entry_time_local >= latest_timestamp_local - timedelta(hours=1):
                if entry['status'] == 'active':
                    uptime_last_hour += 1
                else:
                    downtime_last_hour += 1

            if entry_time_local >= latest_timestamp_local - timedelta(hours=24):
                if entry['status'] == 'active':
                    uptime_last_day += 1
                else:
                    downtime_last_day += 1

            if entry_time_local >= latest_timestamp_local - timedelta(weeks=1):
                if entry['status'] == 'active':
                    uptime_last_week += 1
                else:
                    downtime_last_week += 1

        # Calculate the total business hours in the last day and last week
        total_business_hours_last_day = (end_time_local.hour - start_time_local.hour) + (
                    end_time_local.minute - start_time_local.minute) / 60
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
    report_id = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=23))
    
    
    # Insert report status into the "reports" collection
    report_status = {
        'report_id': report_id,
        'status': 'Running',
        'created_at': get_utc_timestamp()
    }
    reports_collection.insert_one(report_status)
    report = generate_report()
    
    # Generate the CSV file for the report
    with open(f'{app.config["UPLOAD_FOLDER"]}/{report_id}.csv', 'w', newline='') as file:
        fieldnames = report[0].keys()
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report)
    
    # Update the report status to 'Completed' once the report is generated
    reports_collection.update_one({'report_id': report_id}, {'$set': {'status': 'Completed'}})
    
    return jsonify({'report_id': report_id})

@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id') 
    if not report_id:
        return jsonify({'error': 'Missing report_id parameter'}), 400

    # Check the report status in the "reports" collection
    report_status = reports_collection.find_one({'report_id': report_id})
    if not report_status:
        return jsonify({'error': 'Invalid report_id'}), 400

    if report_status['status'] == 'Running':
        return jsonify({'status': 'Running'})
    elif report_status['status'] == 'Completed':
        report_file_path = f'{app.config["UPLOAD_FOLDER"]}/{report_id}.csv'
        if os.path.exists(report_file_path):
            return send_file(report_file_path, as_attachment=True)
        else:
            return jsonify({'error': 'Report file not found'}), 404

@app.route('/load', methods=['POST'])
def load():
    load_csv_data_into_db()
    return jsonify({'result': 'success'}), 200


if __name__ == '__main__':
    # load_csv_data_into_db()
    app.run(debug=True)
