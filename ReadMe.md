## Approach for Generating the Report

1. **Retrieve Unique Store IDs:** Fetch all unique store IDs from the "stores" collection.

2. **Process Each Store:**
   - For each store ID:
     - Fetch all status logs from the "stores" collection for that specific store ID.
     - Group the status logs based on the date, ignoring the time component.
     - For each date, check if it falls within the working days and working hours defined in the "business_hours" collection. If it does not match the working hours, skip the date.
     - For the remaining dates, interpolate and extrapolate the store's status at hourly intervals between the start and end working hours using the "interpolate_data" function.
     - Calculate the uptime and downtime hours based on the interpolated/extrapolated data for the last hour, last day, and last week using the store's latest timestamp and status.

3. **Generate the Report:**
   - Prepare the report data for each store with store ID, uptime hours for the last hour, last day, and last week, and downtime hours for the last hour, last day, and last week.
   - Collect all store reports in a list.

4. **Save the Report:** Save the generated report data into a CSV file.

5. **Update Report Status:** Update the report status to "Completed" once the report is successfully generated.

The Flask web application provides endpoints to trigger the report generation, retrieve the report file, and load data from CSV files into the MongoDB collections.

## Explanation of the Provided Code

1. The code sets up a Flask web application, configuring the MongoDB connection and upload folder.
2. Utility functions like `get_utc_timestamp` and `convert_to_local_time` are defined to work with datetime and timezone conversions.
3. The `interpolate_data` function performs interpolation and extrapolation of store status based on observations and working hours.
4. MongoDB collections for stores, business hours, timezones, and reports are initialized.
5. The `load_csv_data_into_db` function reads data from CSV files and inserts it into the respective MongoDB collections.
6. The `generate_report` function follows the approach outlined above. It fetches the store data, retrieves business hours and timezones, converts timestamps, and calculates uptime and downtime hours for each store.
7. The Flask API provides three endpoints:
   - `/trigger_report`: Initiates report generation and returns the report ID.
   - `/get_report`: Retrieves the report file by providing the report ID.
   - `/load`: Loads data from CSV files into MongoDB collections.
8. When a report is triggered, it generates the report, saves the data to a CSV file, and updates the report status to "Completed."

Please note that the code's comments are helpful in understanding each section's functionality and logic. The provided code handles the process of generating the report for each store efficiently.
