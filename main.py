# https://cloud.google.com/bigquery/docs/reference/libraries

from google.cloud import bigquery
from google.cloud import storage
import argparse
import csv
import os

# Generate log by accessing BQ
def generate_logs(table, date, filename):
    # Build query
    client = bigquery.Client()
    QUERY = (
        "SELECT * "
        "FROM `%s` "
        "WHERE CAST (timestamp as date) = CAST ('%s' as date) "
        "ORDER BY timestamp desc" % (table, date))

    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    # Open and set up csv writer
    f = open(filename, "w", newline="")
    writer = csv.writer(f)

    # Write header
    writer.writerow(["timestamp", "principalEmail", "callerIp", "type",
        "serviceName", "methodName", "authorizationInfo"])

    i = 0  # Row counter

    # Write to csv file
    for row in rows:
        writer.writerow(row)
        i += 1

    print("Generated log with %s rows to %s" % (i, filename))

# Upload log to GCS bucket
def upload_logs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print("File {} uploaded to {}.".format(
        source_file_name,
        destination_blob_name))

# Argument Parser
parser = argparse.ArgumentParser()
parser.add_argument("-d", "--date", required=True, 
    help="Choose date you would like to retrieve logs from BigQuery (YYYY-MM-DD)")
parser.add_argument("-t", "--table", required=True, 
    help="Name of BigQuery database table to query from")
parser.add_argument("-b", "--bucket", required=True, 
    help="Name of bucket to upload logs to")
args = parser.parse_args()

date = args.date # YYYY-MM-DD
bucket = args.bucket
table = args.table

filename = "%s_logging.csv" % date

generate_logs(table, date, filename)
upload_logs(bucket, filename, filename)
