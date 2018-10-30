# https://cloud.google.com/bigquery/docs/reference/libraries
from google.cloud import bigquery
from google.cloud import storage
import argparse
import csv
import logging
import os


# Access BigQuery and generate log
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

    logging.info("Generated BQ log with %s rows to %s" % (i, filename))

# Upload log to GCS bucket
def upload_logs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    logging.info("Uploaded BQ log to gs://%s/%s" %
        (bucket_name, destination_blob_name))

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

# Set "level=logging.DEBUG" if you want more verbose logging output
logging.basicConfig(level=logging.INFO, filename="logfile", filemode="a+",
    format="%(asctime)-15s %(levelname)-8s %(message)s")

# Run it
filename = "%s_logging.csv" % date

try:
    generate_logs(table, date, filename)
except Exception as e:
    logging.error("Generating Logs Failed: %s" % str(e))

try:
    upload_logs(bucket, filename, filename)
except Exception as e:
    logging.error("Uploading Logs Failed: %s" % str(e))

try:
    os.remove(filename) # Delete file after upload
except Exception as e:
    logging.error("Deleting Logs Failed: %s" % str(e))