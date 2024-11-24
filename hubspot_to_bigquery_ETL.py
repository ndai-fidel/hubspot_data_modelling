from dotenv import load_dotenv
import os
import requests
from requests.exceptions import RequestException
from google.cloud import bigquery
from datetime import datetime
from dateutil import parser
import time
import json
from multiprocessing import Pool

# Load environment variables from .env file
load_dotenv('ll_hubspot.env')

# Use the private app access token
HUBSPOT_ACCESS_TOKEN = os.getenv('HUBSPOT_PRIVATE_TOKEN')

# BigQuery connection configuration
BIGQUERY_PROJECT_ID = 'rare-guide-433209-e6'
BIGQUERY_DATASET_ID = 'HUBSPOT'

# Path to your service account JSON file
SERVICE_ACCOUNT_JSON = r'G:\My Drive\Lemon Law\rare-guide-433209-e6-7bc861ae2907.json'

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable to point to your service account key
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_JSON

# Maximum length for column names in BigQuery
MAX_COLUMN_NAME_LENGTH = 63

# File to store the last successful data pull state
LAST_PULL_STATE_FILE = 'last_pull_state.json'

BATCH_SIZE = 100  # Batch size for inserting data into BigQuery

def truncate_column_name(column_name):
    return column_name[:MAX_COLUMN_NAME_LENGTH]

def get_last_pull_state():
    try:
        with open(LAST_PULL_STATE_FILE, 'r') as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_last_pull_state(state):
    try:
        with open(LAST_PULL_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=4)
    except Exception as e:
        print(f"Failed to save last pull state: {e}")

def get_hubspot_data(endpoint, retries=3, backoff_factor=1.0, timeout=30):
    url = f'https://api.hubapi.com{endpoint}'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 401:
                raise Exception("Unauthorized access - please check your access token.")
            elif response.status_code == 429:  # Too many requests
                retry_after = int(response.headers.get("Retry-After", 60))
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            if attempt < retries - 1:
                time.sleep(backoff_factor * (2 ** attempt))
            else:
                raise

def get_all_contact_properties():
    url = 'https://api.hubapi.com/properties/v1/contacts/properties'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve properties: {response.status_code} {response.reason}")
    properties = response.json()
    return [prop['name'] for prop in properties]

def create_table_if_not_exists(client, table_name, schema):
    dataset_ref = client.dataset(BIGQUERY_DATASET_ID)
    table_ref = dataset_ref.table(table_name)
    try:
        client.get_table(table_ref)
        print(f"Table {table_name} already exists.")
    except Exception:
        # Create table if it does not exist
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Created table {table_name}.")

def convert_datetime(iso_datetime):
    try:
        return parser.isoparse(iso_datetime)
    except Exception as e:
        print(f"Error converting datetime: {iso_datetime}, Error: {e}")
        return None

def insert_data_into_bigquery(data, table_name):
    # Initialize BigQuery client inside this function to ensure it's available in worker processes
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)

    if data:
        # Define initial columns and their types
        schema = [
            bigquery.SchemaField('id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('createdAt', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('updatedAt', 'TIMESTAMP', mode='NULLABLE')
        ]

        # Extend columns based on the sample record's properties
        sample_record = data[0]['properties']
        for key, value in sample_record.items():
            truncated_key = truncate_column_name(key)
            if len(truncated_key) > MAX_COLUMN_NAME_LENGTH:
                continue
            if isinstance(value, str):
                schema.append(bigquery.SchemaField(truncated_key, 'STRING', mode='NULLABLE'))
            elif isinstance(value, int):
                schema.append(bigquery.SchemaField(truncated_key, 'INT64', mode='NULLABLE'))
            elif isinstance(value, float):
                schema.append(bigquery.SchemaField(truncated_key, 'FLOAT64', mode='NULLABLE'))
            else:
                schema.append(bigquery.SchemaField(truncated_key, 'STRING', mode='NULLABLE'))

        # Create the table if it doesn't exist
        create_table_if_not_exists(client, table_name, schema)

        # Prepare data for insertion
        rows_to_insert = []
        for record in data:
            properties = record['properties']
            row = {
                'id': record['id'],
                'createdAt': convert_datetime(record['createdAt']),
                'updatedAt': convert_datetime(record['updatedAt']),
            }
            for key in schema:
                if key.name in properties:
                    row[key.name] = properties[key.name]
                else:
                    row[key.name] = None  # Fill missing columns with null

            rows_to_insert.append(row)

        # Insert data in batches
        for i in range(0, len(rows_to_insert), BATCH_SIZE):
            batch = rows_to_insert[i:i + BATCH_SIZE]
            errors = client.insert_rows_json(client.dataset(BIGQUERY_DATASET_ID).table(table_name), batch)
            if errors:
                print(f"Errors while inserting rows: {errors}")
            else:
                print(f"Inserted {len(batch)} rows into {table_name}.")

def process_endpoint(args):
    table_name, endpoint = args
    last_pull_state = get_last_pull_state()
    last_pull_info = last_pull_state.get(table_name, {})
    after = last_pull_info.get('after')
    max_timestamp = last_pull_info.get('timestamp')

    if max_timestamp:
        endpoint += f"&since={max_timestamp}"

    has_more = True

    while has_more:
        if after:
            endpoint_with_after = f"{endpoint}&after={after}"
        else:
            endpoint_with_after = endpoint
        print(f"Extracting data from {endpoint_with_after}")
        response = get_hubspot_data(endpoint_with_after)
        data = response.get('results', [])
        if data:
            insert_data_into_bigquery(data, table_name)
            print(f"Inserted {len(data)} records into {table_name} table")
            record_max_timestamp = max(record['updatedAt'] for record in data)
            if not max_timestamp or record_max_timestamp > max_timestamp:
                max_timestamp = record_max_timestamp
            last_pull_state[table_name] = {'after': after, 'timestamp': max_timestamp}
            save_last_pull_state(last_pull_state)
        else:
            print("No data found in response")
        has_more = response.get('paging', {}).get('next', {}).get('after') is not None
        after = response.get('paging', {}).get('next', {}).get('after')

def extract_and_load_data():
    endpoints = {
        'contacts': '/crm/v3/objects/contacts?properties=' + ','.join(get_all_contact_properties())
    }
    with Pool(processes=len(endpoints)) as pool:
        pool.map(process_endpoint, endpoints.items())

if __name__ == "__main__":
    extract_and_load_data()
