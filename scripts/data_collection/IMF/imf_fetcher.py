# File: scripts/imf_fetcher.py

import requests
import json
import os
import sys
import time
from dotenv import load_dotenv

# --- Configuration ---
# Modern SDMX 3.0 API endpoint that prepares the data
QUERY_ENDPOINT = 'https://data.imf.org/platform/rest/v1/query/data'

# Endpoint to download the data using the token
DOWNLOAD_ENDPOINT_TEMPLATE = 'https://data.imf.org/api/platform/v1/ott/{}'

# Save here
OUTPUT_DIRECTORY = "data/raw/imf_live_data"
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

# Load API keys (2 keys are required)
load_dotenv()
IMF_PRIMARY_KEY = os.getenv('IMF_PRIMARY_KEY')
IMF_SECONDARY_KEY = os.getenv('IMF_SECONDARY_KEY')

# --- Main Functions ---

def prepare_and_get_token(payload):
    """
    Sends the POST request with the required headers to prepare the data
    and returns the one-time download token.
    """
    # API requires authentication keys in the headers
    headers = {
        'Content-Type': 'application/json'
    }
    if IMF_PRIMARY_KEY:
        headers['appkey'] = IMF_PRIMARY_KEY
    if IMF_SECONDARY_KEY:
        headers['key'] = IMF_SECONDARY_KEY

    print("Step 1: Sending POST request to prepare data...")
    print(f" -> Endpoint: {QUERY_ENDPOINT}")
    print(f" -> Payload: {json.dumps(payload, indent=4)}")

    try:
        response = requests.post(QUERY_ENDPOINT, headers=headers, json=payload, timeout=90)
        response.raise_for_status()
        response_json = response.json()
        
        token = response_json.get('oneTimeToken')
        if token:
            print(f" -> Success! Received download token: {token}")
            return token
        else:
            print(" -> ERROR: POST request was successful, but no download token was found.")
            print(" -> Raw Response:", response_json)
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error during data preparation POST request: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f" -> Server responded with status {e.response.status_code}")
            print(f" -> Response body: {e.response.text}")
        return None

def download_data_with_token(token, dataflow_id, query_args):
    """
    Uses the token to make a GET request and download the final JSON data.
    """
    download_url = DOWNLOAD_ENDPOINT_TEMPLATE.format(token)
    
    print("\nStep 2: Sending GET request with token to download data...")
    print(f" -> Endpoint: {download_url}")

    try:
        response = requests.get(download_url, timeout=90)
        response.raise_for_status()
        data = response.json()
        
        # Create descriptive filename
        filename_parts = [dataflow_id] + [arg.replace('=', '-') for arg in query_args]
        filename = f"{'_'.join(filename_parts)}.json"
        output_filepath = os.path.join(OUTPUT_DIRECTORY, filename)
        
        with open(output_filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        
        print(f"\n--- ✅ Success! Full data package saved to: {output_filepath} ---")

    except Exception as e:
        print(f"Error during data download GET request: {e}")


def main():
    """
    Main function to build the query payload and run the two-step fetching process.
    """
    if len(sys.argv) < 3:
        print("\nUsage: python imf_fetcher.py <DATAFLOW_ID> \"DIMENSION1=CODE1\" \"DIMENSION2=CODE2\" ...")
        print("Example: python imf_fetcher.py BOP \"REF_AREA=US\" \"INDICATOR=BCA_BP6_USD\" \"FREQ=Q\"")
        sys.exit(1)

    dataflow_id = sys.argv[1]
    query_args = sys.argv[2:]
    
    # Build the 'where' clause for the payload from command line arguments
    and_clause = [{"@DATAFLOW": dataflow_id}]
    for arg in query_args:
        try:
            key, value = arg.split('=')
            # API expects dimension keys to be prefixed with @ for some
            if key.upper() in ['FREQ', 'REF_AREA']:
                key = f"@{key.upper()}"
            else:
                key = key.upper()
            and_clause.append({key: value})
        except ValueError:
            print(f"Invalid argument format: '{arg}'. Please use 'KEY=VALUE'.")
            sys.exit(1)
            
    # The final payload structure for the POST request
    payload = {
        "query": {"where": {"and": and_clause}},
        "lang": "en"
    }

    print(f"--- Starting Two-Step Fetch for Dataflow: {dataflow_id} ---")
    
    one_time_token = prepare_and_get_token(payload)
    
    if one_time_token:
        print("\nWaiting 5 seconds for the server to prepare the download file...")
        time.sleep(5)
        download_data_with_token(one_time_token, dataflow_id, query_args)
    else:
        print("\n--- Fetch failed: Could not obtain a download token. ---")


if __name__ == '__main__':
    main()