# File: scripts/fetching/fetch_series.py

import requests
import json
import os
import sys
import time
from dotenv import load_dotenv

# --- Configuration ---
PREPARE_DATA_URL = 'https://data.imf.org/platform/rest/v1/query/data'
DOWNLOAD_URL_TEMPLATE = 'https://data.imf.org/api/platform/v1/ott/{}'
OUTPUT_DIRECTORY = "data/raw/imf_live_data"
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

load_dotenv()
IMF_APP_KEY = os.getenv('IMF_PRIMARY_KEY') 

# --- Main Functions ---

def prepare_and_get_token(dataflow_id, series_key):
    """
    Sends the POST request with the industry-standard Authorization header
    to prepare the data and get a download token.
    """
    try:
        freq, area, indicator = series_key.split('.')[:3]
    except ValueError:
        print(f"ERROR: The series key '{series_key}' does not have the expected 3 parts (e.g., Q.US.BCA_BP6_USD).")
        return None

    payload = {
        "query": {
            "where": {
                "and": [
                    {"@DATAFLOW": dataflow_id},
                    {"@FREQ": freq},
                    {"@REF_AREA": area},
                    {"INDICATOR": indicator},
                ]
            }
        },
        "lang": "en"
    }
    
    headers = {
        'Content-Type': 'application/json'
    }
    if IMF_APP_KEY:
        headers['Authorization'] = f'Bearer {IMF_APP_KEY}'
    else:
        print("Warning: IMF_SECONDARY_KEY not found in your environment. Request will likely be forbidden.")

    print("Step 1: Sending POST request to prepare data...")
    print(f" -> Endpoint: {PREPARE_DATA_URL}")
    print(f" -> Payload: {json.dumps(payload, indent=2)}")

    try:
        response = requests.post(PREPARE_DATA_URL, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        response_json = response.json()
        
        token = response_json.get('oneTimeToken')
        if token:
            print(f" -> Success! Received download token: {token}")
            return token
        else:
            print(" -> ERROR: POST request successful, but no download token found.")
            print("Raw Response:", response_json)
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error during data preparation POST request: {e}")
        return None

def download_data_with_token(token, dataflow_id, series_key):
    """
    Uses the token to make a GET request and download the final JSON data.
    """
    download_url = DOWNLOAD_URL_TEMPLATE.format(token)
    
    print("\nStep 2: Sending GET request with token to download data...")
    print(f" -> Endpoint: {download_url}")

    try:
        response = requests.get(download_url, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        filename = f"{dataflow_id}_{series_key.replace('.', '_')}.json"
        output_filepath = os.path.join(OUTPUT_DIRECTORY, filename)
        
        with open(output_filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        
        print(f"--- ✅ Success! Full data package saved to: {output_filepath} ---")

    except requests.exceptions.RequestException as e:
        print(f"Error during data download GET request: {e}")
    except json.JSONDecodeError:
        print("Error: Failed to decode the response as JSON.")
        print("Response Text (first 500 chars):", response.text[:500])


def main():
    """
    Main function to run the two-step fetching process.
    """
    if len(sys.argv) != 3:
        print("\nUsage: python fetch_series.py <DATAFLOW_ID> <SERIES_KEY>")
        print("Example: python fetch_series.py BOP Q.US.BCA_BP6_USD")
        sys.exit(1)

    dataflow_id = sys.argv[1]
    series_key = sys.argv[2]
    
    print(f"--- Starting Two-Step Fetch for {series_key} ---")
    
    one_time_token = prepare_and_get_token(dataflow_id, series_key)
    
    if one_time_token:
        print("\nWaiting 5 seconds for the server to prepare the download file...")
        time.sleep(5)
        download_data_with_token(one_time_token, dataflow_id, series_key)
    else:
        print("\n--- Fetch failed: Could not obtain a download token. ---")


if __name__ == '__main__':
    main()