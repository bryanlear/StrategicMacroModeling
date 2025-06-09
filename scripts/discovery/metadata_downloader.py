#scripts/discovery/metadata_downloader.py

import requests
import time
import json
import os
from dotenv import load_dotenv

API_BASE_URL = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/'
OUTPUT_DIRECTORY = 'scripts/data_collection/IMF_datasets'
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

load_dotenv()
IMF_APP_KEY = os.getenv('IMF_PRIMARY_KEY')

# Rate Limiting
REQUEST_COUNT = 0
MAX_REQUESTS_PER_MINUTE = 10 
TIME_WINDOW_SECONDS = 3
request_timestamps = []

def check_rate_limit():
    """Manages API request rate to avoid hitting limits."""
    global REQUEST_COUNT, request_timestamps
    current_time = time.time()
    
    request_timestamps = [t for t in request_timestamps if current_time - t < TIME_WINDOW_SECONDS]
    
    if len(request_timestamps) >= MAX_REQUESTS_PER_MINUTE:
        sleep_time = TIME_WINDOW_SECONDS - (current_time - request_timestamps[0])
        if sleep_time > 0:
            print(f"Rate limit approaching/hit. Pausing for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
            request_timestamps = [t for t in request_timestamps if time.time() - t < TIME_WINDOW_SECONDS]
    
    request_timestamps.append(time.time())
    REQUEST_COUNT +=1

def make_imf_request(endpoint, params=None):
    """Makes a request to the IMF API, handling the appkey and errors."""
    check_rate_limit()
    
    url = f"{API_BASE_URL}{endpoint}"
    
    request_params = params.copy() if params else {}
    if IMF_APP_KEY:
        request_params['appkey'] = IMF_APP_KEY
        
    print(f"Requesting URL: {url} with params: {request_params}")
    
    try:
        response = requests.get(url, params=request_params, timeout=60)
        print(f"Status Code: {response.status_code}")
        response.raise_for_status()
        
        if 'application/json' in response.headers.get('Content-Type',''):
            return response.json()
        else:
            print(f"Warning: Response not JSON. Content-Type: {response.headers.get('Content-Type','')}")
            print(f"Response text (first 500 chars): {response.text[:500]}")
            return None
            
    except Exception as e:
        print(f"An error occurred during the request: {e}")
        return None

def main():
    """Main function to run the metadata download workflow."""
    print("--- IMF Complete Metadata Downloader ---")
    
    endpoint = None
    default_filename = None
    
    while True:
        choice = input("Download a [1] DataStructure or [2] CodeList? Enter 1 or 2: ").strip()
        if choice == '1':
            dataflow_id = input("Enter the Dataflow ID (e.g., BOP, IFS, IL): ").strip().upper()
            if not dataflow_id:
                print("Dataflow ID cannot be empty.")
                continue
            endpoint = f"DataStructure/{dataflow_id}"
            default_filename = f"datastructure_{dataflow_id}.json"
            break
        elif choice == '2':
            codelist_id = input("Enter the Codelist ID (e.g., CL_AREA_BOP, CL_INDICATOR_IFS): ").strip().upper()
            if not codelist_id:
                print("Codelist ID cannot be empty.")
                continue
            endpoint = f"CodeList/{codelist_id}"
            default_filename = f"codelist_{codelist_id}.json"
            break
        else:
            print("Invalid choice. Please enter 1 or 2.")
            
    user_filename = input(f"Enter filename to save as (default: {default_filename}): ").strip()
 
    final_filename = user_filename if user_filename else default_filename
    
    print(f"\nAttempting to download metadata from endpoint: '{endpoint}'")
    metadata_json = make_imf_request(endpoint)
    
    if metadata_json:
        output_filepath = os.path.join(OUTPUT_DIRECTORY, final_filename)
        try:
            with open(output_filepath, 'w', encoding='utf-8') as f:
                json.dump(metadata_json, f, indent=4)
            print("\n" + "="*50)
            print("  ✅ DOWNLOAD COMPLETE")
            print(f"  Complete metadata saved to: {output_filepath}")
            print("  You can now use this file with the 'discover_series.py' script.")
            print("="*50 + "\n")
        except IOError as e:
            print(f"\nError saving data to file: {e}")
    else:
        print("\n--- Download failed. No data was received from the API. ---")


if __name__ == "__main__":
    main()
