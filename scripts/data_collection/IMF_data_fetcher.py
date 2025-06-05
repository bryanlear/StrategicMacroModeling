import requests
import time
import json
import os
import pandas as pd
from dotenv import load_dotenv

# --- Configuration ---
API_BASE_URL = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/'
OUTPUT_DIRECTORY = "data/raw/imf_api_output" 
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

load_dotenv()
IMF_APP_KEY = os.getenv('IMF_PRIMARY_KEY') 

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
    """Makes a request to the IMF API, handling potential appkey and errors."""
    check_rate_limit()
    
    url = f"{API_BASE_URL}{endpoint}"
    
    request_params = params.copy() if params else {} 
    if IMF_APP_KEY:
        request_params['appkey'] = IMF_APP_KEY
        
    print(f"Requesting URL: {url} with params: {request_params}")
    
    try:
        response = requests.get(url, params=request_params, timeout=60) 
        print(f"Status Code: {response.status_code}")
        response.raise_for_status() # Raises an HTTPError
        
        if 'application/json' in response.headers.get('Content-Type',''):
            return response.json()
        else:
            print(f"Warning: Response not JSON. Content-Type: {response.headers.get('Content-Type','')}")
            print(f"Response text (first 500 chars): {response.text[:500]}")
            return None 
            
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content (first 500 chars): {response.text[:500]}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An unexpected error occurred with the request: {req_err}")
    except json.JSONDecodeError as json_err:
        print(f"Error decoding JSON response: {json_err}")
        print(f"Response text (first 500 chars): {response.text[:500]}")
    return None

def store_series_data(df, dataflow_id, series_key_filter, indicator_name="Indicator"):
    """Stores the fetched DataFrame to CSV and Parquet."""
    if df.empty:
        print(f"DataFrame for {indicator_name} ({series_key_filter}) is empty. No data to store.")
        return
    
    # Sanitize series_key_filter for use in filenames
    filename_base = f"{dataflow_id}_{series_key_filter.replace('.', '_').replace(':', '-')}"
    csv_filepath = os.path.join(OUTPUT_DIRECTORY, f"{filename_base}.csv")
    parquet_filepath = os.path.join(OUTPUT_DIRECTORY, f"{filename_base}.parquet")

    output_dir_for_file = os.path.dirname(csv_filepath)
    if not os.path.exists(output_dir_for_file):
        os.makedirs(output_dir_for_file)
        print(f"Created directory: {output_dir_for_file}")

    try:
        df.to_csv(csv_filepath, index=True)
        print(f"Data for '{indicator_name}' successfully saved to CSV: {csv_filepath}")
    except Exception as e:
        print(f"Error saving data to CSV {csv_filepath}: {e}")

    try:
        df.to_parquet(parquet_filepath, index=True, engine='pyarrow')
        print(f"Data for '{indicator_name}' successfully saved to Parquet: {parquet_filepath}")
    except ImportError:
        print(f"Could not save to Parquet for '{indicator_name}': pyarrow library not found. Install it with 'pip install pyarrow'")
    except Exception as e:
        print(f"Error saving data to Parquet {parquet_filepath}: {e}")


def main_direct_key():
    print("--- IMF Direct Series Key Fetcher ---")
    if not IMF_APP_KEY:
        print("Warning: IMF_PRIMARY_KEY (used as appkey) not found in .env file. API requests might be limited or fail if key is required.")

    # Prompt for the Dataflow ID
    dataflow_id = input("Enter Dataflow ID (e.g., IL for International Liquidity, IFS for Intl Financial Stats): ").strip().upper()
    
    # Prompt for the full series key filter (e.g., M.CN.RXF11FX_REVS.USD)
    # This key represents the dimension values in their correct order for the specified dataflow.
    series_key_filter = input(f"Enter the FULL series key filter for Dataflow '{dataflow_id}' (e.g., FREQ.COUNTRY_CODE.INDICATOR_CODE.CURRENCY_CODE): ").strip()
    
    start_year = input("Enter Start Year (YYYY): ").strip()
    end_year = input("Enter End Year (YYYY): ").strip()
    
    # Prompt for a descriptive name for this series (used for filenames and logging)
    indicator_name_prompt = input(f"Enter a descriptive name for the series '{series_key_filter}' (e.g., China Reserves Excl Gold USD Monthly): ").strip()


    print(f"\nAttempting to fetch: Dataflow='{dataflow_id}', SeriesKeyFilter='{series_key_filter}', Period='{start_year}-{end_year}'")

    # Construct the API endpoint for CompactData
    data_endpoint = f"CompactData/{dataflow_id}/{series_key_filter}"
    data_params = {'startPeriod': start_year, 'endPeriod': end_year}
    
    # Make the API request
    series_data_json = make_imf_request(data_endpoint, params=data_params)
    
    if series_data_json and 'CompactData' in series_data_json and \
       series_data_json['CompactData'] and 'Series' in series_data_json['CompactData']: # Check if 'Series' is not None
        
        series_list = series_data_json['CompactData']['Series']
        if not isinstance(series_list, list):
            series_list = [series_list]

        if series_list: 
            series_detail = series_list[0] 
            observations = series_detail.get('Obs', [])
            
            # Handle case where a single observation might be a dict
            if not isinstance(observations, list):
                observations = [observations]
            
            descriptive_name_for_column = indicator_name_prompt if indicator_name_prompt else series_key_filter

            if observations:
                time_periods = [obs.get('@TIME_PERIOD') for obs in observations if obs] # Ensure obs is not None
                values = [obs.get('@OBS_VALUE') for obs in observations if obs]
                
                try:
                    numeric_values = pd.to_numeric(values, errors='coerce')
                    # Create DataFrame
                    ts_df = pd.DataFrame({
                        'TIME_PERIOD': pd.to_datetime(time_periods, errors='coerce'), 
                        descriptive_name_for_column: numeric_values
                    })
                    ts_df.dropna(subset=['TIME_PERIOD'], inplace=True)
                    ts_df.set_index('TIME_PERIOD', inplace=True)
                    ts_df.dropna(subset=[descriptive_name_for_column], inplace=True) 
                    
                    if not ts_df.empty:
                        print(f"\n--- Successfully Fetched Data for: {descriptive_name_for_column} ---")
                        print(ts_df.tail())
                        store_series_data(ts_df, dataflow_id, series_key_filter, descriptive_name_for_column)
                    else:
                        print(f"No valid numeric observations after processing for {descriptive_name_for_column}.")
                except Exception as e_pd:
                    print(f"Error processing series data into DataFrame for {descriptive_name_for_column}: {e_pd}")
            else:
                print(f"No observations ('Obs' array) found for series with key {series_key_filter} in dataflow {dataflow_id}.")
        else:
             print(f"No 'Series' data found for key {series_key_filter} in dataflow {dataflow_id}, even though CompactData was present.")
    else:
        print(f"Could not retrieve or parse data for series key {series_key_filter} in dataflow {dataflow_id}. Check API response above.")

if __name__ == "__main__":
    main_direct_key()
