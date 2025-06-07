import requests
import json
import os
import time
import pandas as pd
from dotenv import load_dotenv

API_BASE_URL = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/'
OUTPUT_DIRECTORY = "data/raw/imf_api_output"
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

load_dotenv()
IMF_APP_KEY = os.getenv('IMF_SECONDARY_KEY') 

# Fallback for CL_FREQ codelist
FREQ_CODE_FALLBACK = {
    'A': 'Annual', 'S': 'Semiannual', 'Q': 'Quarterly', 'M': 'Monthly',
    'W': 'Weekly', 'D': 'Daily', 'B': 'Daily, business week'
}

# --- Helper Functions ---

def find_key_recursively(data, target_key):
    if isinstance(data, dict):
        if target_key in data:
            return data[target_key]
        for key, value in data.items():
            result = find_key_recursively(value, target_key)
            if result is not None:
                return result
    elif isinstance(data, list):
        for item in data:
            result = find_key_recursively(item, target_key)
            if result is not None:
                return result
    return None

def make_imf_request(endpoint, params=None):
    url = f"{API_BASE_URL}{endpoint}"
    request_params = params.copy() if params else {}
    if IMF_APP_KEY:
        request_params['appkey'] = IMF_APP_KEY
    
    print(f"Requesting URL: {url} ...")
    try:
        time.sleep(0.5) 
        response = requests.get(url, params=request_params, timeout=60)
        response.raise_for_status()
        if 'application/json' in response.headers.get('Content-Type',''):
            json_data = response.json()
            
            # --- MODIFICATION START ---
            # Create a filename based on the endpoint to save the JSON
            safe_endpoint_name = endpoint.replace('/', '_')
            filename = os.path.join(OUTPUT_DIRECTORY, f"{safe_endpoint_name}.json")
            
            with open(filename, 'w') as f:
                json.dump(json_data, f, indent=4)
            print(f" -> Successfully saved raw JSON response to {filename}")
            # --- MODIFICATION END ---

            return json_data
        else:
            print(f"Warning: Response was not JSON. Content-Type: {response.headers.get('Content-Type','')}")
            return None
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content: {response.text[:500]}")
        return response.json() if 'application/json' in response.headers.get('Content-Type','') else None
    except Exception as err:
        print(f"An unexpected error occurred: {err}")
    return None

def get_dataflow_dimensions(dataflow_id):
    print(f"\nFetching structure for Dataflow ID: {dataflow_id}...")
    structure_endpoint = f"structure/{dataflow_id}" 
    structure_json = make_imf_request(structure_endpoint)

    if not structure_json:
        # Fallback to legacy endpoint if modern one fails
        print(f" -> Modern endpoint '/structure/' failed. Trying legacy '/DataStructure/'...")
        structure_endpoint = f"DataStructure/{dataflow_id}"
        structure_json = make_imf_request(structure_endpoint)
        if not structure_json:
            print("Could not fetch data structure from any known endpoint.")
            return None

    dimensions = find_key_recursively(structure_json, 'Dimension')

    if not dimensions:
        print(f"    -> ERROR: Could not find a 'Dimension' list within the response for {dataflow_id}.")
        return None

    print("\n--- Found Required Dimensions (in order) ---")
    try:
        if not isinstance(dimensions, list):
            dimensions = [dimensions]

        dim_ids = []
        for dim in dimensions:
            dim_id = dim.get('@codelist') or dim.get('@id')
            dim_name = dim.get('@conceptRef', 'No Name Found')
            
            if dim_id:
                dim_ids.append(dim_id)
                print(f" -> Dimension ID: {dim_id:<25} (Concept: {dim_name})")
        
        return dim_ids
    except Exception as e:
        print(f"Error processing the found dimension list: {e}")
        return None

def get_codes_for_dimension(codelist_id):
    print(f"  Fetching codes for dimension: {codelist_id}...")
    codelist_endpoint = f"CodeList/{codelist_id}"
    codelist_json = make_imf_request(codelist_endpoint)
    
    if not codelist_json or 'Error' in codelist_json:
        if codelist_id == 'CL_FREQ':
            print(f"    -> API failed for {codelist_id}. Using hardcoded fallback values.")
            return FREQ_CODE_FALLBACK
        else:
            print(f"    -> API call failed for {codelist_id}. Raw response: {codelist_json}")
            return None

    codes = find_key_recursively(codelist_json, 'Code')

    if not codes:
        print(f"    -> Could not find 'Code' list in any expected format for {codelist_id}.")
        return None

    try:
        if not isinstance(codes, list):
            codes = [codes]
        code_dict = {item.get('@value'): item.get('Description', {}).get('#text') for item in codes if item}
        return code_dict
    except (KeyError, TypeError) as e:
        print(f"    -> Error parsing the final code list for {codelist_id}. Error: {e}")
        return None


def fetch_and_process_series(dataflow_id, series_key):
    start_year = input("Enter Start Year (YYYY): ").strip()
    end_year = input("Enter End Year (YYYY): ").strip()

    print(f"\nAttempting to fetch: Dataflow='{dataflow_id}', SeriesKey='{series_key}', Period='{start_year}-{end_year}'")

    data_endpoint = f"CompactData/{dataflow_id}/{series_key}"
    data_params = {'startPeriod': start_year, 'endPeriod': end_year}
    series_data_json = make_imf_request(data_endpoint, params=data_params)

    if not series_data_json:
        print(f"Could not retrieve a valid response for series '{series_key}'.")
        return

    compact_data = series_data_json.get('CompactData', {})
    dataset = compact_data.get('DataSet', {})
    series_list = dataset.get('Series') 

    if not series_list:
        print(f"\n--- ⚠️ No data returned for the series key: {series_key} ---")
        print("This usually means the series does not exist or has no observations for the selected period.")
        return

    if isinstance(series_list, list):
        if not series_list:
             print("API returned an empty list for 'Series'. No data processed.")
             return
        observations = series_list[0].get('Obs', [])
    else: 
        observations = series_list.get('Obs', [])

    if observations:
        if not isinstance(observations, list):
            observations = [observations]
        
        time_periods = [obs.get('@TIME_PERIOD') for obs in observations]
        values = [obs.get('@OBS_VALUE') for obs in observations]
        
        df = pd.DataFrame({'TIME_PERIOD': time_periods, series_key: values})
        df['TIME_PERIOD'] = pd.to_datetime(df['TIME_PERIOD'])
        df[series_key] = pd.to_numeric(df[series_key], errors='coerce')
        df.set_index('TIME_PERIOD', inplace=True)
        df.dropna(inplace=True)

        if not df.empty:
            print(f"\n--- ✅ Successfully Fetched Data for: {series_key} ---")
            print(df.tail())
        else:
            print(f"-> No valid numeric observations after processing for {series_key}.")
    else:
        print(f"-> The series '{series_key}' was found, but it contains no observations for the requested period.")

def main():
    print("--- IMF Interactive Data Fetcher ---")
    
    dataflow_id = input("Enter the Dataflow ID (e.g., IFS, BOP): ").strip().upper()
    if not dataflow_id: return

    dimension_list = get_dataflow_dimensions(dataflow_id)
    if not dimension_list: return

    all_codes = {}
    print("\n--- Discovering available codes for each dimension ---")
    for dim_id in dimension_list:
        codes = get_codes_for_dimension(dim_id)
        if codes:
            all_codes[dim_id] = codes
            print(f"  -> Successfully discovered {len(codes)} codes for dimension '{dim_id}'.")
        else:
            print(f"  -> Failed to get codes for '{dim_id}'. Cannot proceed.")
            return

    print("\n--- Step 2: Interactively Build a Series Key ---")
    
    key_parts_dict = {}
    for dim_id in dimension_list:
        print(f"\n----- Building Dimension: '{dim_id}' -----")
        
        codes_for_dim = all_codes.get(dim_id, {})
        
        if len(codes_for_dim) > 20:
            print(f"This dimension has {len(codes_for_dim)} possible codes.")
            while True:
                search_action = input("Enter a search term to find a code, 'list' (first 20), or press Enter to skip search: ").strip().lower()
                
                if not search_action: break
                
                if search_action == 'list':
                    print("--- First 20 Available Codes ---")
                    for i, (code, desc) in enumerate(codes_for_dim.items()):
                        print(f"  - {code:<20} ({desc})")
                        if i >= 19: break
                    continue

                matches = {code: desc for code, desc in codes_for_dim.items() if search_action in desc.lower() or search_action in code.lower()}
                
                if matches:
                    print("--- Found Matches ---")
                    for code, desc in matches.items():
                        print(f"  - {code:<20} ({desc})")
                    print("---------------------")
                else:
                    print("No matches found.")
        else:
            print("  Available codes:")
            for code, desc in codes_for_dim.items():
                print(f"    - {code:<15} ({desc})")
        
        part = input(f"Enter the chosen code for '{dim_id}': ").strip().upper()
        key_parts_dict[dim_id] = part

    key_parts = [key_parts_dict.get(dim_id, '') for dim_id in dimension_list]
    series_key = ".".join(key_parts)
    print(f"\nConstructed Series Key: {series_key}")

    fetch_and_process_series(dataflow_id, series_key)
    
    print("\n--- Workflow Complete ---")

if __name__ == "__main__":
    main()