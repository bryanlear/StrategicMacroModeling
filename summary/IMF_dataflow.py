import requests
import time
import json
import os

url = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/'
key = 'Dataflow'  
search_term = 'growth' #### specify the search term here
REQUEST_COUNT = 0
MAX_REQUESTS_BEFORE_PAUSE = 5 
PAUSE_DURATION = 2 

OUTPUT_DIRECTORY = "imf_api_output"
OUTPUT_FILENAME = "imf_explored_data.json"

os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
output_filepath = os.path.join(OUTPUT_DIRECTORY, OUTPUT_FILENAME)

collected_data = {
    "search_term": search_term,
    "matching_dataflows": {}, # Store dataflows by ID
    "codelists": {}          # Store unique codelists by ID
}

print("--- Step 1: Fetching Dataflows ---")
raw_response_step1 = requests.get(f'{url}{key}')
REQUEST_COUNT +=1
print(f"Step 1 - Status Code: {raw_response_step1.status_code}")
print(f"Step 1 - Response Text (first 300 chars): {raw_response_step1.text[:300]}")

response_data_step1 = None
if raw_response_step1.status_code == 200 and 'application/json' in raw_response_step1.headers.get('Content-Type',''):
    try:
        response_data_step1 = raw_response_step1.json()
    except requests.exceptions.JSONDecodeError as e:
        print(f"Step 1 - JSONDecodeError: {e}")
        exit() 
else:
    print("Step 1 - Did not receive a successful JSON response.")
    exit() 

if 'Structure' not in response_data_step1 or 'Dataflows' not in response_data_step1['Structure'] or 'Dataflow' not in response_data_step1['Structure']['Dataflows']:
    print("Error: Unexpected API response structure for Dataflow.")
    exit()

series_list = response_data_step1['Structure']['Dataflows']['Dataflow']

print("\n--- Step 2: Searching for Matching Series ---")
matching_series_info_for_step3 = [] # To pass to Step 3
for series in series_list:
    if isinstance(series, dict) and 'Name' in series and isinstance(series['Name'], dict) and '#text' in series['Name']:
        series_name = series['Name']['#text']
        if search_term.lower() in series_name.lower():
            if 'KeyFamilyRef' in series and isinstance(series['KeyFamilyRef'], dict) and 'KeyFamilyID' in series['KeyFamilyRef']:
                series_id = series['KeyFamilyRef']['KeyFamilyID']
                matching_series_info_for_step3.append((series_name, series_id))
                
                if series_id not in collected_data["matching_dataflows"]:
                    collected_data["matching_dataflows"][series_id] = {
                        "id": series_id,
                        "name": series_name,
                        "dimensions": []
                    }
                print(f"Found series - Name: {series_name}, ID: {series_id}")
            else:
                pass
    else:
        pass

if not matching_series_info_for_step3:
    print(f"No series found containing '{search_term}'.")
    exit()

print("\n--- Step 3: Retrieving Dimensions for Matching Series ---")
dimension_map_for_step4 = {} # To pass to Step 4

for series_name, series_id in matching_series_info_for_step3:
    if REQUEST_COUNT >= MAX_REQUESTS_BEFORE_PAUSE:
        print(f"Rate limit pause: sleeping for {PAUSE_DURATION} seconds...")
        time.sleep(PAUSE_DURATION)
        REQUEST_COUNT = 0
    
    datastructure_url = f'{url}{f"DataStructure/{series_id}"}'
    raw_response_step3 = requests.get(datastructure_url)
    REQUEST_COUNT +=1
    print(f"\nFetching DataStructure/{series_id}")
    print(f"Step 3 - Status Code: {raw_response_step3.status_code}")
    print(f"Step 3 - Response Text (first 300 chars): {raw_response_step3.text[:300]}")
    
    response_json_step3 = None 
    if raw_response_step3.status_code == 200 and 'application/json' in raw_response_step3.headers.get('Content-Type',''):
        try:
            response_json_step3 = raw_response_step3.json()
        except requests.exceptions.JSONDecodeError as e:
            print(f"Step 3 - JSONDecodeError for DataStructure/{series_id}: {e}")
            continue 
    else:
        print(f"Step 3 - Did not receive successful JSON for DataStructure/{series_id}. Status: {raw_response_step3.status_code}, Content-Type: {raw_response_step3.headers.get('Content-Type','')}")
        continue
    
    if response_json_step3 is None:
        continue

    current_dataflow_for_storage = collected_data["matching_dataflows"].get(series_id)
    if not current_dataflow_for_storage:
        continue # Should exist from Step 2

    dimension_map_for_step4[series_id] = []

    if 'Structure' in response_json_step3 and \
       'KeyFamilies' in response_json_step3['Structure'] and \
       response_json_step3['Structure']['KeyFamilies'] and \
       'KeyFamily' in response_json_step3['Structure']['KeyFamilies'] and \
       'Components' in response_json_step3['Structure']['KeyFamilies']['KeyFamily'] and \
       'Dimension' in response_json_step3['Structure']['KeyFamilies']['KeyFamily']['Components']:
        
        dimensions = response_json_step3['Structure']['KeyFamilies']['KeyFamily']['Components']['Dimension']
        print(f"Dimensions for series '{series_name}':")

        if not isinstance(dimensions, list):
            dimensions = [dimensions] 

        for n, dimension in enumerate(dimensions):
            codelist_id = dimension.get('@codelist', 'UNKNOWN')
            concept_ref = dimension.get('@conceptRef', 'UNKNOWN')
            
            dim_info = {"conceptRef": concept_ref, "codelistId": codelist_id}
            if dim_info not in current_dataflow_for_storage["dimensions"]: # to avoid duplicates
                 current_dataflow_for_storage["dimensions"].append(dim_info)
            
            if codelist_id not in dimension_map_for_step4[series_id]: #for Step 4 processing
                dimension_map_for_step4[series_id].append(codelist_id)
            print(f"  Dimension {n+1}: {concept_ref} (Codelist ID: {codelist_id})")
    else:
        print(f"Error: Expected structure not found for DataStructure/{series_id}.")
        continue

print("\n--- Step 4: Fetching Codelists for Dimensions ---")
for series_id in dimension_map_for_step4: #Iteratye using series_id from map
    series_name = collected_data["matching_dataflows"].get(series_id, {}).get("name", series_id)
    print(f"\nCodelists for series '{series_name}' (ID: {series_id}):")

    for codelist_id in dimension_map_for_step4[series_id]:
        if codelist_id == "UNKNOWN":
            print(f"  Skipping unknown codelist in series {series_id}")
            continue
        
        if codelist_id in collected_data["codelists"]: # Check if already fetched
            print(f"  Codelist {codelist_id} already fetched and stored. Skipping.")
            continue

        if REQUEST_COUNT >= MAX_REQUESTS_BEFORE_PAUSE:
            print(f"Rate limit pause: sleeping for {PAUSE_DURATION} seconds...")
            time.sleep(PAUSE_DURATION)
            REQUEST_COUNT = 0

        codelist_url = f'{url}{f"CodeList/{codelist_id}"}'
        raw_response_step4 = requests.get(codelist_url)
        REQUEST_COUNT +=1
        print(f"\nFetching CodeList/{codelist_id}")
        print(f"Step 4 - Status Code: {raw_response_step4.status_code}")
        print(f"Step 4 - Response Text (first 300 chars): {raw_response_step4.text[:300]}")
        
        response_json_step4 = None
        if raw_response_step4.status_code == 200 and 'application/json' in raw_response_step4.headers.get('Content-Type',''):
            try:
                response_json_step4 = raw_response_step4.json()
            except requests.exceptions.JSONDecodeError as e:
                print(f"Step 4 - JSONDecodeError for CodeList/{codelist_id}: {e}")
                continue 
        else:
            print(f"Step 4 - Did not receive successful JSON for CodeList/{codelist_id}. Status: {raw_response_step4.status_code}, Content-Type: {raw_response_step4.headers.get('Content-Type','')}")
            continue

        if response_json_step4 is None:
            continue

        if 'Structure' in response_json_step4 and \
           'CodeLists' in response_json_step4['Structure'] and \
           response_json_step4['Structure']['CodeLists'] and \
           'CodeList' in response_json_step4['Structure']['CodeLists']:
            
            codelist_data_container = response_json_step4['Structure']['CodeLists']['CodeList']
            actual_codelist_data = None
            if isinstance(codelist_data_container, list):
                if codelist_data_container:
                    actual_codelist_data = codelist_data_container[0]
            else: 
                actual_codelist_data = codelist_data_container
            
            if actual_codelist_data and 'Code' in actual_codelist_data:
                codes_from_api = actual_codelist_data['Code']
                if not isinstance(codes_from_api, list): 
                    codes_from_api = [codes_from_api]

                current_codelist_entries = []
                print(f"Codelist ID: {codelist_id}")
                for code_entry in codes_from_api:
                    if isinstance(code_entry, dict) and '@value' in code_entry and \
                       'Description' in code_entry and isinstance(code_entry['Description'], dict) and \
                       '#text' in code_entry['Description']:
                        code_value = code_entry['@value']
                        code_description = code_entry['Description']['#text']
                        current_codelist_entries.append({"value": code_value, "description": code_description})
                        print(f"    Code: {code_value}, Description: {code_description}")
                    else:
                        pass
                collected_data["codelists"][codelist_id] = current_codelist_entries
            else:
                print(f"  'Code' key not found in codelist data for ID: {codelist_id}")
        elif 'Error' in response_json_step4:
             print(f"  API Error for CodeList/{codelist_id}: {response_json_step4['Error']}")
        else:
            print(f"  No valid codelist structure found for ID: {codelist_id}")

try:
    with open(output_filepath, 'w') as f:
        json.dump(collected_data, f, indent=4)
    print(f"\n--- Data exploration complete. Results saved to: {output_filepath} ---")
except IOError as e:
    print(f"\nError saving data to file: {e}")
except Exception as e:
    print(f"\nAn unexpected error occurred during file saving: {e}")