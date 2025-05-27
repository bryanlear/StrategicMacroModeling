import requests
import time
import json
import os

url = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/'
key = 'Dataflow'  

# --- Configuration for multiple terms and countries ---
SEARCH_TERMS = ['growth', 'inflation', 'debt', 'gold', 'oil', 'unemployment', 'trade', 'balance of payments', 'foreign exchange reserves', 'GDP per capita', 'current account balance', 'fiscal policy', 'monetary policy', 'exchange rate policy', 'financial stability'] 
TARGET_COUNTRIES = ['United States', 'Germany','France','Netherlands', 'Japan', 'China', 'United Kingdom']
COUNTRY_DIMENSION_CONCEPT_REFS = ["REF_AREA", "AREA", "GEO_AREA", "LOCATION", "COUNTRY", "COU", "GEOGRAPHY","CNT", "SP_REG", "REGION"]
COUNTRY_CODELIST_ID_KEYWORDS = ["AREA", "COUNTRY", "GEO", "NATIONS", "REGIONS"]

REQUEST_COUNT = 0
MAX_REQUESTS_BEFORE_PAUSE = 5 
PAUSE_DURATION = 2 

OUTPUT_DIRECTORY = "imf_api_output_filtered"
OUTPUT_FILENAME = "imf_filtered_explored_data.json"

os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
output_filepath = os.path.join(OUTPUT_DIRECTORY, OUTPUT_FILENAME)

intermediate_collected_data = {"dataflows_matching_search_terms": {}, "all_fetched_codelists": {}}  # Store all codelists fetched by ID

print("--- Step 1: Fetching Dataflows ---")
raw_response_step1 = requests.get(f'{url}{key}')
REQUEST_COUNT +=1
print(f"Step 1 - Status Code: {raw_response_step1.status_code}")
#print(f"Step 1 - Response Text (first 300 chars): {raw_response_step1.text[:300]}")

response_data_step1 = None
if raw_response_step1.status_code == 200 and 'application/json' in raw_response_step1.headers.get('Content-Type',''):
    try:
        response_data_step1 = raw_response_step1.json()
    except requests.exceptions.JSONDecodeError as e:
        print(f"Step 1 - JSONDecodeError: {e}")
        exit() 
else:
    print(f"Step 1 - Did not receive a successful JSON response. Status: {raw_response_step1.status_code}, Content-Type: {raw_response_step1.headers.get('Content-Type','')}")
    exit()

if 'Structure' not in response_data_step1 or 'Dataflows' not in response_data_step1['Structure'] or 'Dataflow' not in response_data_step1['Structure']['Dataflows']:
    print("Error: Unexpected API response structure for Dataflow.")
    exit()

series_list = response_data_step1['Structure']['Dataflows']['Dataflow']


# --- Collecting data for matching series and codelists ---
print("\n--- Step 2: Searching for Matching Series ---")
matching_series_info_for_step3 = [] 
for series in series_list:
    if isinstance(series, dict) and 'Name' in series and isinstance(series['Name'], dict) and '#text' in series['Name']:
        series_name = series['Name']['#text']
        series_matched_any_term = False
        matched_terms = [] 

        for term in SEARCH_TERMS:
            if term.lower() in series_name.lower():
                series_matched_any_term = True
                matched_terms.append(term)

        if series_matched_any_term:
            if 'KeyFamilyRef' in series and isinstance(series['KeyFamilyRef'], dict) and 'KeyFamilyID' in series['KeyFamilyRef']:
                series_id = series['KeyFamilyRef']['KeyFamilyID']
                matching_series_info_for_step3.append((series_name, series_id))

                if series_id not in intermediate_collected_data["dataflows_matching_search_terms"]:
                    intermediate_collected_data["dataflows_matching_search_terms"][series_id] = {
                        "id": series_id,
                        "name": series_name,
                        "matched_search_terms": list(set(matched_terms)), 
                        "dimensions": []
                    }
                else: 
                    existing_terms = intermediate_collected_data["dataflows_matching_search_terms"][series_id].get("matched_search_terms", [])
                    intermediate_collected_data["dataflows_matching_search_terms"][series_id]["matched_search_terms"] = list(set(existing_terms + matched_terms))

                print(f"Found series - Name: {series_name}, ID: {series_id} (Matches: {', '.join(matched_terms)})")
            else:
                pass 
    else:
        pass 

if not matching_series_info_for_step3:
    print(f"No series found containing any of the search terms, try another you dumb fuck: {', '.join(SEARCH_TERMS)}.")
    exit()

# --- Retrieving Dimensions ---

print("\n--- Step 3: Retrieving Dimensions for Matching Series ---")
dimension_map_for_step4 = {}

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
    #print(f"Step 3 - Response Text (first 300 chars): {raw_response_step3.text[:300]}")
    
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

    current_dataflow_for_storage = intermediate_collected_data["dataflows_matching_search_terms"].get(series_id)
    if not current_dataflow_for_storage: # Should exist from Step 2
        print(f"Warning: series_id {series_id} not found in intermediate storage. Skipping.")
        continue

    dimension_map_for_step4[series_id] = []

    if 'Structure' in response_json_step3 and \
       'KeyFamilies' in response_json_step3['Structure'] and \
       response_json_step3['Structure']['KeyFamilies'] and \
       'KeyFamily' in response_json_step3['Structure']['KeyFamilies'] and \
        isinstance(response_json_step3['Structure']['KeyFamilies']['KeyFamily'], dict) and \
       'Components' in response_json_step3['Structure']['KeyFamilies']['KeyFamily'] and \
       'Dimension' in response_json_step3['Structure']['KeyFamilies']['KeyFamily']['Components']:
        
        dimensions = response_json_step3['Structure']['KeyFamilies']['KeyFamily']['Components']['Dimension']
        print(f"Dimensions for series '{series_name}':")

        if not isinstance(dimensions, list):
            dimensions = [dimensions] 

        for n, dimension in enumerate(dimensions):
            codelist_id = dimension.get('@codelist', 'UNKNOWN_CODELIST')
            concept_ref = dimension.get('@conceptRef', 'UNKNOWN_CONCEPT')
            
            dim_info = {"conceptRef": concept_ref, "codelistId": codelist_id}
            if dim_info not in current_dataflow_for_storage["dimensions"]: #avoid duplicates
                 current_dataflow_for_storage["dimensions"].append(dim_info)
            
            if codelist_id != 'UNKNOWN_CODELIST' and codelist_id not in dimension_map_for_step4[series_id]: # to 4 
                dimension_map_for_step4[series_id].append(codelist_id)
            print(f"  Dimension {n+1}: {concept_ref} (Codelist ID: {codelist_id})")
    else:
        print(f"Error: Expected structure not found or KeyFamily is not a dict for DataStructure/{series_id}.")
        # print(f"DEBUG: response_json_step3['Structure']['KeyFamilies']['KeyFamily'] type is {type(response_json_step3['Structure']['KeyFamilies']['KeyFamily'])}")
        continue

# --- Collecting Codelists for Dimensions ---
print("\n--- Step 4: Fetching Codelists for Dimensions ---")
for series_id_for_codelists in dimension_map_for_step4:
    series_name = intermediate_collected_data["dataflows_matching_search_terms"].get(series_id_for_codelists, {}).get("name", series_id_for_codelists)
    print(f"\nProcessing Codelists for series '{series_name}' (ID: {series_id_for_codelists}):")

    
    for codelist_id in dimension_map_for_step4[series_id_for_codelists]:
        if codelist_id == "UNKNOWN_CODELIST":
            print(f"  Skipping unknown codelist in series {series_id_for_codelists}")
            continue

        if codelist_id in intermediate_collected_data["all_fetched_codelists"]:  # Check if already fetched
            print(f"  Codelist {codelist_id} already fetched and stored. Skipping.")
            continue

        if REQUEST_COUNT >= MAX_REQUESTS_BEFORE_PAUSE:
            print(f"Rate limit pause: sleeping for {PAUSE_DURATION} seconds...")
            time.sleep(PAUSE_DURATION)
            REQUEST_COUNT = 0

        codelist_url = f'{url}{f"CodeList/{codelist_id}"}'
        raw_response_step4 = requests.get(codelist_url)
        REQUEST_COUNT += 1
        print(f"\nFetching CodeList/{codelist_id}")
        print(f"Step 4 - Status Code: {raw_response_step4.status_code}")
        # print(f"Step 4 - Response Text (first 300 chars): {raw_response_step4.text[:300]}")

        response_json_step4 = None
        if raw_response_step4.status_code == 200 and 'application/json' in raw_response_step4.headers.get('Content-Type', ''):
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
            # sometimes code list sometimees dict
            if isinstance(codelist_data_container, list):
                if codelist_data_container: # Ensure list =! empty
                    actual_codelist_data = codelist_data_container[0] 
                else:
                    print(f"  CodeList for ID {codelist_id} is an empty list.")
                    continue
            else: # It's a dict
                actual_codelist_data = codelist_data_container

            if actual_codelist_data and 'Code' in actual_codelist_data:
                codes_from_api = actual_codelist_data['Code']
                if not isinstance(codes_from_api, list):
                    codes_from_api = [codes_from_api]

                current_codelist_entries = []
                print(f"Codes for Codelist ID: {codelist_id}")
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
                intermediate_collected_data["all_fetched_codelists"][codelist_id] = current_codelist_entries
            else:
                print(f"  'Code' key not found or actual_codelist_data is None in codelist data for ID: {codelist_id}")
        elif 'Error' in response_json_step4: # Check API errors
             print(f"  API Error for CodeList/{codelist_id}: {response_json_step4.get('Error','Unknown Error Structure')}")
        else:
            print(f"  No valid codelist structure found for ID: {codelist_id}")


# --- Filtering by Country and Finalizing Data ---

print("\n--- Step 5: Filtering Dataflows by Target Countries and Finalizing Output ---")

final_output_data = {
    "search_parameters": {
        "search_terms": SEARCH_TERMS,
        "target_countries": TARGET_COUNTRIES
    },
    "relevant_dataflows": {},
    "referenced_codelists": {}
}

for df_id, df_details in intermediate_collected_data["dataflows_matching_search_terms"].items():
    is_country_relevant_for_this_df = False
    country_dimension_found = False

    for dimension in df_details.get("dimensions", []):
        dim_concept_ref = dimension.get("conceptRef", "").upper()
        dim_codelist_id = dimension.get("codelistId", "")

        # Check if this dimension is likely a country/area dimension
        is_potential_country_dim = False
        if dim_concept_ref in COUNTRY_DIMENSION_CONCEPT_REFS:
            is_potential_country_dim = True
        else:
            for keyword in COUNTRY_CODELIST_ID_KEYWORDS:
                if keyword.lower() in dim_codelist_id.lower():
                    is_potential_country_dim = True
                    break
        
        if is_potential_country_dim:
            country_dimension_found = True #at leat one country dimension found
            # print(f"Debug: Dataflow {df_id}, Potential country dimension: {dim_concept_ref} ({dim_codelist_id})")
            codelist_for_dim = intermediate_collected_data["all_fetched_codelists"].get(dim_codelist_id, [])
            for code_entry in codelist_for_dim:
                code_desc = code_entry.get("description", "").lower()
                for target_country in TARGET_COUNTRIES:
                    if target_country.lower() in code_desc:
                        is_country_relevant_for_this_df = True
                        print(f"Dataflow '{df_details['name']}' (ID: {df_id}) IS relevant for country: {target_country} (found in codelist {dim_codelist_id})")
                        break # Found a target country in this codelist
                if is_country_relevant_for_this_df:
                    break # No need to check other codes in this codelist
        if is_country_relevant_for_this_df:
            break #dont check other dimensions if we already found a relevant country

    if not country_dimension_found and df_details.get("dimensions"):
        print(f"Dataflow '{df_details['name']}' (ID: {df_id}) - No clear country/area dimension found among its dimensions. It will not be included unless a country match was already made (which shouldn't happen without a country dim).")


    if is_country_relevant_for_this_df:
        final_output_data["relevant_dataflows"][df_id] = df_details
        for dimension in df_details.get("dimensions", []):
            codelist_id_to_add = dimension.get("codelistId")
            if codelist_id_to_add and codelist_id_to_add != "UNKNOWN_CODELIST":
                if codelist_id_to_add in intermediate_collected_data["all_fetched_codelists"] and \
                   codelist_id_to_add not in final_output_data["referenced_codelists"]:
                    final_output_data["referenced_codelists"][codelist_id_to_add] = \
                        intermediate_collected_data["all_fetched_codelists"][codelist_id_to_add]

# --- Saving final filtered data ---
try:
    with open(output_filepath, 'w') as f:
        json.dump(final_output_data, f, indent=4)
    if not final_output_data["relevant_dataflows"]:
        print(f"\n--- No dataflows matched both the search terms AND target countries. Output file created but 'relevant_dataflows' is empty: {output_filepath} ---")
    else:
        print(f"\n--- Data exploration and filtering complete. Results saved to: {output_filepath} ---")
except IOError as e:
    print(f"\nError saving data to file: {e}")
except Exception as e:
    print(f"\nAn unexpected error occurred during file saving: {e}")