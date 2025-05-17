import requests
import os
from dotenv import load_dotenv
import json
import datetime

load_dotenv()

# Configuration
DATA_GOV_API_BASE_URL = "https://catalog.data.gov/api/3/action/package_search"
DATA_GOV_API_KEY = os.getenv("DATA_GOV_API_KEY")

# Output filenames
TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
JSON_OUTPUT_FILE = f"data_gov_house_budget_search_results_{TIMESTAMP}.json"
TXT_OUTPUT_FILE = f"data_gov_house_budget_search_results_{TIMESTAMP}.txt"


def search_data_gov_datasets(search_terms, api_key=None, rows_per_query=10):
    """
    Searches data.gov for datasets based on a list of search terms.

    Args:
        search_terms (list): A list of strings, where each string is a search query.
        api_key (str, optional): Your data.gov API key. Defaults to None.
        rows_per_query (int, optional): Number of results to request per query. Defaults to 10.

    Returns:
        dict: A dictionary where keys are search terms and values are lists of
              dictionaries, each representing a found dataset. Returns None on critical error.
    """
    if api_key:
        print("INFO: Using Data.gov API Key for searches.")
    else:
        print("INFO: Data.gov API Key not found or not provided. Searches will be made without an API key.")
        print("      For more extensive use, obtaining and using an API key from data.gov is recommended.")

    headers = {}

    all_results_by_term = {}

    for term in search_terms:
        print(f"\n--- Searching for datasets related to: '{term}' ---")
        params = {
            'q': term,
            'rows': rows_per_query
        }

        term_datasets = []
        try:
            response = requests.get(DATA_GOV_API_BASE_URL, params=params, headers=headers, timeout=20)
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            results_json = response.json()

            if results_json.get("success") and results_json["result"]["count"] > 0:
                total_found = results_json["result"]["count"]
                print(f"Found {total_found} total dataset(s) for '{term}'. Processing up to {rows_per_query}:")
                
                for i, dataset_meta in enumerate(results_json["result"]["results"]):
                    organization_info = dataset_meta.get('organization')
                    org_title = organization_info.get('title', 'N/A') if organization_info else 'N/A'
                    
                    notes = dataset_meta.get('notes', 'N/A')
                    
                    dataset_entry = {
                        "title": dataset_meta.get('title', 'N/A'),
                        "organization": org_title,
                        "description_notes": notes,
                        "data_gov_link": f"https://catalog.data.gov/dataset/{dataset_meta.get('name', '')}",
                        "resources": []
                    }
                    
                    if dataset_meta.get("resources") and len(dataset_meta["resources"]) > 0:
                        for resource_meta in dataset_meta["resources"]:
                            dataset_entry["resources"].append({
                                "name": resource_meta.get('name', 'N/A'),
                                "format": resource_meta.get('format', 'N/A'),
                                "url": resource_meta.get('url', 'N/A'),
                                "description": resource_meta.get('description', 'N/A')
                            })
                    term_datasets.append(dataset_entry)
                all_results_by_term[term] = {
                    "total_found_for_term": total_found,
                    "datasets_retrieved": term_datasets
                }
            else:
                print(f"No datasets found for '{term}'.")
                all_results_by_term[term] = {
                    "total_found_for_term": 0,
                    "datasets_retrieved": []
                }

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred while searching for '{term}': {http_err} - Status Code: {response.status_code}")
        except requests.exceptions.ConnectionError as conn_err:
            print(f"Connection error occurred while searching for '{term}': {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            print(f"Timeout occurred while searching for '{term}': {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            print(f"An error occurred during the request for '{term}': {req_err}")
        except json.JSONDecodeError as json_err:
            print(f"Error decoding JSON response for '{term}': {json_err}. Response text: {response.text[:500]}")
            # Potentially skip this term or return partial results
            all_results_by_term[term] = {"error": "Failed to decode JSON", "datasets_retrieved": []}
            
    return all_results_by_term

def save_results_to_json(results_data, filename):
    """Saves the collected results to a JSON file."""
    if not results_data:
        print("No results to save to JSON.")
        return
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results_data, f, ensure_ascii=False, indent=4)
        print(f"\nResults successfully saved to JSON file: {filename}")
    except IOError as e:
        print(f"Error writing JSON to file {filename}: {e}")

def save_results_to_txt(results_data, filename):
    """Saves the collected results to a formatted text file."""
    if not results_data:
        print("No results to save to TXT.")
        return
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"Data.gov Search Results - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=====================================================================\n")
            for term, term_data in results_data.items():
                f.write(f"\nSearch Term: \"{term}\"\n")
                f.write("---------------------------------------------------------------------\n")
                f.write(f"Total datasets found for this term: {term_data.get('total_found_for_term', 'N/A')}\n")
                
                datasets = term_data.get("datasets_retrieved", [])
                if not datasets:
                    f.write("No specific datasets retrieved or displayed for this term.\n")
                    if "error" in term_data:
                        f.write(f"Error for this term: {term_data['error']}\n")
                    continue

                f.write(f"Displaying details for {len(datasets)} dataset(s):\n")
                for i, dataset in enumerate(datasets):
                    f.write(f"\n  Result {i+1}:\n")
                    f.write(f"    Title: {dataset.get('title', 'N/A')}\n")
                    f.write(f"    Organization: {dataset.get('organization', 'N/A')}\n")
                    
                    desc = dataset.get('description_notes', 'N/A')
                    if desc and len(desc) > 300: # Truncate long descriptions in TXT
                        desc = desc[:300] + "..."
                    f.write(f"    Description/Notes: {desc}\n")
                    f.write(f"    Data.gov Link: {dataset.get('data_gov_link', 'N/A')}\n")
                    
                    resources = dataset.get("resources", [])
                    if resources:
                        f.write(f"    Resources ({len(resources)} available):\n")
                        for j, resource in enumerate(resources[:5]): # Display first 5 resources
                            res_name = resource.get('name', 'N/A')
                            if res_name and len(res_name) > 70: res_name = res_name[:70] + "..."
                            res_desc = resource.get('description', 'N/A')
                            if res_desc and len(res_desc) > 100: res_desc = res_desc[:100] + "..."

                            f.write(f"      - [{j+1}] Name: {res_name}\n")
                            f.write(f"          Format: {resource.get('format', 'N/A')}\n")
                            f.write(f"          Description: {res_desc}\n")
                            f.write(f"          URL: {resource.get('url', 'N/A')}\n")
                    else:
                        f.write("    Resources: No direct resources listed for this dataset.\n")
                f.write("---------------------------------------------------------------------\n")
            f.write("\n--- End of Report ---")
        print(f"\nResults successfully saved to TXT file: {filename}")
    except IOError as e:
        print(f"Error writing TXT to file {filename}: {e}")


if __name__ == "__main__":
    # Define search terms related to the House Budget Committee
    house_budget_search_queries = [
        "House Budget Committee",
        "House Committee on the Budget data",
        "congressional budget House",
        "federal budget process House",
        "House budget resolutions",
        # "House budget testimony", # Often PDFs, might be less structured data
        # "House budget hearings transcripts", # Similar to testimony
        # "House appropriations data", # Related but distinct committee
        "legislative budget data House"
    ]

    # Perform the search
    search_results = search_data_gov_datasets(
        house_budget_search_queries, 
        api_key=DATA_GOV_API_KEY,
        rows_per_query=6
    )

    if search_results:
        # Save results to JSON
        save_results_to_json(search_results, JSON_OUTPUT_FILE)
        
        # Save results to formatted TXT
        save_results_to_txt(search_results, TXT_OUTPUT_FILE)
    else:
        print("No search results were obtained to save.")

    print("\n--- Script finished ---")

