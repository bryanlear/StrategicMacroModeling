# File: scripts/discovery/discover_series.py

import json
import os
import sys

def create_metadata_mappings(structure_data):
    """
    Parses the 'structure' section of the JSON to create lookup dictionaries.
    """
    mappings = {}
    try:
        dimensions_container = structure_data['dimensions']
        series_dims_data = dimensions_container['series']

        for i, dim in enumerate(series_dims_data):
            dim_id = dim['id']
            dim_values = dim.get('values', [])
            mappings[f'dim_{i}'] = {item['id']: item['name'] for item in dim_values}
        
        mappings['series_dimension_keys'] = [d['id'] for d in series_dims_data]
        print(" -> Successfully created metadata mappings.")
        return mappings

    except KeyError as e:
        print(f" -> ERROR: A required key was not found in the 'structure' section: {e}")
        return None

def main():
    """
    Main function to run the interactive discovery workflow.
    """
    print("--- IMF Offline Series Key Discoverer ---")

    data_directory = 'scripts/data_collection/IMF_datasets'
    available_files = [f for f in sorted(os.listdir(data_directory)) if f.endswith('.json')]
    
    if not available_files:
        print(f"No JSON dataset files found in '{data_directory}'. Exiting.")
        return

    print("Available dataset files to explore:")
    for i, filename in enumerate(available_files):
        print(f"  [{i}] {filename}")
    
    try:
        choice = int(input("Enter the number of the dataset file to parse: "))
        input_filepath = os.path.join(data_directory, available_files[choice])
        dataflow_id = available_files[choice].split('_')[-2]
    except (ValueError, IndexError):
        print("Invalid selection. Exiting.")
        return

    print(f"\n--- Parsing Metadata From: {os.path.basename(input_filepath)} (Dataflow: {dataflow_id}) ---")
    
    try:
        with open(input_filepath, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        print(" -> Successfully loaded JSON file.")
    except Exception as e:
        print(f"Error loading file: {e}")
        return

    mappings = create_metadata_mappings(raw_data['structure'])
    if not mappings: return

    print("\n--- Interactively Build a Series Key ---")
    
    key_parts_dict = {}
    dim_names = mappings['series_dimension_keys']

    for i, dim_name in enumerate(dim_names):
        print(f"\n----- Building Dimension: '{dim_name}' -----")
        
        codes_for_dim = mappings.get(f'dim_{i}', {})
        
        if len(codes_for_dim) > 20:
            print(f"This dimension has {len(codes_for_dim)} possible codes.")
            while True:
                search_action = input("Enter a search term to find a code, 'list' (first 20), or press Enter to skip search: ").strip().lower()
                if not search_action: break
                
                if search_action == 'list':
                    print("--- First 20 Available Codes ---")
                    for j, (code, desc) in enumerate(codes_for_dim.items()):
                        print(f"  - {code:<20} ({desc})")
                        if j >= 19: break
                    continue

                matches = {code: desc for code, desc in codes_for_dim.items() if search_action in str(desc).lower() or search_action in str(code).lower()}
                
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
        
        part = input(f"Enter the chosen code for '{dim_name}': ").strip().upper()
        key_parts_dict[dim_name] = part

    ordered_parts = []
    for dim_name in dim_names:
        chosen_code = key_parts_dict.get(dim_name)
        if chosen_code:
            ordered_parts.append(chosen_code)
    
    series_key = ".".join(ordered_parts)
    # --- END FIX ---

    print("\n" + "="*50)
    print("  ✅ DISCOVERY COMPLETE")
    print(f"  Dataflow ID: {dataflow_id}")
    print(f"  Correctly Ordered Series Key: {series_key}")
    print("  Use these with the fetch_series.py script.")
    print("="*50 + "\n")

if __name__ == '__main__':
    main()