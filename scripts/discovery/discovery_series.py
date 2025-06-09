#scripts/discovery/discover_series.py

import json
import os

def create_metadata_mappings(structure_data):
    """
    Parses the complete 'DataStructure' JSON to create lookup dictionaries for all dimensions.
    This function is designed for the format downloaded by metadata_downloader.py.
    """
    mappings = {}
    try:
        # 1. Get list of dimensions and order
        key_family = structure_data['KeyFamilies']['KeyFamily']
        if isinstance(key_family, list):
            key_family = key_family[0]
        
        dimensions = key_family['Components']['Dimension']
        if not isinstance(dimensions, list):
            dimensions = [dimensions]
        
        # Store in correct order
        mappings['series_dimension_keys'] = [d.get('@conceptRef', d.get('@id')) for d in dimensions]

        # 2. Create fast lookup dictionary for all codelists in file
        all_codelists_list = structure_data['CodeLists']['CodeList']
        all_codelists_dict = {cl['@id']: cl for cl in all_codelists_list}
        print(" -> Found all Codelists defined in the file.")

        # 3. For each dimension find codelist and map codes/descriptions
        for i, dim_spec in enumerate(dimensions):
            codelist_id = dim_spec.get('@codelist')
            
            if not codelist_id:
                print(f" -> Warning: Dimension '{dim_spec.get('@conceptRef')}' has no codelist ID specified.")
                mappings[f'dim_{i}'] = {}
                continue

            target_codelist = all_codelists_dict.get(codelist_id)
            if not target_codelist:
                print(f" -> ERROR: Could not find Codelist with ID '{codelist_id}' in the 'CodeLists' section.")
                mappings[f'dim_{i}'] = {}
                continue

            codes = target_codelist.get('Code', [])
            if not isinstance(codes, list):
                codes = [codes] 

            # Create mapping dictionary for current dimension
            code_map = {}
            for code_entry in codes:
                code_value = code_entry.get('@value')
                description_obj = code_entry.get('Description', {})
                description_text = description_obj.get('#text', 'No description') if isinstance(description_obj, dict) else description_obj
                if code_value:
                    code_map[code_value] = description_text
            
            mappings[f'dim_{i}'] = code_map
            
        print(" -> Successfully created metadata mappings from DataStructure file.")
        return mappings

    except (KeyError, TypeError, IndexError) as e:
        print(f" -> ERROR: A required key was not found or the JSON structure was unexpected: {e}")
        return None

def main():
    """
    Main function to run the interactive discovery workflow.
    """
    print("--- IMF Offline Series Key Discoverer ---")

    data_directory = 'scripts/data_collection/IMF_datasets'
    
    if not os.path.exists(data_directory):
        print(f"Error: Directory '{data_directory}' not found. Please create it or run metadata_downloader.py first.")
        return
        
    available_files = [f for f in sorted(os.listdir(data_directory)) if f.endswith('.json')]
    
    if not available_files:
        print(f"No JSON dataset files found in '{data_directory}'.")
        print("Please run 'metadata_downloader.py' first to download a complete DataStructure or CodeList file.")
        return

    print("Available dataset files to explore:")
    for i, filename in enumerate(available_files):
        print(f"  [{i}] {filename}")
    
    try:
        choice = int(input("Enter the number of the dataset file to parse: "))
        input_filepath = os.path.join(data_directory, available_files[choice])
        if 'datastructure_' in os.path.basename(input_filepath):
            dataflow_id = os.path.basename(input_filepath).replace('datastructure_', '').replace('.json', '')
        else: 
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

    if 'Structure' not in raw_data or ('KeyFamilies' not in raw_data['Structure'] and 'CodeLists' not in raw_data['Structure']):
        print("Error: This does not appear to be a valid IMF DataStructure JSON file.")
        print("Please download a full DataStructure file using the metadata_downloader.py script.")
        return
        
    mappings = create_metadata_mappings(raw_data['Structure'])
    if not mappings: return

    print("\n--- Interactively Build a Series Key ---")
    
    key_parts_dict = {}
    dim_names = mappings.get('series_dimension_keys', [])

    for i, dim_name in enumerate(dim_names):
        print(f"\n----- Building Dimension: '{dim_name}' -----")
        
        codes_for_dim = mappings.get(f'dim_{i}', {})
        
        print(f"This dimension has {len(codes_for_dim)} possible codes.")
        while True:
            search_action = input("Enter a search term, 'list' (first 20), press Enter to skip, or 'manual' to enter a code directly: ").strip().lower()
            
            if not search_action: 
                break 

            if search_action == 'list':
                if not codes_for_dim:
                    print("No codes available for this dimension.")
                else:
                    print("--- First 20 Available Codes ---")
                    for j, (code, desc) in enumerate(codes_for_dim.items()):
                        print(f"  - {code:<20} ({desc})")
                        if j >= 19: break
                continue

            if search_action == 'manual':
                break

            matches = {code: desc for code, desc in codes_for_dim.items() if search_action in str(desc).lower() or search_action in str(code).lower()}
            
            if matches:
                print("--- Found Matches ---")
                for code, desc in matches.items():
                    print(f"  - {code:<20} ({desc})")
                print("---------------------")
            else:
                print("No matches found.")
        
        part = input(f"Enter the chosen code for '{dim_name}' (or press Enter to leave blank): ").strip().upper()
        key_parts_dict[dim_name] = part

    ordered_parts = []
    for dim_name in dim_names:
        chosen_code = key_parts_dict.get(dim_name)
        ordered_parts.append(chosen_code if chosen_code else "")
    
    series_key = ".".join(ordered_parts)

    print("\n" + "="*50)
    print("  ✅ DISCOVERY COMPLETE")
    print(f"  Dataflow ID: {dataflow_id}")
    print(f"  Correctly Ordered Series Key: {series_key}")
    print("  You can now use this information with a direct fetcher script (like imf_direct_key_fetcher.py).")
    print("="*50 + "\n")

if __name__ == '__main__':
    main()
