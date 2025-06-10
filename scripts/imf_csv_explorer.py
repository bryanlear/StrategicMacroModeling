import pandas as pd
import os
import re

DATA_DIRECTORY = 'scripts/data_collection/IMF_datasets'
OUTPUT_DIRECTORY = "data/raw/IMF_csv_output" 

def find_dimension_columns(df):
    """
    Identifies which columns are likely dimensions for filtering by looking for
    a standard set of IMF dimension names present in the dataframe columns.
    """
    all_cols = df.columns.tolist()
    
   # Dimensions in IMF datasets can vary, but common ones include:
    potential_dims = [
        "COUNTRY", "INDICATOR", "FREQUENCY", "SECTOR", 
        "BOP_ACCOUNTING_ENTRY", "WEO_SUBJECT",
        "TYPE_OF_TRANSFORMATION", "UNIT", "SCALE"
    ]
    # Find which one exist in the current file
    found_dims = [p for p in potential_dims if p in all_cols]

    print(f" -> Auto-detected Dimension Columns for filtering: {found_dims}")
    return found_dims

def save_timeseries(df, dataflow_id, series_to_process):
    """
    Takes a dataframe and the selected series metadata, then extracts, reshapes,
    and saves the cleaned time series data to CSV and Parquet files.
    """
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    
    # Identify metadata columns (non-time periods) and value columns (time periods)
    metadata_cols = [col for col in df.columns if not re.match(r'^\d{4}(-[MQ]\d{1,2})?$', col)]
    value_cols = [col for col in df.columns if col not in metadata_cols]

    # Wide format to long format
    long_df = pd.melt(
        series_to_process, id_vars=metadata_cols, value_vars=value_cols,
        var_name='Date', value_name='Value'
    )
    
    # Conver 'Value' column a numeric. Empty strings/non-numeric placeholders = 'NaN'
    long_df['Value'] = pd.to_numeric(long_df['Value'], errors='coerce')
    long_df.dropna(subset=['Value'], inplace=True)


    if long_df.empty:
        print("\nError: The selected series has no valid observations in the file. Nothing to save.")
        return

    long_df['Date'] = long_df['Date'].str.replace('M', '-').str.replace('Q', '-')
    long_df['Date'] = pd.to_datetime(long_df['Date'], errors='coerce')
    long_df.dropna(subset=['Date'], inplace=True)
    long_df = long_df.sort_values(by='Date').reset_index(drop=True)
    
    try:
        country = str(series_to_process['COUNTRY.ID'].iloc[0])
        indicator = str(series_to_process['INDICATOR.ID'].iloc[0])
        freq = str(series_to_process['FREQUENCY.ID'].iloc[0])
        output_filename_base = f"data_{dataflow_id}_{country}_{indicator}_{freq}".replace(':', '_')
    except KeyError as e:
        print(f"Warning: Could not find a metadata ID column ({e}) to create filename. Using a generic name.")
        output_filename_base = f"data_{dataflow_id}_extracted_series"


    output_csv = os.path.join(OUTPUT_DIRECTORY, f"{output_filename_base}.csv")
    output_parquet = os.path.join(OUTPUT_DIRECTORY, f"{output_filename_base}.parquet")
    
    final_df_to_save = long_df[['Date', 'Value']]

    final_df_to_save.to_csv(output_csv, index=False)
    print(f"\n✅ Success! Clean time series data saved to: {output_csv}")
    try:
        final_df_to_save.to_parquet(output_parquet, index=False)
        print(f"Clean time series data saved to: {output_parquet}")
    except ImportError:
        print("Could not save to Parquet. Install pyarrow with 'pip install pyarrow'")
    except Exception as e:
        print(f"An error occurred while saving to Parquet: {e}")

def main():
    """
    Main function to run the interactive CSV discovery and extraction workflow.
    """
    print("--- IMF CSV Data Explorer and Extractor ---")

    if not os.path.exists(DATA_DIRECTORY):
        print(f"Error: Source directory '{DATA_DIRECTORY}' not found.")
        return
        
    available_files = [f for f in sorted(os.listdir(DATA_DIRECTORY)) if f.endswith('.csv')]
    
    if not available_files:
        print(f"No CSV files found in '{DATA_DIRECTORY}'.")
        return

    print("Available CSV dataset files to explore:")
    for i, filename in enumerate(available_files):
        print(f"  [{i}] {filename}")
    
    try:
        choice = int(input("Enter the number of the dataset file to parse: "))
        input_filepath = os.path.join(DATA_DIRECTORY, available_files[choice])
        match = re.search(r'STA_([A-Z]+)_', os.path.basename(input_filepath))
        dataflow_id = match.group(1) if match else "UNKNOWN"
    except (ValueError, IndexError):
        print("Invalid selection. Exiting.")
        return

    print(f"\n--- Loading and Analyzing: {os.path.basename(input_filepath)} (Dataflow: {dataflow_id}) ---")
    
    try:
        df = pd.read_csv(input_filepath, low_memory=False)
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return

    print("\n--- Interactively Filter to Find Your Series ---")
    
    dimension_cols = find_dimension_columns(df)
    if not dimension_cols:
        print("Could not automatically determine dimension columns to filter by. Exiting.")
        return

    filtered_df = df.copy()

    for dim_col in dimension_cols:
        options = sorted(filtered_df[dim_col].dropna().unique())
        if not options or len(options) == 1:
            print(f"Skipping dimension '{dim_col}' as it has only one unique option with current filters.")
            continue
        
        print(f"\n----- Filtering by Dimension: '{dim_col}' -----")
        print(f"Found {len(options)} unique option(s).")
        
        while True:
            search_action = input("Enter search term, 'list' (first 20), or press Enter to skip this filter: ").strip().lower()
            if not search_action: break
            
            if search_action == 'list':
                print(f"--- First 20 of {len(options)} options for '{dim_col}' ---")
                for i, option in enumerate(options[:20]): print(f"  [{i}] {option}")
                continue

            matches = [opt for opt in options if search_action in str(opt).lower()]
            
            if matches:
                print("--- Found Matches ---")
                for i, match in enumerate(matches): print(f"  [{i}] {match}")
                try:
                    selection_idx_str = input("Enter the number of your choice (or press Enter to search again): ")
                    if not selection_idx_str: continue # Allow user to re-search
                    
                    selection_idx = int(selection_idx_str)
                    chosen_value = matches[selection_idx]
                    
                    filtered_df = filtered_df[filtered_df[dim_col] == chosen_value].copy()
                    print(f" -> Filtered by '{dim_col}' = '{chosen_value}'. {len(filtered_df)} series remain.")
                    break 
                except (ValueError, IndexError):
                    print("Invalid selection. Please try again.")
            else:
                print("No matches found.")
        
        if len(filtered_df) < 1: break
        if len(filtered_df) == 1:
            print("\nFiltered down to a single series.")
            break

    if filtered_df.empty:
        print("\nNo series matched your filter criteria.")
        return
        
    print("\n" + "="*50)
    print("  ✅ DISCOVERY COMPLETE")
    print(f"  Found {len(filtered_df)} matching series. Displaying first one:")
    
    metadata_cols = [col for col in df.columns if not re.match(r'^\d{4}(-[MQ]\d{1,2})?$', col)]
    print(filtered_df.iloc[0][metadata_cols].to_string())
    print("="*50 + "\n")

    if input("Do you want to extract and save this time series? (y/n): ").lower() == 'y':
        series_to_process = filtered_df.iloc[0:1]
        save_timeseries(df, dataflow_id, series_to_process)
    else:
        print("Exiting without saving.")

if __name__ == '__main__':
    main()
