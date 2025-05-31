import pandas as pd
import requests
import io
import os
import datetime

# --- Configuration ---
OUTPUT_DIR = "data/raw/US_indicators"
GSCPI_CSV_FILE = os.path.join(OUTPUT_DIR, 'nyfed_gscpi.csv')
GSCPI_PARQUET_FILE = os.path.join(OUTPUT_DIR, 'nyfed_gscpi.parquet')
GSCPI_DATA_URL = "https://www.newyorkfed.org/medialibrary/research/interactives/gscpi/downloads/gscpi_data.xlsx"

def fetch_and_process_gscpi(url):
    print(f"Fetching GSCPI data from: {url}")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        excel_file_content = io.BytesIO(response.content)
        
        xls = pd.ExcelFile(excel_file_content)
        
        target_sheet_name = None
        expected_sheet_name = "GSCPI Monthly Data" 

        if expected_sheet_name in xls.sheet_names:
            target_sheet_name = expected_sheet_name
            print(f"Found expected sheet: '{target_sheet_name}'")
        else:
            print(f"Warning: Expected sheet '{expected_sheet_name}' not found.")
            print(f"Available sheets: {xls.sheet_names}")
            if xls.sheet_names:
                target_sheet_name = xls.sheet_names[0] 
                print(f"Attempting to use the first available sheet: '{target_sheet_name}'")
            else:
                print("Error: No sheets found in the Excel file.")
                return pd.DataFrame()

        if target_sheet_name:
            df = pd.read_excel(xls, sheet_name=target_sheet_name, header=0)
            print(f"Successfully read sheet '{target_sheet_name}'.")
            print(f"Columns found: {df.columns.tolist()}")
        else:
            print("Error: Could not determine a target sheet to read.")
            return pd.DataFrame()

        # --- Data Cleaning and Processing ---
        date_col_name = None
        gscpi_col_name = None

        for col in df.columns:
            col_str_original = str(col) 
            col_str_lower_stripped = col_str_original.strip().lower()
            if 'date' == col_str_lower_stripped: 
                date_col_name = col_str_original
            elif 'gscpi' == col_str_lower_stripped: 
                gscpi_col_name = col_str_original
        
        if date_col_name and gscpi_col_name:
            print(f"Identified Date column as: '{date_col_name}', GSCPI column as: '{gscpi_col_name}'")
            df = df[[date_col_name, gscpi_col_name]]
            df.columns = ['Date', 'GSCPI'] # Standardize column names
        elif len(df.columns) >= 2:
            print(f"Warning: Could not precisely match 'Date'/'GSCPI' by name. Columns: {df.columns.tolist()}. Assuming first two columns are Date and GSCPI.")
            df = df.iloc[:, :2] # Take the first two columns
            df.columns = ['Date', 'GSCPI'] # Rename them
        else:
            print(f"Error: Not enough columns to identify 'Date' and 'GSCPI'. Columns available: {df.columns.tolist()}")
            return pd.DataFrame()

        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df.dropna(subset=['Date'], inplace=True)
        
        if df.empty:
            print("DataFrame became empty after Date conversion/dropna.")
            return pd.DataFrame()
            
        df.set_index('Date', inplace=True)
        
        df['GSCPI'] = pd.to_numeric(df['GSCPI'], errors='coerce')
        df.dropna(subset=['GSCPI'], inplace=True)
        
        if df.empty:
            print("DataFrame became empty after GSCPI conversion/dropna.")
            return pd.DataFrame()

        df.sort_index(inplace=True)
        print("GSCPI data processed successfully.")
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching GSCPI data: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred during processing: {e}")
        return pd.DataFrame()


def store_data(df, csv_filepath, parquet_filepath):
    if df.empty:
        print("DataFrame is empty. No data to store.")
        return

    output_dir = os.path.dirname(csv_filepath)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")

    try:
        df.to_csv(csv_filepath, index=True)
        print(f"Data successfully saved to CSV: {csv_filepath}")
    except Exception as e:
        print(f"Error saving data to CSV {csv_filepath}: {e}")

    try:
        df.to_parquet(parquet_filepath, index=True, engine='pyarrow')
        print(f"Data successfully saved to Parquet: {parquet_filepath}")
    except ImportError:
        print(f"Could not save to Parquet: pyarrow library not found. Install it with 'pip install pyarrow'")
    except Exception as e:
        print(f"Error saving data to Parquet {parquet_filepath}: {e}")


if __name__ == "__main__":
    gscpi_df = fetch_and_process_gscpi(GSCPI_DATA_URL)

    if not gscpi_df.empty:
        print("\n--- GSCPI Data (Last 5 entries) ---")
        print(gscpi_df.tail())
        print("\n--- GSCPI Data (First 5 entries) ---")
        print(gscpi_df.head())
        print(f"\nShape of the GSCPI DataFrame: {gscpi_df.shape}")
        
        store_data(gscpi_df, GSCPI_CSV_FILE, GSCPI_PARQUET_FILE)
    else:
        print("Failed to retrieve or process GSCPI data. Final DataFrame is empty.")

