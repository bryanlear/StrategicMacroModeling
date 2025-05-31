import pandas_datareader.data as pdr
import pandas as pd
import datetime
import pyarrow # or fastparquet
import os
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv() 
FRED_API_KEY = os.getenv('FRED_API_KEY')

# --- Output File Configuration ---
OUTPUT_DIR = "data/raw/US_indicators"
US_INDICATORS_CSV_FILE = os.path.join(OUTPUT_DIR, 'us_economic_indicators.csv')
US_INDICATORS_PARQUET_FILE = os.path.join(OUTPUT_DIR, 'us_economic_indicators.parquet')

if FRED_API_KEY:
    os.environ['FRED_API_KEY'] = FRED_API_KEY # pandas_datareader checks this env var
else:
    print("Warning: FRED_API_KEY not found. Data fetching might be limited or fail.")

END_DATE = datetime.datetime.now()
START_DATE = END_DATE - datetime.timedelta(days=2 * 365) # Two years of data

# --- FRED Series IDs for US Indicators ---
# Description: FRED Series ID
series_map = {
    "Gross Domestic Product": "GDP",
    "Real Gross Domestic Product": "GDPC1",
    "Real gross domestic product per capita": "A939RX0Q048SBEA",
    "Industrial Production: Total Index": "INDPRO",
    "Capacity Utilization: Total Index": "TCU",
    "Producer Price Index by Industry: Total Manufacturing Industries": "PCUOMFGOMFG",
    "All Employees, Manufacturing": "MANEMP",
    "Advance Retail Sales: Retail Trade (Excluding Food Services)": "RSXFS", #RSXFS is Retail Sales Excluding Food Services
    "Manufacturers' New Orders: Durable Goods": "DGORDER",
    "Manufacturers' New Orders: Consumer Durable Goods": "ACDGNO",
    "Personal Consumption Expenditures: Durable Goods": "PCEDG",
    "Personal outlays": "A068RC1",
    "Real Disposable Personal Income": "DSPIC96"
}
# --- Data Retrieval Function ---
def fetch_fred_data(series_dict, start_date, end_date):
    """
    Fetches specified series from FRED and combines them into a single DataFrame.
    """
    df_list = []
    print(f"Fetching data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    for short_name, series_id in series_dict.items():
        try:
            print(f"Fetching: {short_name} ({series_id})...")
            data = pdr.DataReader(series_id, 'fred', start_date, end_date)
            data.rename(columns={series_id: short_name}, inplace=True)
            df_list.append(data)
        except Exception as e:
            print(f"Could not retrieve series {series_id} ({short_name}): {e}")
            nan_series_df = pd.DataFrame(index=pd.date_range(start_date, end_date, freq='D'), columns=[short_name])
            df_list.append(nan_series_df)


    if not df_list:
        print("No data fetched.")
        return pd.DataFrame()

    # Combine all dataframes
    # Using outer join to keep all dates and fill missing values with NaN
    combined_df = pd.concat(df_list, axis=1, join='outer')
    
    # Forward fill and then backfill missing values
    # This handles different reporting frequencies (e.g., quarterly GDP vs monthly employment)
    combined_df.ffill(inplace=True)
    combined_df.bfill(inplace=True) # Backfill for any leading NaNs if series start late
    
    # Ensure columns match the order of original series_map keys for consistency and handles cases where some series might have failed to download
    ordered_columns = [name for name in series_map.keys() if name in combined_df.columns]
    combined_df = combined_df[ordered_columns]
    
    return combined_df

# --- Data Storage Function ---
def store_data(df, csv_filepath, parquet_filepath):
    """
    Stores the DataFrame in specified formats.
    """
    if df.empty:
        print("DataFrame is empty. No data to store.")
        return
    # Ensure the output directory exists
    output_dir = os.path.dirname(csv_filepath)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")

    # Store as CSV
    try:
        df.to_csv(csv_filepath, index=True) # index=True to save the date index
        print(f"Data successfully saved to CSV: {csv_filepath}")
    except Exception as e:
        print(f"Error saving data to CSV {csv_filepath}: {e}")

    # Store as Parquet
    try:
        df.to_parquet(parquet_filepath, index=True, engine='pyarrow') # or engine='fastparquet'
        print(f"Data successfully saved to Parquet: {parquet_filepath}")
        pass # Commented out by default
    except ImportError:
        print(f"Could not save to Parquet: pyarrow library not found. Install it with 'pip install pyarrow'")
    except Exception as e:
        print(f"Error saving data to Parquet {parquet_filepath}: {e}")


# --- Main Execution ---
if __name__ == "__main__":
    if not FRED_API_KEY:
        print("FRED_API_KEY is not set. Please set it in your .env file or environment variables.")
    else:
        print("Fetching US Economic Indicators from FRED...")
        us_indicators_df = fetch_fred_data(series_map, START_DATE, END_DATE)

        if not us_indicators_df.empty:
            print("\n--- Combined US Indicators Data (Last 5 entries) ---")
            print(us_indicators_df.tail())
            print("\n--- Combined US Indicators Data (First 5 entries) ---")
            print(us_indicators_df.head())
            print(f"\nShape of the DataFrame: {us_indicators_df.shape}")
            print("\nDescriptive Statistics:")
            print(us_indicators_df.describe())

            # Store the fetched data
            store_data(us_indicators_df, US_INDICATORS_CSV_FILE, US_INDICATORS_PARQUET_FILE)
        else:
            print("Failed to retrieve any data. Nothing to store.")

# --- End of Script ---