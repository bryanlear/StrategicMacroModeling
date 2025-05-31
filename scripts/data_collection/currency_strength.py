import pandas as pd
import datetime
import os
from dotenv import load_dotenv
from fredapi import Fred

load_dotenv()
FRED_API_KEY = os.getenv('FRED_API_KEY')

OUTPUT_DIR = "data/raw/US_indicators"
CURRENCY_STRENGTH_CSV_FILE = os.path.join(OUTPUT_DIR, 'us_currency_strength_indicators.csv')
CURRENCY_STRENGTH_PARQUET_FILE = os.path.join(OUTPUT_DIR, 'us_currency_strength_indicators.parquet')

fred = None
if not FRED_API_KEY:
    print("Critical Error: FRED_API_KEY not found. Please set it in your .env file.")
else:
    fred = Fred(api_key=FRED_API_KEY)
    print("Fred API client initialized for Currency Strength.")

END_DATE = datetime.datetime.now()
START_DATE = END_DATE - datetime.timedelta(days=15 * 365) # 10 years for currency indices

series_map_currency = {
    "Nominal Broad U.S. Dollar Index": "TWEXBGSMTH",
    "Real Broad Dollar Index": "RTWEXBGS",
    "Nominal Advanced Foreign Economies U.S. Dollar Index": "DTWEXAFEGS",
    "Nominal Emerging Market Economies U.S. Dollar Index": "DTWEXEMEGS",
    "Real Broad Effective Exchange Rate for United States": "RBUSBIS",
    "Broad Effective Exchange Rate for United States": "NBUSBIS",
    "Narrow Effective Exchange Rate for United States": "NNUSBIS"
}

def fetch_fred_data_via_api(fred_client, series_dict, start_date, end_date):
    if fred_client is None:
        print("FRED client not initialized. Cannot fetch data.")
        return pd.DataFrame()
        
    df_list = []
    print(f"Fetching FRED data via API from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    for short_name, series_id in series_dict.items():
        try:
            print(f"Fetching: {short_name} ({series_id})...")
            s = fred_client.get_series(series_id, observation_start=start_str, observation_end=end_str)
            
            if s.empty:
                print(f"No data returned for series {series_id} ({short_name}) for the given date range.")
                data_df = pd.DataFrame(columns=[short_name], index=pd.to_datetime([]))
            else:
                data_df = s.to_frame(name=short_name)
            
            df_list.append(data_df)
        except Exception as e:
            print(f"Could not retrieve series {series_id} ({short_name}) via API: {e}")
            empty_idx = pd.date_range(start=start_str, end=end_str, freq='MS') if not df_list else df_list[0].index
            nan_series_df = pd.DataFrame(index=empty_idx, columns=[short_name])
            df_list.append(nan_series_df)

    if not df_list:
        print("No data fetched from FRED via API.")
        return pd.DataFrame()

    combined_df = pd.concat(df_list, axis=1, join='outer') 
    
    if not combined_df.empty:
        combined_df.ffill(inplace=True)
        combined_df.bfill(inplace=True)
    
        ordered_columns = [name for name in series_dict.keys() if name in combined_df.columns]
        combined_df = combined_df[ordered_columns]
    
    return combined_df

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
    if fred is None:
        print("Script cannot run due to missing FRED API key or client initialization failure.")
    else:
        print("Fetching US Currency Strength Indicators from FRED using fredapi...")
        us_currency_strength_df = fetch_fred_data_via_api(fred, series_map_currency, START_DATE, END_DATE)

        if not us_currency_strength_df.empty:
            print("\n--- Combined US Currency Strength Indicators Data (Last 5 entries) ---")
            print(us_currency_strength_df.tail())
            print("\n--- Combined US Currency Strength Indicators Data (First 5 entries) ---")
            print(us_currency_strength_df.head())
            print(f"\nShape of the DataFrame: {us_currency_strength_df.shape}")
            
            store_data(us_currency_strength_df, CURRENCY_STRENGTH_CSV_FILE, CURRENCY_STRENGTH_PARQUET_FILE)
        else:
            print("Failed to retrieve any currency strength data from FRED via API. Nothing to store.")
