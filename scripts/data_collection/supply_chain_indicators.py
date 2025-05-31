import pandas as pd
import datetime
import os
from dotenv import load_dotenv
from fredapi import Fred

load_dotenv()
FRED_API_KEY = os.getenv('FRED_API_KEY')

OUTPUT_DIR = "data/raw/US_indicators"
SUPPLY_CHAIN_EXT_CSV_FILE = os.path.join(OUTPUT_DIR, 'us_supply_chain_extended_indicators.csv')
SUPPLY_CHAIN_EXT_PARQUET_FILE = os.path.join(OUTPUT_DIR, 'us_supply_chain_extended_indicators.parquet')

fred = None
if not FRED_API_KEY:
    print("Critical Error: FRED_API_KEY not found. Please set it in your .env file.")
else:
    fred = Fred(api_key=FRED_API_KEY)
    print("Fred API client initialized for Supply Chain (Extended).")

END_DATE = datetime.datetime.now()
START_DATE = END_DATE - datetime.timedelta(days=15 * 365) # 15 years supply chain

series_map_supply_chain_ext = {
    "PPI Transportation Services Shipping Competitive": "WPU30160108",
    "PPI Transportation Services Water Transportation Freight": "WPU3013",
    "PPI Transportation Services": "WPU30",
    "PPI Transportation Services Truck Transportation Freight": "WPS3012",
    "Total Business Inventories to Sales Ratio": "ISRATIO",
    "Retailers Inventories to Sales Ratio": "RETAILIRSA",
    "Manufacturers Inventories to Sales Ratio": "MNFCTRIRSA",
    "Merchant Wholesalers Inventories to Sales Ratio": "WHLSLRIRSA",
    "Intl Merchandise Trade Exports Commodities US": "XTEXVA01USA664S",
    "Intl Merchandise Trade Imports Commodities US": "XTIMVA01USA664S",
    "Rail Freight Intermodal Traffic": "RAILFRTINTERMODAL",
    "Truck Tonnage Index": "TRUCKD11",
    "Total Vehicle Sales": "TOTALSA",
    "Domestic Auto Inventories": "AUINSA"
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
        print("Fetching US Supply Chain (Extended) Indicators from FRED using fredapi...")
        us_supply_chain_ext_df = fetch_fred_data_via_api(fred, series_map_supply_chain_ext, START_DATE, END_DATE)

        if not us_supply_chain_ext_df.empty:
            print("\n--- Combined US Supply Chain (Extended) Indicators Data (Last 5 entries) ---")
            print(us_supply_chain_ext_df.tail())
            print("\n--- Combined US Supply Chain (Extended) Indicators Data (First 5 entries) ---")
            print(us_supply_chain_ext_df.head())
            print(f"\nShape of the DataFrame: {us_supply_chain_ext_df.shape}")
            
            store_data(us_supply_chain_ext_df, SUPPLY_CHAIN_EXT_CSV_FILE, SUPPLY_CHAIN_EXT_PARQUET_FILE)
        else:
            print("Failed to retrieve any supply chain (extended) data from FRED via API. Nothing to store.")

# --- End of Script ---