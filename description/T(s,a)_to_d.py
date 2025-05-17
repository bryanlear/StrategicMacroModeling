# T(s,a)_to_d.py

import yfinance as yf
import pandas_datareader.data as pdr
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import os
import requests 
from dotenv import load_dotenv
import numpy as np 

# --- Configuration ---
LOCAL_DATA_FILE = 'combined_data.csv' 
load_dotenv()
FRED_API_KEY = os.getenv('FRED_API_KEY') 
DATA_GOV_API_KEY = os.getenv('DATA_GOV_API_KEY')
DATA_GOV_SEARCH_URL = "https://catalog.data.gov/api/3/action/package_search"

# Range for data fetching
END_DATE = datetime.datetime.now()
START_DATE = END_DATE - datetime.timedelta(days=10 * 365) # Years of data to fetch

YF_TICKERS_MAP = {
}

FRED_GENERAL_TICKERS_MAP = {
    "US_2Y_Treasury": "DGS2",
    "US_10Y_Treasury": "DGS10",
    "USD_Index_FRED": "DTWEXBGS",
    "EUR_USD_FRED": "DEXUSEU",
    "GBP_USD_FRED": "DEXUSUK",
    "JPY_PER_USD_FRED": "DEXJPUS" # Inverted to USD_PER_JPY_FRED
}

ENERGY_SERIES_FRED = {
    "US_Regular_Conventional_Gas_price": "GASREGCOVW",
    "Natural_Gas_Exports_Index": "IQ112",
    "Natural_Gas_Imports_Index": "IR10110",
    "Natural_Gas_Consumption": "NATURALGASD11",
    "Crude_Import_price_Index": "IR10000",
    "CBOE_Crude_Oil_ETF_Volatility_Index": "OVXCLS",
    "WTI": "DCOILWTICO",
    "Brent": "DCOILBRENTEU",
    "Producer_Price_Index_Gasoline": "WPS0571",
    "Producer_Price_Index_Final_Demand_Energy": "PPIDES",
    "Avg_Gasoline_Unleaded_Regular": "APU000074714"
}

ALL_FRED_TICKERS = {**FRED_GENERAL_TICKERS_MAP, **ENERGY_SERIES_FRED}

# --- Helper Functions ---
def _print_api_key_status():
    """Prints the status of loaded API keys."""
    if FRED_API_KEY == '12345':
        print(f"INFO: Using placeholder FRED API Key: '{FRED_API_KEY}' (Note: often not required for public FRED data)")
    else:
        print("INFO: FRED API Key loaded.")

    if DATA_GOV_API_KEY:
        print("INFO: Data.gov API Key loaded.")
    else:
        print("INFO: Data.gov API Key not found in .env. Searches will be made without an API key (if applicable).")

def search_data_gov(query):
    """
    Searches data.gov for datasets matching the query.
    This is a fallback discovery tool.
    """
    print(f"\nINFO: Attempting to search data.gov for '{query}' as a fallback...")
    params = {'q': query, 'rows': 3} # Limit results
    headers = {}

    try:
        response = requests.get(DATA_GOV_SEARCH_URL, params=params, headers=headers, timeout=10)
        response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
        results = response.json()
        
        if results.get('success') and results['result']['count'] > 0:
            print(f"INFO: Found {results['result']['count']} potential datasets on data.gov for '{query}':")
            for i, dataset in enumerate(results['result']['results']):
                title = dataset.get('title', 'N/A')
                org_info = dataset.get('organization')
                org_title = org_info.get('title', 'N/A') if org_info else 'N/A'
                resource_url = (dataset.get('resources')[0].get('url', 'N/A')
                                if dataset.get('resources') and dataset['resources'] else 'N/A')
                dataset_link = f"https://catalog.data.gov/dataset/{dataset.get('name', '')}"
                
                print(f"  {i+1}. Title: {title}")
                print(f"     Organization: {org_title}")
                print(f"     Example Resource URL: {resource_url}")
                print(f"     Data.gov link: {dataset_link}")
        else:
            print(f"INFO: No datasets found on data.gov for '{query}'.")
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Could not connect or search data.gov for '{query}': {e}")
    except ValueError as e: # Includes JSONDecodeError
        print(f"ERROR: Could not decode JSON response from data.gov for '{query}': {e}")
    print("INFO: Manual investigation of data.gov results would be required to integrate them.")

def standardize_series(series):
    """Applies Z-score standardization to a pandas Series."""
    series_numeric = pd.to_numeric(series, errors='coerce')
    mean = series_numeric.mean()
    std_dev = series_numeric.std(ddof=0) # Population std
    if std_dev == 0 or pd.isna(std_dev): # Avoid division by zero or NaN std
        print(f"WARNING: Could not standardize series '{series.name}' (std is 0 or NaN). Returning NaNs.")
        return pd.Series(np.nan, index=series_numeric.index, name=series.name + "_std")
    return (series_numeric - mean) / std_dev

# --- Core Data Fetching and Processing ---
def fetch_and_store_data(data_file_path=LOCAL_DATA_FILE, start_date_param=START_DATE, end_date_param=END_DATE):
    """
    Fetches financial data from various sources, processes it, and stores it in a local CSV file.
    Returns True if successful and data is saved, False otherwise, and the path to the data file.
    """
    _print_api_key_status()
    print(f"--- Starting Data Fetching ({start_date_param.strftime('%Y-%m-%d')} to {end_date_param.strftime('%Y-%m-%d')}) ---")
    all_series_data = {} 

    if YF_TICKERS_MAP:
        print("Fetching data from Yahoo Finance...")
        for name, ticker in YF_TICKERS_MAP.items():
            try:
                data = yf.download(ticker, start=start_date_param, end=end_date_param, progress=False, timeout=10)
                if not data.empty and not data['Close'].isnull().all():
                    series_to_use = data['Adj Close'] if 'Adj Close' in data.columns and not data['Adj Close'].isnull().all() else data['Close']
                    all_series_data[name] = series_to_use
                    print(f"Successfully fetched {name} ({ticker}) from Yahoo Finance.")
                else:
                    print(f"WARNING: No data (or all NaN data) returned for {name} ({ticker}) from Yahoo Finance.")
                    search_data_gov(f"{name} financial data") # Fallback search
            except Exception as e:
                print(f"ERROR: Fetching {name} ({ticker}) from Yahoo Finance failed: {e}")
                search_data_gov(f"{name} financial data") # Fallback search
    else:
        print("INFO: No tickers specified for Yahoo Finance, skipping yfinance fetch.")

    # Fetch from FRED
    print("\nFetching data from FRED...")
    for name, series_id in ALL_FRED_TICKERS.items():
        try:
            data = pdr.get_data_fred(series_id, start=start_date_param, end=end_date_param)
            if not data.empty and not data[series_id].isnull().all():
                all_series_data[name] = data[series_id]
                print(f"Successfully fetched {name} ({series_id}) from FRED.")
            else:
                print(f"WARNING: No data (or all NaN data) for {name} ({series_id}) from FRED.")
                search_data_gov(f"{name} FRED series {series_id}") # Fallback search
        except Exception as e:
            print(f"ERROR: Fetching {name} ({series_id}) from FRED failed: {e}")
            search_data_gov(f"{name} FRED series {series_id}") # Fallback search

    if not all_series_data:
        print("CRITICAL ERROR: No data was fetched from any source. Cannot proceed.")
        return False, None

    combined_data = pd.DataFrame(all_series_data)
    
    if combined_data.empty:
        print("CRITICAL ERROR: Combined data is empty before transformations. No data to save or plot.")
        return False, None
        
    combined_data.index = pd.to_datetime(combined_data.index)

    # Data Transformation: Invert JPY_PER_USD_FRED to get USD_PER_JPY_FRED
    if "JPY_PER_USD_FRED" in combined_data.columns:
        jpy_series = pd.to_numeric(combined_data["JPY_PER_USD_FRED"], errors='coerce')
        combined_data["USD_PER_JPY_FRED"] = np.where(
            jpy_series.notna() & (jpy_series != 0), 
            1 / jpy_series, 
            np.nan
        )
        print("INFO: Calculated USD_PER_JPY_FRED from JPY_PER_USD_FRED.")
    else:
        print("WARNING: JPY_PER_USD_FRED not found, cannot calculate USD_PER_JPY_FRED.")
    # Data Transformation: Fill forward and drop all-NaN rows
    combined_data = combined_data.ffill().dropna(how='all')

    if combined_data.empty:
        print("CRITICAL ERROR: Combined data is empty after cleaning and transformations. Cannot save or plot.")
        return False, None

    # Save to local CSV
    try:
        combined_data.to_csv(data_file_path)
        print(f"\n--- Data successfully fetched and saved to '{data_file_path}' ---")
        if not combined_data.empty:
            print("Columns in saved data:", combined_data.columns.tolist())
            print("Sample of saved data (first 5 rows):")
            print(combined_data.head())
        return True, data_file_path
    except Exception as e:
        print(f"CRITICAL ERROR: Saving data to CSV '{data_file_path}' failed: {e}")
        return False, None

def load_and_prepare_data_for_plotting(data_file_path=LOCAL_DATA_FILE):
    """
    Loads data from the local CSV and prepares it for plotting, including standardization.
    Returns the original and standardized DataFrames.
    """
    print(f"\n--- Reading data from '{data_file_path}' for plotting ---")
    try:
        all_data = pd.read_csv(data_file_path, index_col=0, parse_dates=True)
        if all_data.empty:
            print("ERROR: Loaded data is empty. Cannot generate plots.")
            return None, None
        print("Data loaded successfully. Standardizing relevant series...")
        print("Original columns available:", all_data.columns.tolist())
    except FileNotFoundError:
        print(f"ERROR: Data file '{data_file_path}' not found. Please ensure data fetching was successful.")
        return None, None
    except Exception as e:
        print(f"ERROR: Reading data from CSV '{data_file_path}' failed: {e}")
        return None, None

    # Standardize columns
    cols_for_standardization = [
        "USD_Index_FRED", "US_2Y_Treasury", "US_10Y_Treasury",
        "EUR_USD_FRED", "GBP_USD_FRED", "USD_PER_JPY_FRED",
        "Brent", "WTI"
    ]

    standardized_data = pd.DataFrame(index=all_data.index)
    for col in cols_for_standardization:
        if col in all_data.columns:
            standardized_series_data = standardize_series(all_data[col])
            standardized_data[col + "_std"] = standardized_series_data
            print(f"Standardized {col} as {col}_std")
        else:
            print(f"WARNING: Column {col} not found in loaded data for standardization.")
    
    print("Columns available after standardization attempt:", standardized_data.columns.tolist())
    return all_data, standardized_data


# --- Plotting Functions ---

def plot_standardized_usd_treasuries(standardized_df):
    """Plots standardized USD Index and Treasury yields."""
    if standardized_df is None or standardized_df.empty:
        print("Plot 1: No standardized data to plot.")
        return

    plt.figure(figsize=(12, 6)) 
    plot_series_present = []
    
    series_to_check = {
        "USD_Index_FRED_std": "USD Index (Std)",
        "US_2Y_Treasury_std": "2-Year Treasury Yield (Std)",
        "US_10Y_Treasury_std": "10-Year Treasury Yield (Std)"
    }

    for col, label in series_to_check.items():
        if col in standardized_df.columns and not standardized_df[col].isnull().all():
            plt.plot(standardized_df.index, standardized_df[col], label=label)
            plot_series_present.append(col)
        else:
            print(f"Plot 1: Data for '{label}' not available or all NaN.")

    if plot_series_present:
        plt.title("Standardized USD Index, 2-Year & 10-Year Treasury Yields")
        plt.xlabel("Date")
        plt.ylabel("Standardized Value (Z-score)")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()
    else:
        print("Plot 1: Not enough valid standardized data to generate the plot.")
        plt.close() 

def plot_treasury_spread(original_df):
    """Plots the 10Y-2Y Treasury yield spread using original values."""
    if original_df is None or original_df.empty:
        print("Plot 2: No original data to plot treasury spread.")
        return

    required_cols = ["US_10Y_Treasury", "US_2Y_Treasury"]
    if not all(col in original_df.columns for col in required_cols):
        print(f"Plot 2: Missing one or more required columns for spread: {required_cols}")
        return

    us_10y_orig = pd.to_numeric(original_df["US_10Y_Treasury"], errors='coerce')
    us_2y_orig = pd.to_numeric(original_df["US_2Y_Treasury"], errors='coerce')

    if us_10y_orig.isnull().all() or us_2y_orig.isnull().all():
        print("Plot 2: Original Treasury data is all NaN after numeric conversion.")
        return
        
    treasury_yield_diff = us_10y_orig - us_2y_orig
    
    plt.figure(figsize=(12, 6))
    plt.plot(original_df.index, treasury_yield_diff, label="10Y-2Y Treasury Yield Spread", color='purple')
    plt.title("Treasury Yield Difference (10-Year - 2-Year) Over Time (Original Values)")
    plt.xlabel("Date")
    plt.ylabel("Yield Difference (%)")
    plt.axhline(0, color='black', linestyle='--', linewidth=0.8) 
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def plot_standardized_fx_treasuries(standardized_df):
    """Plots standardized USD Index, Treasuries, and FX rates."""
    if standardized_df is None or standardized_df.empty:
        print("Plot 3: No standardized data to plot.")
        return

    plt.figure(figsize=(12, 7))
    plot_series_present = []
    
    series_to_check = {
        "USD_Index_FRED_std": "USD Index (Std)",
        "US_2Y_Treasury_std": "2Y Treasury (Std)",
        "US_10Y_Treasury_std": "10Y Treasury (Std)",
        "EUR_USD_FRED_std": "EUR/USD (Std)",
        "GBP_USD_FRED_std": "GBP/USD (Std)",
        "USD_PER_JPY_FRED_std": "USD/JPY (Std)"
    }

    for col, label in series_to_check.items():
        if col in standardized_df.columns and not standardized_df[col].isnull().all():
            plt.plot(standardized_df.index, standardized_df[col], label=label)
            plot_series_present.append(col)
        else:
            print(f"Plot 3: Data for '{label}' not available or all NaN.")
            
    if plot_series_present:
        plt.title("Standardized USD Index, Treasuries & FX Rates")
        plt.xlabel("Date")
        plt.ylabel("Standardized Value (Z-score)")
        plt.legend(loc='best')
        plt.grid(True)
        plt.tight_layout()
        plt.show()
    else:
        print("Plot 3: Not enough valid standardized data to generate the plot.")
        plt.close()

def plot_standardized_usd_treasuries_commodities(standardized_df):
    """Plots standardized USD Index, Treasuries, Brent & WTI Oil Prices on a single Y-axis."""
    if standardized_df is None or standardized_df.empty:
        print("Plot 4: No standardized data to plot.")
        return

    plt.figure(figsize=(12, 7))
    plot_series_present = []
    
    series_to_check = {
        "USD_Index_FRED_std": ("USD Index (Std)", "tab:red"),
        "US_10Y_Treasury_std": ("10Y Treasury (Std)", "tab:orange"),
        "US_2Y_Treasury_std": ("2Y Treasury (Std)", "tab:green"),
        "Brent_std": ("Brent Oil (Std)", "tab:blue"),
        "WTI_std": ("WTI Oil (Std)", "tab:cyan")
    }

    for col, (label, color_val) in series_to_check.items():
        if col in standardized_df.columns and not standardized_df[col].isnull().all():
            plt.plot(standardized_df.index, standardized_df[col], label=label, color=color_val)
            plot_series_present.append(col)
        else:
            print(f"Plot 4: Data for '{label}' not available or all NaN.")

    if plot_series_present:
        plt.title("Standardized USD Index, Treasuries, Brent & WTI Oil Prices")
        plt.xlabel("Date")
        plt.ylabel("Standardized Value (Z-score)")
        plt.legend(loc='best')
        plt.grid(True)
        plt.tight_layout()
        plt.show()
    else:
        print("Plot 4: Not enough valid standardized data to generate the plot.")
        plt.close()

# --- Main execution block ---
if __name__ == "__main__":
    print("Running T(s,a)_to_d.py directly for testing...")
    
    # Step 1: Fetch and store data
    success, file_path = fetch_and_store_data()

    if success and file_path:
        print(f"\nData fetching and storage successful. Data saved to: {file_path}")
        # Step 2: Load data and generate plots
        original_data, standardized_data_for_plots = load_and_prepare_data_for_plotting(file_path)
        
        if original_data is not None and standardized_data_for_plots is not None:
            print("\n--- Generating All Plots (Test Run) ---")
            plot_standardized_usd_treasuries(standardized_data_for_plots)
            plot_treasury_spread(original_data)
            plot_standardized_fx_treasuries(standardized_data_for_plots)
            plot_standardized_usd_treasuries_commodities(standardized_data_for_plots)
            print("\n--- All plots generated (Test Run). ---")
        else:
            print("Could not load or prepare data for plotting after fetching (Test Run).")
    else:
        print("\nData fetching and storage failed. Cannot proceed to plotting (Test Run).")
    
    print("\nDirect script execution of T(s,a)_to_d.py finished.")
