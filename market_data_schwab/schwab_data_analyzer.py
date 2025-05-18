# schwab_data_analyzer.py

from dotenv import load_dotenv
from time import sleep
import schwabdev 
import datetime
import logging
import os
import json
import pandas as pd 

# --- Configuration ---
OUTPUT_DIRECTORY = "schwab_api_output_options" 
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True) 

# --- Helper Functions ---
def _initialize_client():
    """Initializes and returns the Schwab API client."""
    load_dotenv()
    if not os.getenv('app_key') or len(os.getenv('app_key')) != 32 or \
       not os.getenv('app_secret') or len(os.getenv('app_secret')) != 16:
        print("CRITICAL: 'app_key' (32 chars) and 'app_secret' (16 chars) must be in the .env file.")
        return None
    if not os.getenv('callback_url'):
        print("CRITICAL: 'callback_url' must be in the .env file (e.g., https://127.0.0.1).")
        return None

    logging.basicConfig(level=logging.WARNING)
    try:
        client = schwabdev.Client(os.getenv('app_key'), os.getenv('app_secret'), os.getenv('callback_url'))
        print("Schwab client initialized successfully.")
        return client
    except Exception as e:
        print(f"CRITICAL: Failed to initialize Schwab client: {e}")
        return None

def save_json_to_file(data, filename_prefix, sub_directory=OUTPUT_DIRECTORY):
    """Helper function to save JSON data to a file in the specified subdirectory."""
    filepath = os.path.join(sub_directory, f"{filename_prefix}.json")
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Successfully saved: {filepath}")
    except Exception as e:
        print(f"ERROR saving to {filepath}: {e}")

# --- Data Fetching Functions ---

def fetch_price_history(client, symbol, filename_prefix,
                        period_type="year", period="1",
                        frequency_type="daily", frequency="1",
                        need_extended_hours=False):
    """Fetches and saves price history for a given symbol."""
    if not client: return None
    print(f"\nFetching Price History for {symbol} ({period} {period_type}, {frequency_type})...")
    try:
        history_data = client.price_history(
            symbol,
            periodType=period_type,
            period=period,
            frequencyType=frequency_type,
            frequency=frequency,
            needExtendedHoursData=str(need_extended_hours).lower() #
        ).json()
        save_json_to_file(history_data, f"{filename_prefix}_price_history")
        return history_data
    except Exception as e:
        print(f"ERROR fetching price history for {symbol}: {e}")
        return None

def fetch_options_data(client, symbol, filename_prefix, num_expirations=2, strike_count=5, contract_type="ALL"):
    """
    Fetches and saves option expiration chain and option chains for a given symbol.
    Args:
        client: Initialized Schwab client.
        symbol (str): The underlying symbol (e.g., SPY, $SPX.X, /ES).
        filename_prefix (str): Prefix for saved JSON files.
        num_expirations (int): Number of near-term expiration dates to fetch full chains for.
        strike_count (int): Number of strikes around the current price to fetch.
        contract_type (str): "CALL", "PUT", or "ALL".
    """
    if not client: return None, None
    print(f"\nFetching Option Expiration Chain for {symbol}...")
    exp_chain_data = None
    options_chains_data_all_exp = {}
    try:
        exp_chain_data = client.option_expiration_chain(symbol).json()
        save_json_to_file(exp_chain_data, f"{filename_prefix}_option_exp_chain")
    except Exception as e:
        print(f"ERROR fetching option expiration chain for {symbol}: {e}")
        return None, None 

    sleep(1) 

    if exp_chain_data and exp_chain_data.get('status') == 'SUCCESS' and 'expirationList' in exp_chain_data and exp_chain_data['expirationList']:
        print(f"Found {len(exp_chain_data['expirationList'])} expiration dates for {symbol}.")
        
        # Sort expirations to get near-term ones (API might not always return them sorted)
        # Assuming expirationDate is in a format sortable as string e.g. "2024-06-21T00:00:00"
        sorted_expirations = sorted(exp_chain_data['expirationList'], key=lambda x: x['expirationDate'])

        for i, exp_info in enumerate(sorted_expirations[:num_expirations]):
            exp_date_str = exp_info['expirationDate'].split('T')[0] # Format YYYY-MM-DD
            print(f"\nFetching Option Chain for {symbol} (Expiration: {exp_date_str}, Strike Count: {strike_count})...")
            try:
                chain_data = client.option_chains(
                    symbol,
                    strikeCount=strike_count,
                    contractType=contract_type,
                    fromDate=exp_date_str, 
                    toDate=exp_date_str  
                ).json()
                
                safe_exp_date_filename = exp_date_str.replace('-', '')
                save_json_to_file(chain_data, f"{filename_prefix}_option_chain_{safe_exp_date_filename}")
                options_chains_data_all_exp[exp_date_str] = chain_data
            except Exception as e:
                print(f"ERROR fetching option chain for {symbol}, Expiration {exp_date_str}: {e}")
            
            if i < num_expirations -1 : # Avoids unnecessary sleep after the last fetch
                 sleep(1)
    else:
        status = exp_chain_data.get('status', 'UNKNOWN') if exp_chain_data else 'UNKNOWN'
        error_msg = exp_chain_data.get('error', 'No expiration list found or request failed.') if exp_chain_data else 'No expiration list found.'
        print(f"WARNING: Could not retrieve valid expiration dates for {symbol}. Status: {status}. Message: {error_msg}")

    return exp_chain_data, options_chains_data_all_exp

def get_underlying_price_changes_df(price_history_json, symbol_name=""):
    """
    Processes price history JSON to a DataFrame with date, close price, and price change.
    Args:
        price_history_json (dict): JSON response from client.price_history.
        symbol_name (str): Name of the symbol for column naming.
    Returns:
        pandas.DataFrame or None
    """
    if not price_history_json or price_history_json.get('status') != 'SUCCESS' or not price_history_json.get('candles'):
        print(f"WARNING: Invalid or empty price history data for {symbol_name}.")
        return None

    candles = price_history_json['candles']
    df = pd.DataFrame(candles)
    df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
    df.set_index('datetime', inplace=True)
    df.sort_index(inplace=True) # Ensure data is sorted by date

    df[f'{symbol_name}_close'] = df['close']
    df[f'{symbol_name}_price_change'] = df['close'].diff()
    df[f'{symbol_name}_pct_change'] = df['close'].pct_change() * 100

    print(f"Processed price changes for {symbol_name}.")
    return df[[f'{symbol_name}_close', f'{symbol_name}_price_change', f'{symbol_name}_pct_change']]


# --- Main Orchestration Function ---
def fetch_all_spy_and_sp500_data(client):
    """
    Main function to fetch all requested SPY and S&P 500 data.
    """
    if not client:
        print("CRITICAL: Schwab client not available. Halting data fetching.")
        return

    print("\n=== Fetching data for SPY (ETF) ===")
    spy_price_history = fetch_price_history(client, "SPY", "SPY")
    sleep(1)
    spy_exp_chain, spy_options = fetch_options_data(client, "SPY", "SPY", num_expirations=2, strike_count=10)
    sleep(1)

    print("\n=== Fetching data for S&P 500 Index ($SPX.X) ===")
    spx_symbol = "$SPX.X" # Schwab symbol for S&P 500 Index
    spx_price_history = fetch_price_history(client, spx_symbol, "SPX_Index")
    sleep(1)
    spx_exp_chain, spx_options = fetch_options_data(client, spx_symbol, "SPX_Index", num_expirations=2, strike_count=10)
    sleep(1)

    print("\n=== Attempting to fetch data for E-mini S&P 500 Futures (/ES) ===")
    es_future_symbol_attempt1 = "/ES" # Common futures symbol
    es_future_symbol_attempt2 = "ES" # Alternative symbol for E-mini S&P 500 Futures

    es_price_history = fetch_price_history(client, es_future_symbol_attempt1, "ES_Future")
    if not es_price_history or es_price_history.get('status') != 'SUCCESS':
        print(f"NOTE: Price history fetch failed for {es_future_symbol_attempt1}, trying {es_future_symbol_attempt2}...")
        es_price_history = fetch_price_history(client, es_future_symbol_attempt2, "ES_Future")
    sleep(1)

    # This is an experimental attempt.
    print(f"Attempting option chain for {es_future_symbol_attempt1} (futures)...")
    es_exp_chain, es_options = fetch_options_data(client, es_future_symbol_attempt1, "ES_Future_Options", num_expirations=1, strike_count=5)
    if not es_exp_chain or not es_options: # If first attempt failed
        print(f"NOTE: Option fetch failed for {es_future_symbol_attempt1}, trying {es_future_symbol_attempt2}...")
        es_exp_chain, es_options = fetch_options_data(client, es_future_symbol_attempt2, "ES_Future_Options", num_expirations=1, strike_count=5)
    
    print("\n--- All requested data fetching attempts complete. ---")


# --- If script is run directly ---
if __name__ == '__main__':
    print("Schwab Data Analyzer - Focused Fetcher for SPY & S&P500 Options/Prices")
    print(f"Output JSON files will be saved in '{OUTPUT_DIRECTORY}'.")
    print("Ensure your .env file is correctly set up with Schwab API credentials.")
    
    client = _initialize_client()
    if client:
        try:
            fetch_all_spy_and_sp500_data(client)
            print(f"\nData retrieval attempts finished. Check the '{OUTPUT_DIRECTORY}' directory for JSON files.")
        except Exception as e:
            print(f"\nAn unexpected error occurred during the main process: {e}")
    else:
        print("Exiting due to client initialization failure.")

