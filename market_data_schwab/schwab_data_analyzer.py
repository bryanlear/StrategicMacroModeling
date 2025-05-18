# schwab_data_analyzer.py

from dotenv import load_dotenv
from time import sleep
import schwabdev 
import datetime
import logging
import os
import json
import pandas as pd
import numpy as np 
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# --- Configuration ---
OUTPUT_DIRECTORY = "schwab_api_output_options"
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
API_SLEEP_DURATION = 2

# --- Helper Functions ---
def _initialize_client():
    """Initializes and returns the Schwab API client."""
    load_dotenv()
    app_key = os.getenv('app_key')
    app_secret = os.getenv('app_secret')
    callback_url = os.getenv('callback_url')

    env_vars_ok = True
    if not app_key or len(app_key) != 32:
        print("CRITICAL: 'app_key' (32 chars) must be in the .env file.")
        env_vars_ok = False
    if not app_secret or len(app_secret) != 16:
        print("CRITICAL: 'app_secret' (16 chars) must be in the .env file.")
        env_vars_ok = False
    if not callback_url:
        print("CRITICAL: 'callback_url' must be in the .env file (e.g., https://127.0.0.1).")
        env_vars_ok = False
    if not env_vars_ok:
        return None

    logging.basicConfig(level=logging.INFO) 
    
    try:
        client = schwabdev.Client(app_key, app_secret, callback_url)
        print("INFO: Schwab client initialized successfully.")
        return client
    except Exception as e:
        print(f"CRITICAL: Failed to initialize Schwab client: {e}")
        return None

def save_json_to_file(data, filename_prefix, sub_directory=OUTPUT_DIRECTORY):
    """Helper function to save JSON data to a file."""
    filepath = os.path.join(sub_directory, f"{filename_prefix}.json")
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"INFO: Successfully saved: {filepath}")
    except Exception as e:
        print(f"ERROR: Could not save to {filepath}: {e}")

def _handle_api_response(response, operation_description="API Call"):
    """
    Handles Schwab API HTTP response, parses JSON, and checks for common error patterns.
    Returns parsed JSON data or a structured error dictionary.
    """
    response_data = {"schwab_api_status": "UNKNOWN_ERROR", "message": "Initial error state"} 
    try:
        if response.status_code != 200:
            err_msg = f"{operation_description} failed with HTTP status {response.status_code}."
            print(f"ERROR: {err_msg}")
            response_text = response.text
            try:
                error_details = response.json()
                print(f"Error details (JSON): {json.dumps(error_details, indent=2)}")
                response_data = {"schwab_api_status": "HTTP_ERROR", "http_status_code": response.status_code, "details": error_details, "message": err_msg}
            except json.JSONDecodeError:
                print(f"Response text (non-JSON, first 500 chars): {response_text[:500]}")
                response_data = {"schwab_api_status": "HTTP_ERROR", "http_status_code": response.status_code, "response_text": response_text[:500], "message": err_msg}
            return response_data 

        data = response.json()
        
        if isinstance(data, dict) and "status" in data and data["status"] != "SUCCESS":
            print(f"WARNING: {operation_description} returned API status '{data['status']}'. Message: {data.get('error', data.get('message', 'No specific error message.'))}")
            data["schwab_api_status"] = data.get("status", "API_REPORTED_NON_SUCCESS")
            return data
 
        if isinstance(data, dict):
            data["schwab_api_status"] = data.get("status", "SUCCESS_IMPLIED") 
        elif isinstance(data, list): 
            return {"schwab_api_status": "SUCCESS_IMPLIED", "data_list": data}
        return data

    except json.JSONDecodeError as e:
        err_msg = f"JSONDecodeError for {operation_description}. Response status: {response.status_code if hasattr(response, 'status_code') else 'N/A'}."
        print(f"ERROR: {err_msg} Details: {e}")
        response_text_attr = getattr(response, 'text', 'N/A')
        print(f"Response text (first 500 chars): {response_text_attr[:500]}")
        return {"schwab_api_status": "CLIENT_ERROR", "error_type": "JSONDecodeError", "message": str(e), "response_text": response_text_attr[:500]}
    except Exception as e:
        err_msg = f"Unexpected error handling response for {operation_description}."
        print(f"ERROR: {err_msg} Details: {e}")
        return {"schwab_api_status": "CLIENT_ERROR", "error_type": "UnexpectedResponseError", "message": str(e)}

# --- Data Fetching Functions ---
def fetch_price_history(client, symbol, filename_prefix,
                        period_type="year", period="2", 
                        frequency_type="daily", frequency="1",
                        need_extended_hours=False):
    if not client: return None
    operation_desc = f"Price History for {symbol} ({period} {period_type}, {frequency_type})"
    print(f"\nINFO: Fetching {operation_desc}...")
    history_data = None 
    try:
        response = client.price_history(
            symbol,
            periodType=period_type,
            period=str(period), 
            frequencyType=frequency_type,
            frequency=str(frequency), 
            needExtendedHoursData=str(need_extended_hours).lower()
        )
        history_data = _handle_api_response(response, operation_desc)
        save_json_to_file(history_data, f"{filename_prefix}_price_history_{period}yr")
        
        if isinstance(history_data, dict) and history_data.get("schwab_api_status", "").startswith("SUCCESS") and history_data.get("candles"):
            print(f"INFO: {operation_desc} fetched successfully.")
        elif isinstance(history_data, dict) and history_data.get("empty") is True: 
             print(f"INFO: {operation_desc} returned 'empty: true' (no candles for the period).")
        else: 
             print(f"WARNING: {operation_desc} did not return expected data structure or indicates failure. Review saved JSON. Status: {history_data.get('schwab_api_status', 'UNKNOWN') if isinstance(history_data, dict) else 'NOT_A_DICT'}")
        return history_data
    except Exception as e:
        print(f"ERROR: Exception during {operation_desc}: {e}")
        save_json_to_file({"schwab_api_status": "CLIENT_EXCEPTION", "error": str(e)}, f"{filename_prefix}_price_history_{period}yr_error")
        return {"schwab_api_status": "CLIENT_EXCEPTION", "error": str(e)} 

def fetch_options_data(client, symbol, filename_prefix, num_expirations=3, strike_count=25, contract_type="ALL", include_quotes="TRUE"):
    if not client: return None, {} 
    operation_desc_exp = f"Option Expiration Chain for {symbol}"
    print(f"\nINFO: Fetching {operation_desc_exp}...")
    exp_chain_data = None
    options_chains_data_all_exp = {} 
    
    try:
        response = client.option_expiration_chain(symbol)
        exp_chain_data = _handle_api_response(response, operation_desc_exp)
        save_json_to_file(exp_chain_data, f"{filename_prefix}_option_exp_chain")

        if not isinstance(exp_chain_data, dict) or exp_chain_data.get('schwab_api_status', "").startswith("HTTP_ERROR") or \
           exp_chain_data.get('status') == 'FAILED' or \
           not exp_chain_data.get('expirationList') or not isinstance(exp_chain_data['expirationList'], list):
            error_msg = exp_chain_data.get('error', exp_chain_data.get('message', 'No expiration list found or request failed.')) if isinstance(exp_chain_data, dict) else 'Request failed or no data.'
            status_msg = exp_chain_data.get('status', exp_chain_data.get('schwab_api_status', 'UNKNOWN')) if isinstance(exp_chain_data, dict) else 'UNKNOWN'
            print(f"WARNING: Could not retrieve valid expiration dates for {symbol} via {operation_desc_exp}. Status: {status_msg}. Message: {error_msg}")
            return exp_chain_data, options_chains_data_all_exp
    except Exception as e:
        print(f"ERROR: Exception during {operation_desc_exp}: {e}")
        save_json_to_file({"schwab_api_status": "CLIENT_EXCEPTION", "error": str(e)}, f"{filename_prefix}_option_exp_chain_error")
        return {"schwab_api_status": "CLIENT_EXCEPTION", "error": str(e)}, options_chains_data_all_exp

    sleep(API_SLEEP_DURATION)

    if exp_chain_data.get('expirationList'):
        print(f"INFO: Found {len(exp_chain_data['expirationList'])} expiration dates for {symbol}.")
        try:
            valid_expirations = [exp for exp in exp_chain_data['expirationList'] if isinstance(exp, dict) and exp.get('expirationDate')]
            sorted_expirations = sorted(valid_expirations, key=lambda x: x.get('expirationDate', ''))
        except Exception as sort_e:
            print(f"WARNING: Could not sort expiration dates for {symbol}, using original order. Error: {sort_e}")
            sorted_expirations = valid_expirations

        for i, exp_info in enumerate(sorted_expirations[:num_expirations]):
            exp_date_full = exp_info.get('expirationDate')
            if not exp_date_full:
                print(f"WARNING: Skipping expiration due to missing 'expirationDate' field in exp_info for {symbol}.")
                continue
            
            exp_date_str = exp_date_full.split('T')[0]
            operation_desc_chain = f"Option Chain for {symbol} (Exp: {exp_date_str}, Strikes: {strike_count}, Quotes: {include_quotes})"
            print(f"\nINFO: Fetching {operation_desc_chain}...")
            try:
                response_chain = client.option_chains(
                    symbol, strikeCount=strike_count, contractType=contract_type,
                    fromDate=exp_date_str, toDate=exp_date_str, includeQuotes=include_quotes
                )
                chain_data = _handle_api_response(response_chain, operation_desc_chain)
                
                safe_exp_date_filename = exp_date_str.replace('-', '')
                save_json_to_file(chain_data, f"{filename_prefix}_option_chain_{safe_exp_date_filename}")
                options_chains_data_all_exp[exp_date_str] = chain_data
                
                if isinstance(chain_data, dict) and chain_data.get("schwab_api_status","").startswith("SUCCESS") and \
                   (chain_data.get("callExpDateMap") or chain_data.get("putExpDateMap")):
                     print(f"INFO: {operation_desc_chain} fetched successfully.")
                else:
                     print(f"WARNING: {operation_desc_chain} might have failed or returned incomplete data. Review saved JSON. Status: {chain_data.get('schwab_api_status', 'UNKNOWN') if isinstance(chain_data, dict) else 'NOT_A_DICT'}")

            except Exception as e:
                print(f"ERROR: Exception during {operation_desc_chain}: {e}")
                save_json_to_file({"schwab_api_status": "CLIENT_EXCEPTION", "error": str(e)}, f"{filename_prefix}_option_chain_{exp_date_str.replace('-', '')}_error")
            
            if i < num_expirations - 1: sleep(API_SLEEP_DURATION)
    else:
        print(f"INFO: Skipping individual option chain fetches for {symbol} as expiration list was not successfully retrieved or was empty.")
    return exp_chain_data, options_chains_data_all_exp

def get_underlying_price_changes_df(price_history_json, symbol_name=""):
    if not isinstance(price_history_json, dict) or \
       price_history_json.get("schwab_api_status", "").startswith("HTTP_ERROR") or \
       price_history_json.get("schwab_api_status", "").startswith("CLIENT_ERROR") or \
       price_history_json.get('status') == 'FAILED' or \
       not price_history_json.get('candles'):
        print(f"WARNING: Invalid or empty price history data for {symbol_name} (API status: {price_history_json.get('schwab_api_status', price_history_json.get('status', 'N/A')) if isinstance(price_history_json, dict) else 'N/A'}). Cannot process.")
        return None

    candles = price_history_json['candles']
    if not isinstance(candles, list) or not candles:
        print(f"WARNING: 'candles' field is not a list or is empty for {symbol_name}.")
        return None
        
    df = pd.DataFrame(candles)
    if not all(col in df.columns for col in ['datetime', 'close']):
        print(f"WARNING: Price history DataFrame for {symbol_name} missing essential 'datetime' or 'close' columns.")
        return None
        
    df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
    df.set_index('datetime', inplace=True)
    df.sort_index(inplace=True)
    df[f'{symbol_name}_close'] = pd.to_numeric(df['close'], errors='coerce')
    df[f'{symbol_name}_price_change'] = df[f'{symbol_name}_close'].diff()
    df[f'{symbol_name}_pct_change'] = df[f'{symbol_name}_close'].pct_change() * 100
    output_cols = [f'{symbol_name}_close', f'{symbol_name}_price_change', f'{symbol_name}_pct_change']
    if 'volume' in df.columns:
        df[f'{symbol_name}_volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype(np.int64)
        output_cols.append(f'{symbol_name}_volume')
    else:
        print(f"INFO: Volume data not found in price history for {symbol_name}.")
    print(f"INFO: Processed price changes and volume (if available) for {symbol_name}.")
    return df[output_cols]

def fetch_major_indices_data(client):
    if not client: return
    major_indices_symbols = {
        "Dow_Jones_Industrial_Average": "$DJI.X",
        "NASDAQ_Composite": "$COMPX.X",
        "Russell_2000_Small_Cap": "$RUT.X"
    }
    print("\n=== Fetching data for Major Market Indices ===")
    for name, symbol in major_indices_symbols.items():
        filename_prefix = name.replace(" ", "_").replace("/", "_").replace("$","").replace(".","_")
        operation_desc_quote = f"Quote for {name} ({symbol})"
        print(f"\n--- {name} ({symbol}) ---")
        try:
            response = client.quotes(symbol) 
            quote_data = _handle_api_response(response, operation_desc_quote)
            save_json_to_file(quote_data, f"{filename_prefix}_quote")
            
            if isinstance(quote_data, dict) and symbol in quote_data and \
               isinstance(quote_data[symbol], dict) and 'quote' in quote_data[symbol]:
                 print(f"INFO: {operation_desc_quote} fetched successfully.")
            elif isinstance(quote_data, dict) and "schwab_api_status" in quote_data:
                pass 
            else:
                 print(f"WARNING: {operation_desc_quote} data might be empty or incomplete. Review saved JSON.")
        except Exception as e:
            print(f"ERROR: Exception during {operation_desc_quote}: {e}")
        sleep(API_SLEEP_DURATION)
        fetch_price_history(client, symbol, filename_prefix, period="2", period_type="year")
        sleep(API_SLEEP_DURATION)

def fetch_all_market_data(client):
    if not client:
        print("CRITICAL: Schwab client not available. Halting data fetching.")
        return

    print("\n=== Fetching data for SPY (ETF) ===")
    fetch_price_history(client, "SPY", "SPY", period="2", period_type="year")
    sleep(API_SLEEP_DURATION)
    fetch_options_data(client, "SPY", "SPY", num_expirations=3, strike_count=25, include_quotes="TRUE")
    sleep(API_SLEEP_DURATION)

    print("\n=== Fetching data for S&P 500 Index ($SPX.X) ===")
    spx_symbol = "$SPX.X"
    fetch_price_history(client, spx_symbol, "SPX_Index", period="2", period_type="year")
    sleep(API_SLEEP_DURATION)
    fetch_options_data(client, spx_symbol, "SPX_Index", num_expirations=3, strike_count=25, include_quotes="TRUE")
    sleep(API_SLEEP_DURATION)

    print("\n=== Attempting to fetch data for E-mini S&P 500 Futures (/ES or ES) ===")
    es_future_symbols_to_try = ["/ES", "ES"] 
    successful_es_symbol_for_history = None
    for es_symbol in es_future_symbols_to_try:
        print(f"INFO: Attempting price history for E-mini S&P 500 Futures with symbol: {es_symbol}")
        es_filename_symbol_part = es_symbol.replace('/', 'SLASH') 
        es_history = fetch_price_history(client, es_symbol, f"ES_Future_{es_filename_symbol_part}", period="2", period_type="year")
        if es_history and isinstance(es_history, dict) and \
           es_history.get("schwab_api_status", "").startswith("SUCCESS") and es_history.get("candles"):
            successful_es_symbol_for_history = es_symbol
            print(f"INFO: Successfully fetched price history for {es_symbol}.")
            break 
        else:
            print(f"WARNING: Price history fetch failed or returned no data for {es_symbol}.")
        sleep(API_SLEEP_DURATION) 
    
    if successful_es_symbol_for_history:
        print(f"\nINFO: Attempting option chain for E-mini S&P 500 Futures using successful history symbol: {successful_es_symbol_for_history} (experimental)...")
        es_filename_symbol_part = successful_es_symbol_for_history.replace('/', 'SLASH')
        fetch_options_data(client, successful_es_symbol_for_history, f"ES_Future_{es_filename_symbol_part}_Options", 
                           num_expirations=1, strike_count=10, include_quotes="TRUE")
    else:
        print("WARNING: No successful price history for any ES futures symbols tried, skipping options attempt for ES.")
    sleep(API_SLEEP_DURATION)
    
    fetch_major_indices_data(client)
    print("\n--- All requested data fetching attempts complete. ---")

# --- Plotting Functions (remain the same as previous interactive version) ---
def plot_price_and_volume(df, symbol_col_prefix, plot_title_prefix):
    if df is None or df.empty:
        print(f"Plotting: No data to plot for {plot_title_prefix}.")
        return None
    close_col = f'{symbol_col_prefix}_close'
    volume_col = f'{symbol_col_prefix}_volume'
    if close_col not in df.columns:
        print(f"Plotting: Closing price column '{close_col}' not found for {plot_title_prefix}.")
        return None

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.7, 0.3],
                        subplot_titles=(f"{plot_title_prefix} Price", f"{plot_title_prefix} Volume"))
    fig.add_trace(go.Scatter(x=df.index, y=df[close_col], name='Close Price'), row=1, col=1)
    
    volume_plotted = False
    if volume_col in df.columns and not df[volume_col].isnull().all():
        fig.add_trace(go.Bar(x=df.index, y=df[volume_col], name='Volume', marker_color='rgba(100,149,237,0.7)'), row=2, col=1)
        fig.update_yaxes(title_text="Volume", row=2, col=1)
        volume_plotted = True
    else:
        print(f"INFO: Volume data not plotted for {plot_title_prefix} (missing or all NaN).")
        fig.layout.yaxis2.visible = False 
        fig.layout.xaxis2.title.text = "" 

    fig.update_layout(title_text=f'{plot_title_prefix} - Price and Volume (2 Years)',
                      xaxis_title='Date' if not volume_plotted else None, 
                      xaxis2_title='Date' if volume_plotted else None,
                      yaxis_title='Price / Level',
                      legend_title_text='Metric',
                      showlegend=True,
                      height=600 if volume_plotted else 400,
                      xaxis_rangeslider_visible=False)
    fig.update_yaxes(title_text="Price / Level", row=1, col=1)
    return fig

def display_sample_options_data_text(filename_prefix, underlying_symbol, output_dir=OUTPUT_DIRECTORY):
    """Prints a textual summary of sample options data from saved JSON."""
    print(f"\n--- Sample Options Data for {underlying_symbol} ({filename_prefix}) ---")
    exp_chain_filepath = os.path.join(output_dir, f"{filename_prefix}_option_exp_chain.json")
    exp_date_to_load_yyyymmdd = None
    exp_date_to_load_yyyy_mm_dd = None
    
    try:
        if not os.path.exists(exp_chain_filepath):
            print(f"WARNING: Expiration chain file not found: {exp_chain_filepath}")
            return
        with open(exp_chain_filepath, 'r') as f:
            exp_chain_json = json.load(f)
        
        if isinstance(exp_chain_json, dict) and \
           exp_chain_json.get("schwab_api_status", "").startswith("SUCCESS") and \
           exp_chain_json.get('expirationList'):
            valid_expirations = [exp for exp in exp_chain_json['expirationList'] if isinstance(exp, dict) and exp.get('expirationDate')]
            if valid_expirations:
                sorted_expirations = sorted(valid_expirations, key=lambda x: x['expirationDate'])
                exp_date_full = sorted_expirations[0]['expirationDate'] 
                exp_date_to_load_yyyy_mm_dd = exp_date_full.split('T')[0]
                exp_date_to_load_yyyymmdd = exp_date_to_load_yyyy_mm_dd.replace('-', '')
            else:
                print(f"WARNING: No valid expiration entries found in chain for {underlying_symbol}.")
                return 
        else:
            print(f"WARNING: Expiration chain data for {underlying_symbol} is invalid or indicates failure. Status: {exp_chain_json.get('schwab_api_status', 'N/A') if isinstance(exp_chain_json, dict) else 'N/A'}")
            return 
            
    except Exception as e:
        print(f"ERROR loading or processing expiration chain {exp_chain_filepath}: {e}")
        return

    if not exp_date_to_load_yyyymmdd:
        print(f"Could not determine a valid example expiration date for {underlying_symbol}. Cannot load sample option chain.")
        return

    option_chain_filepath = os.path.join(output_dir, f"{filename_prefix}_option_chain_{exp_date_to_load_yyyymmdd}.json")
    error_option_chain_filepath = os.path.join(output_dir, f"{filename_prefix}_option_chain_{exp_date_to_load_yyyymmdd}_error.json")
    
    if os.path.exists(error_option_chain_filepath):
        print(f"ERROR: Error file found for option chain: {error_option_chain_filepath}. Skipping display.")
        return
    if not os.path.exists(option_chain_filepath):
        print(f"WARNING: Option chain file not found: {option_chain_filepath}")
        return

    try:
        with open(option_chain_filepath, 'r') as f:
            options_json = json.load(f)
        print(f"INFO: Displaying sample from: {option_chain_filepath} for Expiration: {exp_date_to_load_yyyy_mm_dd}")
        
        if isinstance(options_json, dict) and options_json.get("schwab_api_status", "").startswith("SUCCESS"):
            print(f"  Underlying Price: {options_json.get('underlyingPrice', 'N/A')}")
            underlying_details = options_json.get('underlying', {})
            if isinstance(underlying_details, dict):
                 print(f"  Underlying Details: Mark={underlying_details.get('mark', 'N/A')}, Change={underlying_details.get('change', 'N/A')}, PctChange={underlying_details.get('percentChange', 'N/A')}%")
            
            for contract_map_key, contract_type_label in [('callExpDateMap', 'Call'), ('putExpDateMap', 'Put')]:
                if contract_map_key in options_json and isinstance(options_json[contract_map_key], dict) and options_json[contract_map_key]:
                    date_keys = list(options_json[contract_map_key].keys())
                    actual_exp_date_key_in_map = None
                    for key_in_map in date_keys:
                        if exp_date_to_load_yyyy_mm_dd in key_in_map:
                            actual_exp_date_key_in_map = key_in_map
                            break
                    if not actual_exp_date_key_in_map:
                        print(f"  WARNING: Could not find matching expiration date key for {exp_date_to_load_yyyy_mm_dd} in {contract_type_label} map. Available keys: {date_keys}")
                        continue
                    contracts_for_exp = options_json[contract_map_key][actual_exp_date_key_in_map]
                    if not isinstance(contracts_for_exp, dict):
                        print(f"  WARNING: Contracts data for {actual_exp_date_key_in_map} is not in expected format (dict of strikes).")
                        continue

                    print(f"\n  Sample {contract_type_label} Options (Expiration: {exp_date_to_load_yyyy_mm_dd}):")
                    count = 0
                    try:
                        sorted_strikes = sorted(contracts_for_exp.keys(), key=lambda k: float(str(k).replace(',',''))) 
                    except ValueError:
                        print("  WARNING: Could not sort strikes numerically, using original order.")
                        sorted_strikes = list(contracts_for_exp.keys())

                    for strike in sorted_strikes[:5]: 
                        details_list = contracts_for_exp.get(strike) 
                        if isinstance(details_list, list) and details_list:
                            for details in details_list[:1]: 
                                print(f"    Strike: {str(strike):<8} Last: {details.get('last', 'N/A'):<7} Bid: {details.get('bid', 'N/A'):<7} Ask: {details.get('ask', 'N/A'):<7} IV: {details.get('volatility', 'N/A'):<6} Delta: {details.get('delta', 'N/A'):<6}")
                                count += 1
                    if count == 0: print(f"    No {contract_type_label.lower()} options found in this sample for this expiration.")
        elif isinstance(options_json, dict): 
            error_msg = options_json.get('error', options_json.get('message', 'Unknown issue or non-SUCCESS status'))
            print(f"WARNING: Option chain data for {underlying_symbol} ({exp_date_to_load_yyyy_mm_dd}) indicates failure or no data: {error_msg}. Status: {options_json.get('schwab_api_status', 'N/A')}")
        else: 
            print(f"WARNING: Option chain data for {underlying_symbol} ({exp_date_to_load_yyyy_mm_dd}) is not in the expected dictionary format.")
    except FileNotFoundError:
        print(f"WARNING: Option chain file not found: {option_chain_filepath}")
    except Exception as e:
        print(f"ERROR loading or displaying options data from {option_chain_filepath}: {e}")

# --- Main execution block ---
if __name__ == '__main__':
    print("Schwab Data Analyzer - v4: Refined API Handling, Options, Prices, Volume, Indices")
    print(f"Output JSON files will be saved in '{OUTPUT_DIRECTORY}'.")
    
    client = _initialize_client()
    if client:
        try:
            fetch_all_market_data(client)
            print(f"\nData retrieval attempts finished. Check the '{OUTPUT_DIRECTORY}' directory.")
            
            print("\n--- Testing Plot Generation (SPY) if data was fetched ---")
            spy_history_file = os.path.join(OUTPUT_DIRECTORY, "SPY_price_history_2yr.json")
            if os.path.exists(spy_history_file):
                with open(spy_history_file, 'r') as f_spy_test:
                    spy_history_json_test = json.load(f_spy_test)
                spy_df_test = get_underlying_price_changes_df(spy_history_json_test, "SPY_Test")
                if spy_df_test is not None:
                    fig_test_spy = plot_price_and_volume(spy_df_test, "SPY_Test", "SPY Test Plot")
                    if fig_test_spy:
                        print("Displaying SPY test plot in browser...")
                        fig_test_spy.show()
            else:
                print(f"Test: SPY price history file not found at {spy_history_file} for plotting test.")

            print("\n--- Testing Options Data Display (SPY) if data was fetched ---")
            display_sample_options_data_text("SPY", "SPY ETF")

        except Exception as e:
            print(f"\nAn unexpected error occurred during the main process of the test run: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("Exiting due to client initialization failure.")
    print("\nDirect script execution of schwab_data_analyzer.py finished.")
