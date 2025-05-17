

from dotenv import load_dotenv
from time import sleep
import schwabdev
import datetime
import logging
import os
import json 

OUTPUT_DIRECTORY = "schwab_api_output"
#create out directory
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

def save_json_to_file(data, filename_prefix):
    """Helper function to save JSON data to a file in the output directory."""
    filepath = os.path.join(OUTPUT_DIRECTORY, f"{filename_prefix}.json")
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Successfully saved data to: {filepath}")
    except Exception as e:
        print(f"Error saving data to {filepath}: {e}")

def custom_data_retrieval_and_save():
    load_dotenv()  

    #warning if .env file is not found
    if not os.getenv('app_key') or len(os.getenv('app_key')) != 32 or \
       not os.getenv('app_secret') or len(os.getenv('app_secret')) != 16:
        raise Exception("Add your app key (32 chars) and app secret (16 chars) to the .env file.")
    if not os.getenv('callback_url'):
        raise Exception("Add your callback_url to the .env file (e.g., https://127.0.0.1).")

    # logging level
    logging.basicConfig(level=logging.WARNING) # WARNING to reduce noise, INFO for more detail

    #client
    client = schwabdev.Client(os.getenv('app_key'), os.getenv('app_secret'), os.getenv('callback_url'))
    print(f"Client created. Attempting to fetch data and save to '{OUTPUT_DIRECTORY}' directory...\n")

    # symbols
    equity_indices_symbols = {
        "S&P_500": "$SPX.X",
        "Dow_Jones": "$DJI.X",
        "NASDAQ_Composite": "$COMPX.X",
        "Russell_2000_Small_Cap": "$RUT.X"
    }
    treasury_indices_symbols = {
        "5-Year_Treasury_Note_Index": "$FVX.X",
        "10-Year_Treasury_Note_Index": "$TNX.X",
    }
    treasury_etf_symbols = {
        "Schwab_Short-Term_US_Treasury_ETF": "SCHO",
        "iShares_7-10_Year_Treasury_Bond_ETF": "IEF",
        "iShares_20_Plus_Year_Treasury_Bond_ETF": "TLT"
    }
    etf_symbols_of_interest = ["SPY"]
    stock_symbols_of_interest = ["AAPL", "MSFT", "NVDA", "META", "GOOGL", "JPM", "AMZN"]

    # --- 1. Retrieve Quotes for Indices ---
    print("--- Quotes for Equity Indices ---")
    all_index_symbols_list = list(equity_indices_symbols.values())
    if all_index_symbols_list:
        try:
            index_quotes_data = client.quotes(all_index_symbols_list).json()
            print(json.dumps(index_quotes_data, indent=2)) # Still print to console
            save_json_to_file(index_quotes_data, "equity_indices_quotes")
        except Exception as e:
            print(f"Error fetching equity index quotes: {e}")
    sleep(1)

    print("\n--- Quotes for Treasury Yield Indices/ETFs ---")
    all_treasury_symbols_list = list(treasury_indices_symbols.values()) + list(treasury_etf_symbols.values())
    if all_treasury_symbols_list:
        try:
            treasury_quotes_data = client.quotes(all_treasury_symbols_list).json()
            print(json.dumps(treasury_quotes_data, indent=2)) # Still print to console
            save_json_to_file(treasury_quotes_data, "treasury_indices_etfs_quotes")
        except Exception as e:
            print(f"Error fetching treasury index/ETF quotes: {e}")
    sleep(1)

    # --- 2. Retrieve Data for SPY ETF ---
    print(f"\n--- Data for ETF: SPY ---")
    spy_symbol = "SPY"
    try:
        print(f"\nQuote for {spy_symbol}:")
        spy_quote_data = client.quote(spy_symbol).json()
        print(json.dumps(spy_quote_data, indent=2))
        save_json_to_file(spy_quote_data, f"{spy_symbol}_quote")
        sleep(1)

        print(f"\nOption Expiration Chain for {spy_symbol}:")
        spy_exp_chain_data = client.option_expiration_chain(spy_symbol).json()
        print(json.dumps(spy_exp_chain_data, indent=2))
        save_json_to_file(spy_exp_chain_data, f"{spy_symbol}_option_expiration_chain")
        sleep(1)

        if spy_exp_chain_data and 'expirationList' in spy_exp_chain_data and spy_exp_chain_data['expirationList']:
            example_exp_date = spy_exp_chain_data['expirationList'][0]['expirationDate']
            # Format the expiration date to remove dashes and keep only the date part
            safe_example_exp_date = example_exp_date.split('T')[0].replace('-', '') 
            print(f"\nOption Chains for {spy_symbol} (Expiration: {example_exp_date}, Strike Count 2 for brevity):")
            spy_option_chains_data = client.option_chains(spy_symbol, strikeCount=2, toDate=example_exp_date, fromDate=example_exp_date).json()
            print(json.dumps(spy_option_chains_data, indent=2))
            save_json_to_file(spy_option_chains_data, f"{spy_symbol}_option_chain_{safe_example_exp_date}_limited")
        else:
            print(f"Could not retrieve expiration dates to fetch example option chain for {spy_symbol}.")
        sleep(1)

        print(f"\nPrice History (1 year) for {spy_symbol}:")
        spy_price_history_data = client.price_history(spy_symbol, periodType="year", period="1").json()
        print(json.dumps(spy_price_history_data, indent=2))
        save_json_to_file(spy_price_history_data, f"{spy_symbol}_price_history_1yr")
        sleep(1)

    except Exception as e:
        print(f"Error fetching data for {spy_symbol}: {e}")
        sleep(1)

    # --- 3. Retrieve Data for Stocks ---
    print(f"\n--- Data for Stocks: {', '.join(stock_symbols_of_interest)} ---")
    for symbol in stock_symbols_of_interest:
        print(f"\n--- {symbol} ---")
        try:
            print(f"Quote (includes volume) for {symbol}:")
            stock_quote_data = client.quote(symbol).json()
            print(json.dumps(stock_quote_data, indent=2))
            save_json_to_file(stock_quote_data, f"{symbol}_quote")
            sleep(1)

            print(f"\nInstrument Information (P/E, Div Yield, Market Cap) for {symbol}:")
            stock_instruments_data = client.instruments(symbol, "fundamental").json()
            print(json.dumps(stock_instruments_data, indent=2))
            save_json_to_file(stock_instruments_data, f"{symbol}_instruments_fundamental")
            sleep(1)

            print(f"\nOption Expiration Chain for {symbol}:")
            stock_exp_chain_data = client.option_expiration_chain(symbol).json()
            print(json.dumps(stock_exp_chain_data, indent=2))
            save_json_to_file(stock_exp_chain_data, f"{symbol}_option_expiration_chain")
            sleep(1)

            if stock_exp_chain_data and 'expirationList' in stock_exp_chain_data and stock_exp_chain_data['expirationList']:
                example_exp_date_stock = stock_exp_chain_data['expirationList'][0]['expirationDate']
                safe_example_exp_date_stock = example_exp_date_stock.split('T')[0].replace('-', '')
                print(f"\nOption Chains for {symbol} (Expiration: {example_exp_date_stock}, Strike Count 2 for brevity):")
                stock_option_chains_data = client.option_chains(symbol, strikeCount=2, toDate=example_exp_date_stock, fromDate=example_exp_date_stock).json()
                print(json.dumps(stock_option_chains_data, indent=2))
                save_json_to_file(stock_option_chains_data, f"{symbol}_option_chain_{safe_example_exp_date_stock}_limited")
            else:
                print(f"Could not retrieve expiration dates to fetch example option chain for {symbol}.")
            sleep(1)

            print(f"\nPrice History (1 year) for {symbol}:")
            stock_price_history_data = client.price_history(symbol, periodType="year", period="1").json()
            print(json.dumps(stock_price_history_data, indent=2))
            save_json_to_file(stock_price_history_data, f"{symbol}_price_history_1yr")
            sleep(1)

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            sleep(1)

    # --- 4. Retrieve Market Movers ---
    print("\n--- Market Movers ---")
    indices_for_movers = {
        "S&P_500": "$SPX.X",
        "Dow_Jones": "$DJI.X",
        "NASDAQ_Composite": "$COMPX.X"
    }
    for index_name, index_symbol in indices_for_movers.items():
        try:
            print(f"\nMovers for {index_name} ({index_symbol}):")
            plain_symbol = index_symbol.replace('.X', '')
            movers_data = client.movers(plain_symbol).json()
            print(json.dumps(movers_data, indent=2))
            save_json_to_file(movers_data, f"movers_{index_name}")
        except Exception as e:
            print(f"Error fetching movers for {index_name} ({index_symbol}): {e}")
        sleep(1)

if __name__ == '__main__':
    print("Welcome to the Schwabdev Custom Data Retriever with File Saving!")
    print("This script will fetch specific market data, quotes, and instrument info.")
    print(f"Output JSON files will be saved in the '{OUTPUT_DIRECTORY}' subdirectory.")
    print("Make sure your .env file is set up with APP_KEY, APP_SECRET, and CALLBACK_URL.")
    print("Documentation: https://tylerebowers.github.io/Schwabdev/")

    try:
        custom_data_retrieval_and_save()
        print(f"\nData retrieval and saving complete. Check the '{OUTPUT_DIRECTORY}' directory.")
    except Exception as e:
        print(f"\nAn error occurred during the process: {e}")
        print("Please check your .env file, API credentials, and internet connection.")