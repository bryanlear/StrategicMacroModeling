import pandas_datareader.data as pdr
import pandas as pd
import datetime
import os
from dotenv import load_dotenv

load_dotenv()
FRED_API_KEY = os.getenv('FRED_API_KEY')

OUTPUT_DIR = "data/raw/US_indicators"
MARKET_SENTIMENT_INDICATORS_CSV_FILE = os.path.join(OUTPUT_DIR, 'us_market_sentiment_indicators.csv')
MARKET_SENTIMENT_INDICATORS_PARQUET_FILE = os.path.join(OUTPUT_DIR, 'us_market_sentiment_indicators.parquet')

if FRED_API_KEY:
    os.environ['FRED_API_KEY'] = FRED_API_KEY
else:
    print("Warning: FRED_API_KEY not found. Data fetching might be limited or fail.")

END_DATE = datetime.datetime.now()
START_DATE = END_DATE - datetime.timedelta(days=5 * 365) # Default to 5 years

series_map_sentiment = {
    "NASDAQ Composite Index": "NASDAQCOM",
    "S&P 500": "SP500",
    "Dow Jones Industrial Average": "DJIA",
    "CBOE Volatility Index VIX": "VIXCLS",
    "CBOE Equity VIX on Goldman Sachs": "VXGSCLS",
    "Moody's Baa Corp Bond Yield Relative to 10Y Treasury": "BAA10Y",
    "Moody's Aaa Corp Bond Yield Relative to 10Y Treasury": "AAA10Y",
    "Chicago Fed National Financial Conditions Index": "NFCI",
    "St. Louis Fed Financial Stress Index": "STLFSI4",
    "University of Michigan Consumer Sentiment": "UMCSENT",
    "Composite Business Confidence Amplitude Adjusted US": "BSCICP03USM665S",
    "Equity Market Volatility Tracker Macro Business Investment Sentiment": "EMVMACROBUS",
    "Chicago Fed National Financial Conditions Risk Subindex": "NFCIRISK"
}

def fetch_fred_data(series_dict, start_date, end_date):
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

    combined_df = pd.concat(df_list, axis=1, join='outer')
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
    if not FRED_API_KEY:
        print("FRED_API_KEY is not set. Please set it in your .env file or environment variables.")
    else:
        print("Fetching US Market Sentiment & Stability Economic Indicators from FRED...")
        us_market_sentiment_indicators_df = fetch_fred_data(series_map_sentiment, START_DATE, END_DATE)

        if not us_market_sentiment_indicators_df.empty:
            print("\n--- Combined US Market Sentiment & Stability Indicators Data (Last 5 entries) ---")
            print(us_market_sentiment_indicators_df.tail())
            print("\n--- Combined US Market Sentiment & Stability Indicators Data (First 5 entries) ---")
            print(us_market_sentiment_indicators_df.head())
            print(f"\nShape of the DataFrame: {us_market_sentiment_indicators_df.shape}")
            
            store_data(us_market_sentiment_indicators_df, MARKET_SENTIMENT_INDICATORS_CSV_FILE, MARKET_SENTIMENT_INDICATORS_PARQUET_FILE)
        else:
            print("Failed to retrieve any data. Nothing to store.")
