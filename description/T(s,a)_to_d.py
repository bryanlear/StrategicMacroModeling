# T(s,a)_to_d.py

import yfinance as yf
import pandas_datareader.data as pdr
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots 
import datetime
import os
import requests
from dotenv import load_dotenv
import numpy as np

# --- Configuration ---
LOCAL_DATA_FILE = 'combined_data.txt'
load_dotenv()
FRED_API_KEY = os.getenv('FRED_API_KEY')
DATA_GOV_API_KEY = os.getenv('DATA_GOV_API_KEY')
DATA_GOV_SEARCH_URL = "https://catalog.data.gov/api/3/action/package_search"
END_DATE = datetime.datetime.now()
START_DATE = END_DATE - datetime.timedelta(days=2 * 365)

YF_TICKERS_MAP = {}
FRED_GENERAL_TICKERS_MAP = {
    "Federal_Funds_Rate": "DFF",
    "US_2Y_Treasury": "DGS2",
    "US_10Y_Treasury": "DGS10",
    "USD_Index_FRED": "DTWEXBGS",
    "EUR_USD_FRED": "DEXUSEU",
    "GBP_USD_FRED": "DEXUSUK",
    "JPY_PER_USD_FRED": "DEXJPUS"
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
    if FRED_API_KEY == '12345':
        print(f"INFO: Using placeholder FRED API Key: '{FRED_API_KEY}'")
    else:
        print("INFO: FRED API Key loaded.")
    if DATA_GOV_API_KEY:
        print("INFO: Data.gov API Key loaded.")
    else:
        print("INFO: Data.gov API Key not found. Searches will be made without an API key.")

def search_data_gov(query):
    print(f"\nINFO: Attempting to search data.gov for '{query}' as a fallback...")
    params = {'q': query, 'rows': 3}
    headers = {}
    try:
        response = requests.get(DATA_GOV_SEARCH_URL, params=params, headers=headers, timeout=10)
        response.raise_for_status()
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
                print(f"  {i+1}. Title: {title}\n     Organization: {org_title}\n     Example Resource URL: {resource_url}\n     Data.gov link: {dataset_link}")
        else:
            print(f"INFO: No datasets found on data.gov for '{query}'.")
    except Exception as e:
        print(f"ERROR: Could not connect or search data.gov for '{query}': {e}")
    print("INFO: Manual investigation of data.gov results would be required to integrate them.")

def standardize_series(series):
    series_numeric = pd.to_numeric(series, errors='coerce')
    mean = series_numeric.mean()
    std_dev = series_numeric.std(ddof=0)
    if std_dev == 0 or pd.isna(std_dev):
        print(f"WARNING: Could not standardize series '{series.name}' (std is 0 or NaN). Returning NaNs.")
        return pd.Series(np.nan, index=series_numeric.index, name=series.name + "_std")
    return (series_numeric - mean) / std_dev

# --- Core Data Fetching and Processing ---
def fetch_and_store_data(data_file_path=LOCAL_DATA_FILE, start_date_param=START_DATE, end_date_param=END_DATE):
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
                    print(f"WARNING: No data for {name} ({ticker}) from Yahoo Finance.")
                    search_data_gov(f"{name} financial data")
            except Exception as e:
                print(f"ERROR: Fetching {name} ({ticker}) from Yahoo Finance failed: {e}")
                search_data_gov(f"{name} financial data")
    else:
        print("INFO: No tickers specified for Yahoo Finance.")

    print("\nFetching data from FRED...")
    for name, series_id in ALL_FRED_TICKERS.items():
        try:
            data = pdr.get_data_fred(series_id, start=start_date_param, end=end_date_param)
            if not data.empty and not data[series_id].isnull().all():
                all_series_data[name] = data[series_id]
                print(f"Successfully fetched {name} ({series_id}) from FRED.")
            else:
                print(f"WARNING: No data for {name} ({series_id}) from FRED.")
                search_data_gov(f"{name} FRED series {series_id}")
        except Exception as e:
            print(f"ERROR: Fetching {name} ({series_id}) from FRED failed: {e}")
            search_data_gov(f"{name} FRED series {series_id}")

    if not all_series_data:
        print("CRITICAL ERROR: No data fetched. Cannot proceed.")
        return False, None

    combined_data = pd.DataFrame(all_series_data)
    if combined_data.empty:
        print("CRITICAL ERROR: Combined data is empty. Cannot proceed.")
        return False, None
        
    combined_data.index = pd.to_datetime(combined_data.index)
    if "JPY_PER_USD_FRED" in combined_data.columns:
        jpy_series = pd.to_numeric(combined_data["JPY_PER_USD_FRED"], errors='coerce')
        combined_data["USD_PER_JPY_FRED"] = np.where(
            jpy_series.notna() & (jpy_series != 0), 1 / jpy_series, np.nan)
        print("INFO: Calculated USD_PER_JPY_FRED.")
    else:
        print("WARNING: JPY_PER_USD_FRED not found.")
    combined_data = combined_data.ffill().dropna(how='all')
    if combined_data.empty:
        print("CRITICAL ERROR: Combined data empty after cleaning. Cannot save.")
        return False, None
    try:
        combined_data.to_csv(data_file_path)
        print(f"\n--- Data successfully fetched and saved to '{data_file_path}' ---")
        return True, data_file_path
    except Exception as e:
        print(f"CRITICAL ERROR: Saving data to CSV '{data_file_path}' failed: {e}")
        return False, None

def load_and_prepare_data_for_plotting(data_file_path=LOCAL_DATA_FILE):
    print(f"\n--- Reading data from '{data_file_path}' for plotting ---")
    try:
        all_data = pd.read_csv(data_file_path, index_col=0, parse_dates=True)
        if all_data.empty:
            print("ERROR: Loaded data is empty.")
            return None, None
        print("Data loaded successfully. Standardizing relevant series...")
    except FileNotFoundError:
        print(f"ERROR: Data file '{data_file_path}' not found. Please run data fetching first.")
        return None, None
    except Exception as e:
        print(f"ERROR: Reading data from CSV '{data_file_path}' failed: {e}")
        return None, None

    cols_for_standardization = [
        "USD_Index_FRED", "US_2Y_Treasury", "US_10Y_Treasury",
        "EUR_USD_FRED", "GBP_USD_FRED", "USD_PER_JPY_FRED",
        "Brent", "WTI", "Federal_Funds_Rate"
    ]
    standardized_data = pd.DataFrame(index=all_data.index)
    for col in cols_for_standardization:
        if col in all_data.columns:
            standardized_series_data = standardize_series(all_data[col])
            standardized_data[col + "_std"] = standardized_series_data
        else:
            print(f"WARNING: Column {col} not found in loaded data for standardization.")
            
        standardized_data = standardized_data.dropna(axis=1, how='all')

    if standardized_data.empty:
        print("WARNING: Standardized data is empty after processing. No standardized plots can be generated.")
        
    return all_data, standardized_data


# --- Plotting Functions ---

def plot_standardized_usd_treasuries(standardized_df):
    """Plots standardized USD Index and Treasury yields using Plotly."""
    if standardized_df is None or standardized_df.empty:
        print("Plot 1: No standardized data to plot.")
        return None
    
    cols_to_plot = []
    labels_map = {}
    series_to_check = {
        "USD_Index_FRED_std": "USD Index (Std)",
        "US_2Y_Treasury_std": "2-Year Treasury Yield (Std)",
        "US_10Y_Treasury_std": "10-Year Treasury Yield (Std)"
    }
    for col, label in series_to_check.items():
        if col in standardized_df.columns and not standardized_df[col].isnull().all():
            cols_to_plot.append(col)
            labels_map[col] = label
        else:
            print(f"Plot 1: Data for '{label}' not available or all NaN.")

    if not cols_to_plot:
        print("Plot 1: Not enough valid standardized data to generate the plot.")
        return None

    fig = px.line(standardized_df,
                  y=cols_to_plot,
                  title="Standardized USD Index, 2-Year & 10-Year Treasury Yields",
                  labels={"value": "Standardized Value (Z-score)", "date": "Date", **labels_map})
    fig.update_layout(legend_title_text='Series')
    return fig

def plot_treasury_spread(original_df):
    """Plots the 10Y-2Y Treasury yield spread using Plotly (original values)."""
    if original_df is None or original_df.empty:
        print("Plot 2: No original data to plot treasury spread or DFF.")
        return None

    # Define the required columns for the plot
    required_cols = ["US_10Y_Treasury", "US_2Y_Treasury", "Federal_Funds_Rate"] # Use Federal_Funds_Rate as per ALL_FRED_TICKERS

    # Check if all required columns are present in the DataFrame
    if not all(col in original_df.columns for col in required_cols):
        print(f"Plot 2: Missing one or more required columns for spread and DFF: {required_cols}")
        return None

    # Convert the relevant columns to numeric, coercing any errors to NaN
    us_10y_orig = pd.to_numeric(original_df["US_10Y_Treasury"], errors='coerce')
    us_2y_orig = pd.to_numeric(original_df["US_2Y_Treasury"], errors='coerce')
    dff_orig = pd.to_numeric(original_df["Federal_Funds_Rate"], errors='coerce') # Use Federal_Funds_Rate column name

    # Check if all values in the converted series are NaN for either series
    if us_10y_orig.isnull().all() or us_2y_orig.isnull().all():
        print("Plot 2: Original Treasury data (10Y or 2Y) is all NaN after numeric conversion. Cannot plot spread.")
        return None
    if dff_orig.isnull().all():
        print("Plot 2: Original Fed Funds Rate (DFF) data is all NaN after numeric conversion. Cannot plot DFF.")
        # We can still plot the spread if DFF is NaN, but the user asked for both.
        # For now, if DFF is all NaN, return None as the plot won't be complete as requested.
        return None
        
    treasury_yield_diff = us_10y_orig - us_2y_orig
    
    # Create the figure with a secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add the Treasury Yield Spread trace to the primary y-axis
    fig.add_trace(
        go.Scatter(x=original_df.index, y=treasury_yield_diff, name='Yield Spread (10Y - 2Y)', mode='lines', line=dict(color='blue')),
        secondary_y=False,
    )

    # Add the Fed Funds Rate (DFF) trace to the secondary y-axis
    fig.add_trace(
        go.Scatter(x=original_df.index, y=dff_orig, name='Fed Funds Rate (DFF)', mode='lines', line=dict(color='red')),
        secondary_y=True,
    )

    # Add a horizontal line at y=0 for the yield spread on the primary y-axis
    fig.add_hline(y=0, line_dash="dash", line_color="black", annotation_text="0 Spread", annotation_position="bottom right", secondary_y=False)

    # Update layout and titles
    fig.update_layout(
        title_text="Treasury Yield Difference (10-Year - 2-Year) and Fed Funds Rate (Original Values)",
        showlegend=True,
        legend_title_text='Series',
        hovermode="x unified" # Improves hover experience for multiple traces
    )

    # Set y-axis titles
    fig.update_yaxes(title_text="Yield Difference (%)", secondary_y=False, title_font=dict(color="blue"))
    fig.update_yaxes(title_text="Fed Funds Rate (%)", secondary_y=True, title_font=dict(color="red"))
    fig.update_xaxes(title_text="Date")

    return fig

def plot_standardized_fx_treasuries(standardized_df):
    """Plots standardized USD Index, Treasuries, and FX rates using Plotly."""
    if standardized_df is None or standardized_df.empty:
        print("Plot 3: No standardized data to plot.")
        return None
    
    cols_to_plot = []
    labels_map = {}
    series_to_check = {
        "Federal_Funds_Rate_std": "Federal Funds Rate (Std)",
        "USD_Index_FRED_std": "USD Index (Std)",
        "US_2Y_Treasury_std": "2Y Treasury (Std)",
        "US_10Y_Treasury_std": "10Y Treasury (Std)",
        "EUR_USD_FRED_std": "EUR/USD (Std)",
        "GBP_USD_FRED_std": "GBP/USD (Std)",
        "USD_PER_JPY_FRED_std": "USD/JPY (Std)"
    }
    for col, label in series_to_check.items():
        if col in standardized_df.columns and not standardized_df[col].isnull().all():
            cols_to_plot.append(col)
            labels_map[col] = label
        else:
            print(f"Plot 3: Data for '{label}' not available or all NaN.")
            
    if not cols_to_plot:
        print("Plot 3: Not enough valid standardized data to generate the plot.")
        return None
        
    fig = px.line(standardized_df,
                  y=cols_to_plot,
                  title="Standardized USD Index, Treasuries & FX Rates",
                  labels={"value": "Standardized Value (Z-score)", "date": "Date", **labels_map})
    fig.update_layout(legend_title_text='Series')
    return fig

def plot_standardized_usd_treasuries_commodities(standardized_df):
    """Plots standardized USD Index, Treasuries, Brent & WTI Oil Prices using Plotly."""
    if standardized_df is None or standardized_df.empty:
        print("Plot 4: No standardized data to plot.")
        return None
        
    cols_to_plot = []
    labels_map = {}
    colors_map = {} 

    series_to_check = {
        "USD_Index_FRED_std": ("USD Index (Std)", "red"),
        "US_10Y_Treasury_std": ("10Y Treasury (Std)", "orange"),
        "US_2Y_Treasury_std": ("2Y Treasury (Std)", "green"),
        "Brent_std": ("Brent Oil (Std)", "blue"),
        "WTI_std": ("WTI Oil (Std)", "cyan")
    }

    for col, (label, color_val) in series_to_check.items():
        if col in standardized_df.columns and not standardized_df[col].isnull().all():
            cols_to_plot.append(col)
            labels_map[col] = label
            colors_map[col] = color_val # Store mapping for Plotly
        else:
            print(f"Plot 4: Data for '{label}' not available or all NaN.")

    if not cols_to_plot:
        print("Plot 4: Not enough valid standardized data to generate the plot.")
        return None
        
    fig = px.line(standardized_df,
                  y=cols_to_plot,
                  title="Standardized USD Index, Treasuries, Brent & WTI Oil Prices",
                  labels={"value": "Standardized Value (Z-score)", "date": "Date", **labels_map},
                  color_discrete_map=colors_map) 
    fig.update_layout(legend_title_text='Series')
    return fig

# --- Main execution block ---
if __name__ == "__main__":
    print("Running T(s,a)_to_d.py directly for testing interactive plots...")
    
    fda_module = globals()
    
    success, file_path = fda_module['fetch_and_store_data']()
    
    
    if success and file_path:
        original_data, standardized_data_for_plots = fda_module['load_and_prepare_data_for_plotting'](file_path)
        
        if original_data is not None and standardized_data_for_plots is not None:
            print("\n--- Generating All Plots (Test Run - will open in browser) ---")
            
            # Plot 1
            print("\nGenerating Interactive Plot 1: Standardized USD Index, 2-Year & 10-Year Treasury Yields...")
            if not standardized_data_for_plots.empty:
                fig1 = fda_module['plot_standardized_usd_treasuries'](standardized_data_for_plots)
                if fig1: fig1.show()
            else:
                print("Standardized data is empty. Skipping Plot 1.")

            # Plot 2
            print("\nGenerating Interactive Plot 2: Treasury Yield Difference (Original Values)...")
            if not original_data.empty:
                fig2 = fda_module['plot_treasury_spread'](original_data)
                if fig2: fig2.show()
            else:
                print("Original data is empty. Skipping Plot 2.")

            # Plot 3
            print("\nGenerating Interactive Plot 3: Standardized USD Index, Treasuries & FX Rates...")
            if not standardized_data_for_plots.empty:
                fig3 = fda_module['plot_standardized_fx_treasuries'](standardized_data_for_plots)
                if fig3: fig3.show()
            else:
                print("Standardized data is empty. Skipping Plot 3.")

            # Plot 4
            print("\nGenerating Interactive Plot 4: Standardized USD Index, Treasuries, Brent & WTI Oil Prices...")
            if not standardized_data_for_plots.empty:
                fig4 = fda_module['plot_standardized_usd_treasuries_commodities'](standardized_data_for_plots)
                if fig4: fig4.show()
            else:
                print("Standardized data is empty. Skipping Plot 4.")
            
            print("\n--- All plots generated (Test Run). Check your browser. ---")
        else:
            print("Could not load or prepare data for plotting after fetching (Test Run).")
    else:
        print("\nData fetching and storage failed. Cannot proceed to plotting (Test Run).")
    print("\nDirect script execution of T(s,a)_to_d.py finished.")

