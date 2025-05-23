import asyncio
import json
import logging
import os 
from dotenv import load_dotenv 
from schwabdev import Client 
from schwabdev.stream import Stream

load_dotenv()

# --- Configuration ---
API_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
CALLBACK_URL = os.getenv("CALLBACK_URL")
TOKEN_FILE_PATH = os.getenv("TOKEN_PATH", "tokens.json") 
ETF_SYMBOL = "SPY" 

# Configure logging to see stream activity
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Data Handlers ---
async def handle_level_one_equity(data):
    """Handles Level One Equity data (quotes, volume)."""
    logger.info(f"LEVEL ONE EQUITY: {json.dumps(data, indent=2)}")
    # Example: Accessing last price and total volume
    # 'key' = symbol, 'content' = data items.
    
    # e.g., assuming 'content' is a list of items:
    # if data.get('content'):
    #    item = data['content'][0]
    #    logger.info(f"Symbol: {item.get('key')}, Last: {item.get('LAST_PRICE')}, Vol: {item.get('TOTAL_VOLUME')}")


async def handle_timesale_equity(data):
    """Handles Time & Sales data (individual trades)."""
    logger.info(f"TIME & SALES: {json.dumps(data, indent=2)}")
    # e.g., accessing trade price and size
    # if data.get('content'):
    #    item = data['content'][0]
    #    logger.info(f"Symbol: {item.get('key')}, Trade: {item.get('LAST_SIZE')} @ {item.get('LAST_PRICE')}")
    #########!! To a aggregate data for volume profile

async def handle_chart_equity(data):
    """Handles Chart Equity data (OHLCV candles)."""
    logger.info(f"CHART EQUITY: {json.dumps(data, indent=2)}")
    # e.g., accessing OHLCV for a candle
    # if data.get('content'):
    #    item = data['content'][0]
    #    logger.info(f"Symbol: {item.get('key')}, O:{item.get('OPEN_PRICE')} H:{item.get('HIGH_PRICE')} L:{item.get('LOW_PRICE')} C:{item.get('CLOSE_PRICE')} V:{item.get('VOLUME')}")
    # If Schwabdev streams 1-minute candles for CHART_EQUITY,
    # aggregate them into 1-hour candles 

async def handle_level_two_book(data): # Handler for NASDAQ_BOOK or NYSE_BOOK
    """Handles Level Two Order Book data."""
    logger.info(f"LEVEL TWO BOOK: {json.dumps(data, indent=2)}")
    # bids = data.get('content', {}).get('bids', [])
    # asks = data.get('content', {}).get('asks', [])
    # if bids and asks:
    #     logger.info(f"Symbol: {data.get('key')}, Top Bid: {bids[0]}, Top Ask: {asks[0]}")

async def main_streaming_loop():
    """Main function to set up and run the streaming client using Schwabdev."""
    if not all([API_KEY, APP_SECRET, CALLBACK_URL]):
        logger.error("Missing one or more required environment variables: APP_KEY, APP_SECRET, CALLBACK_URL. Please check your .env file.")
        return
    if TOKEN_FILE_PATH == "tokens.json" and not os.getenv("TOKEN_PATH"): 
        logger.warning("TOKEN_FILE_PATH is using the default value. Consider setting TOKEN_PATH in your .env file or ensure the default path is correct.")


    try:
        # Initialize Schwabdev Client (handles auth and token management)
        api_client = Client(
            api_key=API_KEY,
            secret_key=APP_SECRET,
            callback_url=CALLBACK_URL,
            token_path=TOKEN_FILE_PATH
        )
        logger.info("Schwabdev Client initialized. It will handle token acquisition/refresh.")

        # Initialize Schwabdev Stream
        stream_client = Stream(api_client)
        logger.info("Schwabdev Stream initialized.")

        # Add more handlers for every different data type
        # Schwabdev has specific add_X_handler methods
        stream_client.add_level_one_equity_handler(handle_level_one_equity)
        stream_client.add_timesale_handler(handle_timesale_equity) # Note: add_timesale_handler
        stream_client.add_chart_equity_handler(handle_chart_equity)

        # For Level 2, SPECIFY exchange: NASDAQ:
        stream_client.add_nasdaq_book_handler(handle_level_two_book)
        #  NYSE:
        stream_client.add_nyse_book_handler(handle_level_two_book)

        logger.info("Data handlers added.")

        # Define symbols list
        symbols_to_stream = [ETF_SYMBOL]

        # Subscribe to services using Schwabdev's methods and field helpers
        logger.info(f"Subscribing to services for symbols: {symbols_to_stream}")

        # Level One Equity Quotes
        await stream_client.level_one_equity(symbols=symbols_to_stream, fields=stream_client.level_one_equity_fields())
        logger.info(f"Subscribed to Level One Equity for {symbols_to_stream}")

        # Time & Sales (individual trades)
        await stream_client.timesale(symbols=symbols_to_stream, fields=stream_client.timesale_fields_all())
        logger.info(f"Subscribed to Time & Sales for {symbols_to_stream}")

        # Chart Equity (OHLCV candles)
        await stream_client.chart_equity(symbols=symbols_to_stream, fields=stream_client.chart_fields_all())
        logger.info(f"Subscribed to Chart Equity for {symbols_to_stream}. Aggregation may be needed for 1-hour candles.")

        # Level Two Data (Order Book) - Example for NASDAQ listed symbol
        # Ensure ETF_SYMBOL is listed on NASDAQ for this to work.
        # await stream_client.nasdaq_book(symbols=symbols_to_stream, fields=stream_client.nasdaq_book_fields_all())
        # logger.info(f"Subscribed to NASDAQ Book (Level II) for {symbols_to_stream}")
        # Or for NYSE:
        # await stream_client.nyse_book(symbols=symbols_to_stream, fields=stream_client.nyse_book_fields_all())


        # Start the stream (Schwabdev handles reconnection automatically if auto_reconnect=True)
        logger.info("Starting Schwabdev stream...")
        await stream_client.start(auto_reconnect=True) # This starts tasks for receiving messages

        # Keep the main coroutine alive while the stream runs in the background
        logger.info("Stream started. Monitoring for data. Press Ctrl+C to stop.")
        try:
            while True:
                await asyncio.sleep(1)  # Keep alive
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received. Stopping stream...")
        finally:
            logger.info("Stopping Schwabdev stream...")
            await stream_client.stop()
            logger.info("Schwabdev stream stopped.")

    except Exception as e:
        logger.critical(f"An error occurred in the main streaming loop: {e}", exc_info=True)

if __name__ == "__main__":
    try:
        asyncio.run(main_streaming_loop())
    except Exception as e:
        logger.critical(f"Critical error in main execution: {e}", exc_info=True)
