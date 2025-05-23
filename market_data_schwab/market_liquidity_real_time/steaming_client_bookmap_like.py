import asyncio
import json
import logging
import os
from dotenv import load_dotenv
from schwabdev import Client
from schwabdev.stream import Stream
from collections import deque, OrderedDict #order book display and trade log
import datetime #trade timestamps

load_dotenv()

# --- Configuration ---
API_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
CALLBACK_URL = os.getenv("CALLBACK_URL")
TOKEN_FILE_PATH = os.getenv("TOKEN_PATH", "path/to/your/schwab_token.json")
ETF_SYMBOL = "SPY" 
DISPLAY_INTERVAL = 5  # seconds per update
MAX_BOOK_LEVELS_DISPLAY = 10 # Number bid/ask levels to display
MAX_TRADES_DISPLAY = 10 # Number of recent trades to display

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# Silence Schwabdev's own INFO logs if they are too verbose for specific output
# logging.getLogger('schwabdev.client').setLevel(logging.WARNING)
# logging.getLogger('schwabdev.stream').setLevel(logging.WARNING)


# --- Global Data Structures for Bookmap-like analysis ---
# Store order book data: {'bids': {price: volume}, 'asks': {price: volume}}
# Prices == keys, volumes == values.
# Bids to be displayed highest price first, Asks lowest price first.
order_book = {
    ETF_SYMBOL: {
        'bids': OrderedDict(), # price: totalVolume
        'asks': OrderedDict()  # price: totalVolume
    }
}
# Store last N trades: deque([(timestamp, price, volume, aggressor_side)])
trade_log = {
    ETF_SYMBOL: deque(maxlen=100) # Store 100 most recent trades
}
# Store current Best Bid and Offer (BBO)
current_bbo = {
    ETF_SYMBOL: {'bid_price': None, 'bid_size': None, 'ask_price': None, 'ask_size': None, 'timestamp': None}
}

# --- Data Handlers ---
async def handle_level_one_equity(data):
    """Handles Level One Equity data (quotes, BBO, volume)."""
    # logger.info(f"LEVEL ONE EQUITY: {json.dumps(data, indent=2)}")
    try:
        content = data.get('content', [{}])[0]
        symbol = content.get('key', ETF_SYMBOL) # Assuming key == symbol
        if symbol not in current_bbo: # Initialize if not present
             current_bbo[symbol] = {'bid_price': None, 'bid_size': None, 'ask_price': None, 'ask_size': None, 'timestamp': None}
        
        current_bbo[symbol]['bid_price'] = content.get('BID_PRICE')
        current_bbo[symbol]['bid_size'] = content.get('BID_SIZE')
        current_bbo[symbol]['ask_price'] = content.get('ASK_PRICE')
        current_bbo[symbol]['ask_size'] = content.get('ASK_SIZE')
        current_bbo[symbol]['timestamp'] = data.get('timestamp', datetime.datetime.now().timestamp() * 1000)

    except Exception as e:
        logger.error(f"Error processing Level One Equity: {e} - Data: {data}")


async def handle_level_two_book_data(data, book_type="NASDAQ_BOOK"): # NASDAQ_BOOK or NYSE_BOOK
    """
    Handles Level Two Order Book data (e.g., NASDAQ_BOOK, NYSE_BOOK).
    Assumes 'data' contains snapshots of aggregated depth at each price level.
    """
    # logger.info(f"{book_type} DATA: {json.dumps(data, indent=2)}")
    try:
        symbol = data.get('key', ETF_SYMBOL)
        content = data.get('content')

        if not content:
            logger.warning(f"No content in {book_type} data for {symbol}")
            return

        if symbol not in order_book: # Initialize if not present
            order_book[symbol] = {'bids': OrderedDict(), 'asks': OrderedDict()}
            current_bbo[symbol] = {'bid_price': None, 'bid_size': None, 'ask_price': None, 'ask_size': None, 'timestamp': None}

        # Create new OrderedDicts for the update
        new_bids = OrderedDict()
        new_asks = OrderedDict()

        # Process bids (typically sorted highest price first in data)
        raw_bids = content.get('bids', [])
        for bid_level in raw_bids:
            price = bid_level.get('price')
            volume = bid_level.get('totalVolume') # Or 'volume' !!!!! check ##########################
            if price is not None and volume is not None:
                new_bids[float(price)] = int(volume)
        
        # Process asks
        raw_asks = content.get('asks', [])
        for ask_level in raw_asks:
            price = ask_level.get('price')
            volume = ask_level.get('totalVolume')
            if price is not None and volume is not None:
                new_asks[float(price)] = int(volume)

        # Update the main order book for the symbol
        # Sort bids by price descending, asks by price ascending
        order_book[symbol]['bids'] = OrderedDict(sorted(new_bids.items(), key=lambda item: item[0], reverse=True))
        order_book[symbol]['asks'] = OrderedDict(sorted(new_asks.items(), key=lambda item: item[0]))

        # Update BBO from Level 2 if available and more recent than Level 1
        if order_book[symbol]['bids']:
            best_bid_price = next(iter(order_book[symbol]['bids']))
            current_bbo[symbol]['bid_price'] = best_bid_price
            current_bbo[symbol]['bid_size'] = order_book[symbol]['bids'][best_bid_price]
        if order_book[symbol]['asks']:
            best_ask_price = next(iter(order_book[symbol]['asks']))
            current_bbo[symbol]['ask_price'] = best_ask_price
            current_bbo[symbol]['ask_size'] = order_book[symbol]['asks'][best_ask_price]
        current_bbo[symbol]['timestamp'] = data.get('timestamp', datetime.datetime.now().timestamp() * 1000)

    except Exception as e:
        logger.error(f"Error processing {book_type} data: {e} - Data: {data}", exc_info=True)


async def handle_nasdaq_book(data): # Wrapper for NASDAQ
    await handle_level_two_book_data(data, "NASDAQ_BOOK")

async def handle_nyse_book(data): # Wrapper for NYSE
    await handle_level_two_book_data(data, "NYSE_BOOK")


async def handle_timesale_equity(data):
    """Handles Time & Sales data (individual trades)."""
    # logger.info(f"TIME & SALES: {json.dumps(data, indent=2)}")
    try:
        content = data.get('content', [{}])[0]
        symbol = content.get('key', ETF_SYMBOL)
        trade_price = float(content.get('LAST_PRICE'))
        trade_volume = int(content.get('LAST_SIZE'))
        trade_time_ms = content.get('TRADE_TIME') # Milliseconds since epoch

        if symbol not in trade_log: # Initialize if not present
            trade_log[symbol] = deque(maxlen=100)
        if symbol not in current_bbo: # Ensure BBO entry exists
             current_bbo[symbol] = {'bid_price': None, 'bid_size': None, 'ask_price': None, 'ask_size': None, 'timestamp': None}


        aggressor_side = "UNKNOWN"
        bbo_at_trade = current_bbo[symbol] # Use the most recent BBO

        if bbo_at_trade['bid_price'] is not None and bbo_at_trade['ask_price'] is not None:
            if trade_price >= bbo_at_trade['ask_price']:
                aggressor_side = "BUY"  # Lifted the ask or traded at/above ask
            elif trade_price <= bbo_at_trade['bid_price']:
                aggressor_side = "SELL" # Hit the bid or traded at/below bid
            # Could add a 'BETWEEN' if trade_price is between bid and ask (market order)

        trade_dt = datetime.datetime.fromtimestamp(trade_time_ms / 1000) if trade_time_ms else datetime.datetime.now()
        
        trade_entry = {
            'time': trade_dt.strftime('%H:%M:%S.%f')[:-3], # Millisecond precision
            'price': trade_price,
            'volume': trade_volume,
            'aggressor': aggressor_side
        }
        trade_log[symbol].append(trade_entry)

    except Exception as e:
        logger.error(f"Error processing Time & Sales: {e} - Data: {data}")


async def handle_chart_equity(data): # Keep this if you need chart data for other purposes
    logger.debug(f"CHART EQUITY: {json.dumps(data, indent=2)}")


async def display_bookmap_like_textual(symbol=ETF_SYMBOL):
    """Prints a textual representation of the order book and recent trades."""
    if os.name == 'nt': # Windows
        _ = os.system('cls')
    else: # macOS and Linux
        _ = os.system('clear')

    logger.info(f"--- Bookmap-like View for {symbol} --- ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---")

    bbo = current_bbo.get(symbol, {})
    book = order_book.get(symbol, {'bids': OrderedDict(), 'asks': OrderedDict()})
    trades = list(trade_log.get(symbol, deque())) # Get a copy

    # Display Asks (lowest price first, up to MAX_BOOK_LEVELS_DISPLAY)
    logger.info("\n--- ASKS (Price: Volume) ---")
    ask_levels_shown = 0
    for price, volume in list(book['asks'].items())[:MAX_BOOK_LEVELS_DISPLAY]: # Iterate over copy
        logger.info(f"{price:.2f} : {volume:<8}{'<-- BEST ASK' if price == bbo.get('ask_price') else ''}")
        ask_levels_shown += 1
    if not ask_levels_shown:
        logger.info(" (No ask data or empty)")


    # Display Current BBO Spread
    logger.info("\n--- SPREAD ---")
    if bbo.get('ask_price') and bbo.get('bid_price'):
        spread = bbo['ask_price'] - bbo['bid_price']
        logger.info(f"Ask: {bbo.get('ask_price', 'N/A'):.2f} (Size: {bbo.get('ask_size', 'N/A')})")
        logger.info(f"Bid: {bbo.get('bid_price', 'N/A'):.2f} (Size: {bbo.get('bid_size', 'N/A')})")
        logger.info(f"Spread: {spread:.2f}")
    else:
        logger.info(" (BBO data not fully available)")


    # Display Bids (highest price first, up to MAX_BOOK_LEVELS_DISPLAY)
    logger.info("\n--- BIDS (Price: Volume) ---")
    bid_levels_shown = 0
    for price, volume in list(book['bids'].items())[:MAX_BOOK_LEVELS_DISPLAY]: # Iterate over a copy
        logger.info(f"{price:.2f} : {volume:<8}{'<-- BEST BID' if price == bbo.get('bid_price') else ''}")
        bid_levels_shown +=1
    if not bid_levels_shown:
        logger.info(" (No bid data or empty)")


    # Display Recent Trades (most recent first, up to MAX_TRADES_DISPLAY)
    logger.info(f"\n--- RECENT TRADES (Last {min(len(trades), MAX_TRADES_DISPLAY)}) ---")
    for trade in reversed(trades[-MAX_TRADES_DISPLAY:]):
        color_prefix = ""
        color_suffix = "" # ANSI escape codes for color
        if trade['aggressor'] == 'BUY':
            color_prefix = "\033[92m"  # Green
        elif trade['aggressor'] == 'SELL':
            color_prefix = "\033[91m"  # Red
        if color_prefix:
            color_suffix = "\033[0m"   # Reset
        
        logger.info(f"{color_prefix}{trade['time']} | {trade['price']:.2f} | Vol: {trade['volume']:<6} | {trade['aggressor']:<7}{color_suffix}")
    if not trades:
        logger.info(" (No trades logged yet)")
    logger.info("------------------------------------")


async def periodic_display_task(symbol=ETF_SYMBOL, interval=DISPLAY_INTERVAL):
    while True:
        await display_bookmap_like_textual(symbol)
        await asyncio.sleep(interval)


async def main_streaming_loop():
    """Main function to set up and run the streaming client using Schwabdev."""
    if not all([API_KEY, APP_SECRET, CALLBACK_URL]):
        logger.error("Missing required environment variables: APP_KEY, APP_SECRET, CALLBACK_URL. Check .env file.")
        return
    
    logger.info(f"Token file will be managed at: {TOKEN_FILE_PATH}")

    try:
        api_client = Client(
            api_key=API_KEY, secret_key=APP_SECRET,
            callback_url=CALLBACK_URL, token_path=TOKEN_FILE_PATH
        )
        logger.info("Schwabdev Client initialized.")

        stream_client = Stream(api_client)
        logger.info("Schwabdev Stream initialized.")

        # Add handlers
        stream_client.add_level_one_equity_handler(handle_level_one_equity)
        stream_client.add_timesale_handler(handle_timesale_equity)
        # Choose ONE of the book handlers based on ETF's listing exchange:
        stream_client.add_nasdaq_book_handler(handle_nasdaq_book) # NASDAQ-listed ETFs e.g., QQQ
        # stream_client.add_nyse_book_handler(handle_nyse_book)   # NYSE-listed ETFs e.g., SPY (ARCA book)
        
        # stream_client.add_chart_equity_handler(handle_chart_equity) ######################### Optional

        logger.info("Data handlers added.")
        symbols_to_stream = [ETF_SYMBOL]

        # Subscribe to services
        logger.info(f"Subscribing to services for: {symbols_to_stream}")
        await stream_client.level_one_equity(symbols=symbols_to_stream, fields=stream_client.level_one_equity_fields())
        await stream_client.timesale(symbols=symbols_to_stream, fields=stream_client.timesale_fields_all())
        
        # IMPORTANT: Choose the correct book for ticker 
        # For SPY (ARCA which is part of NYSE group): NYSE_BOOK
        # For QQQ (NASDAQ): NASDAQ_BOOK
        
        # CHECK SCHWAB  API docs for mapping symbols to correct book services########################!!!!!!!!!!!########
        
        await stream_client.nasdaq_book(symbols=symbols_to_stream, fields=stream_client.nasdaq_book_fields_all())
        # await stream_client.nyse_book(symbols=symbols_to_stream, fields=stream_client.nyse_book_fields_all())
        # await stream_client.chart_equity(symbols=symbols_to_stream, fields=stream_client.chart_fields_all()) ########################## Optional

        logger.info("Subscriptions sent.")
        await stream_client.start(auto_reconnect=True)
        logger.info("Schwabdev stream started. Monitoring for data...")

        # Start periodic display task
        display_task = asyncio.create_task(periodic_display_task(symbol=ETF_SYMBOL, interval=DISPLAY_INTERVAL))

        try:
            while True: # Keep main alive
                await asyncio.sleep(3600) # Sleep for long time, tasks run in background
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received. Stopping...")
        finally:
            logger.info("Stopping display task...")
            display_task.cancel()
            try:
                await display_task
            except asyncio.CancelledError:
                logger.info("Display task successfully cancelled.")
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

