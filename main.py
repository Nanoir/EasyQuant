import ccxt
import pandas as pd
from pymongo import MongoClient

pd.set_option('expand_frame_repr', False)

# Set up the MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['iQuant']

def fetch_and_store(symbol):
    # Fetch Kline data from OKX for different timeframes
    timeframes = ['5m', '15m', '30m', '1h', '4h', '1d']

    # Create a dictionary to store data for each timeframe
    kline_data = {}

    for timeframe in timeframes:
        klines = okx.fetch_ohlcv(symbol, timeframe, limit=301)  # Fetch 301 to have an extra data point for comparison

        # Create DataFrame from fetched Kline data
        df = pd.DataFrame(klines, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])

        # Convert timestamp to datetime
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')

        # Remove the last row as it does not have a closing value
        df = df.iloc[:-1]

        # Convert DataFrame to a list of dictionaries
        kline_data[timeframe] = df.to_dict(orient='records')

    # Create collection if not exists
    collection_name = symbol
    if collection_name not in mongo_db.list_collection_names():
        mongo_db.create_collection(collection_name)
        print(f"Collection {collection_name} created.")

    # Update documents for each timeframe
    for timeframe, data in kline_data.items():
        existing_data = mongo_db[collection_name].find_one({'timeframe': timeframe})

        # Get the latest timestamp from the existing data
        latest_timestamp = None

        if existing_data and 'data' in existing_data:
            latest_timestamp = max(existing_data['data'], key=lambda x: x['datetime'])['datetime']

        # Filter out new data points
        new_data = [point for point in data if not latest_timestamp or point['datetime'] > pd.Timestamp(latest_timestamp)]

        if new_data:
            # Append new data to existing data
            if existing_data:
                existing_data['data'].extend(new_data)
                # Update existing document
                mongo_db[collection_name].update_one({'timeframe': timeframe}, {'$set': {'data': existing_data['data']}})
                print(f"Data for {symbol} ({timeframe}) updated successfully.")
            else:
                # Insert new document into MongoDB
                mongo_db[collection_name].insert_one({'timeframe': timeframe, 'data': new_data})
                print(f"Data for {symbol} ({timeframe}) inserted successfully.")
        else:
            print(f"No new data for {symbol} ({timeframe}).")

# Instantiate the OKX exchange object
okx = ccxt.okx({
    'timeout': 30000,
    'enableRateLimit': True,
    'proxies': {
        'http': 'http://127.0.0.1:7890',
        'https': 'http://127.0.0.1:7890'
    }
})

okx.load_markets()

# Define symbols
symbols = ['BTC/USDT', 'ETH/USDT', 'ARB/USDT', 'SOL/USDT', 'BNB/USDT', 'OP/USDT', 'UNI/USDT', 'LTC/USDT', 'OKB/USDT',
           'DOGE/USDT']

# Fetch and store data for each symbol
for symbol in symbols:
    fetch_and_store(symbol)
