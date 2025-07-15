import pandas as pd
from fastapi import FastAPI
import os

# ==============================
# Global control variables
# ==============================

orders_df = None       # Main DataFrame containing combined order data
current_index = 0      # Cursor to simulate streaming batches

# ==============================
# Data path configuration
# ==============================

# Get the current file path
current_file_path = os.path.dirname(os.path.abspath(__file__))

# Define the relative path to the 'data' folder (assuming project structure: project/data/)
data_path = os.path.join(current_file_path, '..', 'data')

# ==============================
# Load and prepare dataset
# ==============================

def load_and_prepare_data():
    """
    Load the Olist order and order items CSV files,
    merge them, sort chronologically and store in the global DataFrame 'orders_df'.
    """
    global orders_df

    print("Loading source data...")

    # Read orders and order items CSV files
    orders = pd.read_csv(os.path.join(data_path, 'olist_orders_dataset.csv'))
    order_items = pd.read_csv(os.path.join(data_path, 'olist_ordes_items_dataset.csv'))

    print('Processing and merging data...')

    # Merge datasets on 'order_id'
    df = pd.merge(order_items, orders, on='order_id')

    # Convert purchase timestamp to datetime
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])

    # Sort by purchase timestamp (oldest first)
    df = df.sort_values(by='order_purchase_timestamp', ascending=True).reset_index(drop=True)

    # Save to global DataFrame
    orders_df = df

    print('Data loaded successfully!')
    print(f'Total orders to serve: {len(orders_df)}')

# ==============================
# FastAPI app setup
# ==============================

app = FastAPI(
    title="Olist Orders API",
    description="A simple API that serves Olist orders in batches to simulate a streaming feed.",
    version="1.0.0"
)

# Load data on startup
load_and_prepare_data()

# ==============================
# Health check endpoint
# ==============================

@app.get('/health')
def healthcheck():
    """
    Health check endpoint to verify that the API is running.
    """
    return {"message": "ok"}

# ==============================
# Orders batch endpoint
# ==============================

@app.get('/orders')
def get_new_orders():
    """
    Serve the next batch of orders.
    When all orders are served, the cursor is reset.
    """
    global current_index

    batch_size = 100  # Number of records per batch

    # If all orders have been served, reset index and notify client
    if current_index >= len(orders_df):
        print("All orders served. Resetting cursor.")
        current_index = 0
        return {"message": "All orders served. Feed has been reset.", "orders": []}

    # Define next batch range
    end_index = current_index + batch_size
    batch = orders_df.iloc[current_index:end_index]

    # Update cursor
    current_index = end_index

    print(f'Serving {len(batch)} orders. Next index: {current_index}')

    # Convert batch DataFrame to list of dicts and replace NaN with None
    batch_dict = batch.where(pd.notnull(batch), None).to_dict('records')

    return {"orders": batch_dict}
