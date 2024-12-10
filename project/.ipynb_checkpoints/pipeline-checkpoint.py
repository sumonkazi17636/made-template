import io
import os
import requests
import pandas as pd
import sqlite3

# Configuration
DATA_URLS = {
    "chicago_building_permits": "https://data.cityofchicago.org/api/views/6irb-gasv/rows.csv?accessType=DOWNLOAD",
    "cdc_data": "https://data.cdc.gov/api/views/hk9y-quqm/rows.csv?accessType=DOWNLOAD"
}

DATA_DIR = "../data"
DB_FILE = os.path.join(DATA_DIR, "chicago_cdc.db")

def download_data(url):
    """Download dataset from a URL and return it as a pandas DataFrame."""
    response = requests.get(url)
    response.raise_for_status()  # Raises error for bad requests
    data = pd.read_csv(io.StringIO(response.text))
    return data

def clean_chicago_data(df):
    """Clean and transform Chicago building permits data."""
    df.fillna(0, inplace=True)
    df['Week End'] = pd.to_datetime(df['Week End'], errors='coerce')
    df.drop_duplicates(inplace=True)
    return df

def clean_cdc_data(df):
    """Clean and transform CDC data."""
    df.fillna(0, inplace=True)
    df['Data As Of'] = pd.to_datetime(df['Data As Of'], errors='coerce')
    df.drop_duplicates(inplace=True)
    return df

def save_to_sqlite(df, table_name, db_file):
    """Save the DataFrame to an SQLite database."""
    with sqlite3.connect(db_file) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)

def save_to_csv(df, file_name):
    """Save the DataFrame to a CSV file."""
    file_path = os.path.join(DATA_DIR, file_name)
    df.to_csv(file_path, index=False)
    print(f"Data saved to {file_path}")

def main():
    # Ensure data directory exists
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    # Download, clean, and save each dataset
    for name, url in DATA_URLS.items():
        print(f"Processing {name} dataset...")

        # Step 1: Download data
        df = download_data(url)

        # Step 2: Clean data
        if name == "chicago_building_permits":
            df = clean_chicago_data(df)
        elif name == "cdc_data":
            df = clean_cdc_data(df)

        # Step 3: Save data to SQLite and CSV
        save_to_sqlite(df, name, DB_FILE)
        save_to_csv(df, f"{name}.csv")

    print("Pipeline completed. Data saved to DB file and CSV files")

if __name__ == "__main__":
    main()
