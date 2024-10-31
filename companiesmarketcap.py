# Name: companiesmarketcap.py
# Version: 0.1
# Author: drhdev
# Description: Downloads largest companies in the USA by market cap data, stores data in MySQL database, updates new and changed data entries.

import os
import sys
import csv
import logging
import requests
import sqlalchemy
from sqlalchemy import create_engine, Table, Column, String, Float, DateTime, MetaData
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from datetime import datetime
import shutil

# Set the base directory
base_dir = os.path.dirname(os.path.abspath(__file__))

# Set up logging
log_filename = os.path.join(base_dir, f"companiesmarketcap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logger = logging.getLogger('companiesmarketcap.py')
logger.setLevel(logging.DEBUG)

# Create a new log file for each run
handler = logging.FileHandler(log_filename)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Limit the number of log files to 10 by deleting the oldest
log_files = sorted([f for f in os.listdir(base_dir) if f.startswith('companiesmarketcap_') and f.endswith('.log')])
if len(log_files) > 10:
    for old_log in log_files[:-10]:
        os.remove(os.path.join(base_dir, old_log))

# Console handler for verbose mode
if '-v' in sys.argv:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

# Load environment variables
load_dotenv(os.path.join(base_dir, '.env'))
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_charset = os.getenv('DB_CHARSET')

# Hardcoded table name and column prefix
table_name = 'companiesmarketcap'
columns_prefix = 'cmc_'

# Validate environment variables
required_env_vars = ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_CHARSET']
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    logger.error(f"Missing environment variables: {', '.join(missing_vars)}. Please check the .env file.")
    sys.exit(1)

# Create temporary directory for CSV file
temp_dir = os.path.join(base_dir, 'temp_companiesmarketcap')
if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

# Define database URL and structure
try:
    db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}?charset={db_charset}"
    metadata = MetaData()
    companiesmarketcap_table = Table(
        table_name, metadata,
        Column(f'{columns_prefix}rank', String(5)),
        Column(f'{columns_prefix}name', String(255)),
        Column(f'{columns_prefix}symbol', String(20), primary_key=True),
        Column(f'{columns_prefix}marketcap', Float),
        Column(f'{columns_prefix}latest_price', Float),
        Column(f'{columns_prefix}country', String(100)),
        Column(f'{columns_prefix}last_polled', DateTime),
        Column(f'{columns_prefix}last_updated', DateTime),
        Column(f'{columns_prefix}origin', String(255), default="https://companiesmarketcap.com")
    )

    # Connect to the MySQL database
    engine = create_engine(db_url)
    metadata.create_all(engine)
    connection = engine.connect()
    transaction = connection.begin()
except SQLAlchemyError as e:
    logger.error(f"Database connection failed: {e}")
    sys.exit(1)

# Download CSV file
url = "https://companiesmarketcap.com/usa/largest-companies-in-the-usa-by-market-cap/?download=csv"
csv_path = os.path.join(temp_dir, 'companiesmarketcap.csv')

try:
    response = requests.get(url)
    response.raise_for_status()
    with open(csv_path, 'wb') as f:
        f.write(response.content)
    logger.info("CSV file downloaded successfully.")
except requests.RequestException as e:
    logger.error(f"Failed to download CSV: {e}")
    sys.exit(1)

# Verify CSV structure and required columns
required_columns = {'Rank', 'Name', 'Symbol', 'marketcap', 'price (USD)', 'country'}
try:
    with open(csv_path, 'r') as f:
        csv_reader = csv.DictReader(f)
        csv_columns = set(csv_reader.fieldnames)
        if not required_columns.issubset(csv_columns):
            logger.error("CSV structure is invalid or missing required columns.")
            sys.exit(1)
except Exception as e:
    logger.error(f"Error reading CSV structure: {e}")
    sys.exit(1)

# Clear existing table data
try:
    connection.execute(companiesmarketcap_table.delete())
    transaction.commit()
    transaction = connection.begin()
    logger.info("Existing data in the table cleared successfully.")
except SQLAlchemyError as e:
    logger.error(f"Failed to clear existing data: {e}")
    sys.exit(1)

# Process CSV file and insert new data
try:
    with open(csv_path, 'r') as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            # Convert fields with safe handling for empty values
            rank = int(row['Rank']) if row['Rank'] else None
            name = row['Name'] if row['Name'] else None
            symbol = row['Symbol'] if row['Symbol'] else None
            marketcap = float(row['marketcap']) if row['marketcap'] else None
            latest_price = float(row['price (USD)']) if row['price (USD)'] else None
            country = row['country'] if row['country'] else None
            last_polled = datetime.now()

            # Ensure at least the symbol is not None, otherwise skip
            if not symbol:
                logger.warning(f"Skipping row with missing symbol: {row}")
                continue

            # Prepare the insert statement
            stmt = insert(companiesmarketcap_table).values(
                **{
                    f'{columns_prefix}rank': rank,
                    f'{columns_prefix}name': name,
                    f'{columns_prefix}symbol': symbol,
                    f'{columns_prefix}marketcap': marketcap,
                    f'{columns_prefix}latest_price': latest_price,
                    f'{columns_prefix}country': country,
                    f'{columns_prefix}last_polled': last_polled,
                    f'{columns_prefix}last_updated': datetime.now(),
                    f'{columns_prefix}origin': "https://companiesmarketcap.com"
                }
            )

            # Execute statement
            connection.execute(stmt)
            logger.debug(f"Inserted data for symbol={symbol}")

        # Commit all changes after inserting
        transaction.commit()
        logger.info("Database updated with new CSV data successfully.")
except (SQLAlchemyError, csv.Error) as e:
    logger.error(f"Failed to process CSV and update database: {e}")
    transaction.rollback()
    sys.exit(1)
finally:
    # Clean up temp directory
    if os.path.exists(csv_path):
        os.remove(csv_path)
    if os.path.exists(temp_dir) and len(os.listdir(temp_dir)) == 0:
        os.rmdir(temp_dir)

    # Close the database connection
    connection.close()
    logger.info("Script completed successfully.")