# Name: earnings-calendar.py
# Version: 0.1.8
# Author: drhdev
# Description: Downloads earnings calendar data from Alpha Vantage API daily, stores data in MySQL database, updates new and changed data entries.

import os
import sys
import csv
import logging
import requests
import sqlalchemy
from sqlalchemy import create_engine, Table, Column, String, Float, DateTime, MetaData, select
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from datetime import datetime
import shutil

# Set the base directory
base_dir = os.path.dirname(os.path.abspath(__file__))

# Set up logging
log_filename = os.path.join(base_dir, f"earnings-calendar_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logger = logging.getLogger('earnings-calendar.py')
logger.setLevel(logging.DEBUG)

# Create a new log file for each run
handler = logging.FileHandler(log_filename)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Limit the number of log files to 10 by deleting the oldest
log_files = sorted([f for f in os.listdir(base_dir) if f.startswith('earnings-calendar_') and f.endswith('.log')])
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
api_key = os.getenv('ALPHAVANTAGE_API_KEY')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_charset = os.getenv('DB_CHARSET')
table_name = os.getenv('TABLE_NAME')
columns_prefix = os.getenv('COLUMNS_PREFIX')

# Validate environment variables
required_env_vars = ['ALPHAVANTAGE_API_KEY', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_CHARSET', 'TABLE_NAME', 'COLUMNS_PREFIX']
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    logger.error(f"Missing environment variables: {', '.join(missing_vars)}. Please check the .env file.")
    sys.exit(1)

# Create temporary directory
temp_dir = os.path.join(base_dir, 'temp')
if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

# Hardcoded table name and column prefix
table_name = 'earningscalendar'
columns_prefix = 'ec_'

# Define database URL and structure
try:
    db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}?charset={db_charset}"
    metadata = MetaData()
    earnings_calendar = Table(
        table_name, metadata,
        Column(f'{columns_prefix}symbol', String(20), primary_key=True),
        Column(f'{columns_prefix}name', String(255)),
        Column(f'{columns_prefix}report_date', DateTime),
        Column(f'{columns_prefix}fiscal_date_ending', DateTime),
        Column(f'{columns_prefix}estimate', Float),
        Column(f'{columns_prefix}currency', String(10)),
        Column(f'{columns_prefix}last_polled', DateTime),
        Column(f'{columns_prefix}last_updated', DateTime),
        Column(f'{columns_prefix}last_changed', String(255))
    )

    # Connect to the MySQL database
    engine = create_engine(db_url)
    metadata.create_all(engine)
    connection = engine.connect()
    transaction = connection.begin()
except SQLAlchemyError as e:
    logger.error(f"Database connection failed: {e}")
    sys.exit(1)

# Download CSV file from Alpha Vantage API
url = f"https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&apikey={api_key}&datatype=csv"
csv_path = os.path.join(temp_dir, 'earnings_calendar.csv')

try:
    response = requests.get(url)
    response.raise_for_status()
    with open(csv_path, 'wb') as f:
        f.write(response.content)
    logger.info("CSV file downloaded successfully.")
except requests.RequestException as e:
    logger.error(f"Failed to download CSV: {e}")
    sys.exit(1)

# Process CSV file and update the database
try:
    with open(csv_path, 'r') as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            # Convert fields with safe handling for empty values
            symbol = row['symbol'] if row['symbol'] else None
            name = row['name'] if row['name'] else None
            report_date = datetime.strptime(row['reportDate'], '%Y-%m-%d') if row['reportDate'] else None
            fiscal_date_ending = datetime.strptime(row['fiscalDateEnding'], '%Y-%m-%d') if row['fiscalDateEnding'] else None
            estimate = float(row['estimate']) if row['estimate'] else None
            currency = row['currency'] if row['currency'] else None
            last_polled = datetime.now()

            # Log the data being processed
            logger.debug(f"Processing data: symbol={symbol}, name={name}, report_date={report_date}, "
                         f"fiscal_date_ending={fiscal_date_ending}, estimate={estimate}, currency={currency}")

            # Ensure at least the symbol is not None, otherwise skip
            if not symbol:
                logger.warning(f"Skipping row with missing symbol: {row}")
                continue

            # Determine which columns have changed
            changed_columns = []
            if name:
                changed_columns.append(f'{columns_prefix}name')
            if report_date:
                changed_columns.append(f'{columns_prefix}report_date')
            if fiscal_date_ending:
                changed_columns.append(f'{columns_prefix}fiscal_date_ending')
            if estimate:
                changed_columns.append(f'{columns_prefix}estimate')
            if currency:
                changed_columns.append(f'{columns_prefix}currency')
            last_changed = ', '.join(changed_columns)[:255] if changed_columns else None

            # Prepare the insert or update statement
            stmt = insert(earnings_calendar).values(
                **{
                    f'{columns_prefix}symbol': symbol,
                    f'{columns_prefix}name': name,
                    f'{columns_prefix}report_date': report_date,
                    f'{columns_prefix}fiscal_date_ending': fiscal_date_ending,
                    f'{columns_prefix}estimate': estimate,
                    f'{columns_prefix}currency': currency,
                    f'{columns_prefix}last_polled': last_polled
                }
            ).on_duplicate_key_update(
                **{
                    f'{columns_prefix}name': name,
                    f'{columns_prefix}report_date': report_date,
                    f'{columns_prefix}fiscal_date_ending': fiscal_date_ending,
                    f'{columns_prefix}estimate': estimate,
                    f'{columns_prefix}currency': currency,
                    f'{columns_prefix}last_polled': last_polled,
                    f'{columns_prefix}last_updated': datetime.now(),
                    f'{columns_prefix}last_changed': last_changed
                }
            )

            # Execute statement
            connection.execute(stmt)
            logger.debug(f"Inserted/Updated data for symbol={symbol}")

            # Commit after each insert/update
            transaction.commit()
            transaction = connection.begin()

            # Verify data was written correctly by querying the database
            query = select(
                earnings_calendar.c[f'{columns_prefix}symbol'],
                earnings_calendar.c[f'{columns_prefix}name'],
                earnings_calendar.c[f'{columns_prefix}report_date'],
                earnings_calendar.c[f'{columns_prefix}fiscal_date_ending'],
                earnings_calendar.c[f'{columns_prefix}estimate'],
                earnings_calendar.c[f'{columns_prefix}currency']
            ).where(earnings_calendar.c[f'{columns_prefix}symbol'] == symbol)
            result = connection.execute(query).fetchone()
            if result:
                logger.debug(f"Verified data in database for symbol={symbol}: {result}")
            else:
                logger.error(f"Failed to verify data in database for symbol={symbol}")
    logger.info("Database updated successfully.")
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
    transaction.commit()
    connection.close()
    logger.info("Script completed successfully.")
