# earnings-calendar
A collection of Python script that download a) earnings calendar data from Alpha Vantage API and b) companies' market cap data from companiesmarketcap.com as CSV, stores data in MySQL database, updates new and changed data entries.

### Overview
The script earnings-calendar.py is designed to automate the downloading of an earnings calendar CSV file from the Alpha Vantage API and store the data in a MySQL database. The script companiesmarketcap.py downloads a CSV file with the current marketcap of >3,000 US traded companies and stores them in a MySQL database. Both log operations for easier debugging and monitoring.

### Step-by-Step Workflow

1. **Environment Setup**
    - **Base Directory and Logging**: 
      - The scripts first set up the base directory (`base_dir`) and a logging system that records the script’s activities. 
      - Log files are created in the format `name_YYYYMMDD_HHMMSS.log` in the script directory. 
      - The number of log files is limited to 10 each, with the oldest files being deleted to prevent clutter.

    - **Verbose Option**:
      - If the scripts are run with the `-v` flag, a console handler is added to display log entries in real-time on the console.

    - **Environment Variables**:
      - The scripts use a shared `.env` file to manage sensitive information and configuration settings, such as:
        - Alpha Vantage API key.
        - Database credentials and connection details (name, user, password, host, charset).
        - Table settings like table name and column prefix are hardcoded in each script.

2. **Validating API Key**
    - The earnings-calendar.py script checks if the API key is found in the `.env` file. If not, an error is logged, and the script exits.The companiesmarketcap.py script does not need an API key.

3. **Temporary Directory**
    - A temporary directory named `temp_earnings-calendar` is created for the earnings-calendar.py script and `temp_companiesmarketcap` to store the CSV files downloaded. If the directories don’t exist, they are created.

4. **Database Structure Definition**
    - **SQLAlchemy** is used to define the structure of the MySQL database table.
    - The database credentials are read from the `.env` file, and an SQLAlchemy connection (`engine`) is established.
    - The table name and column prefix are configurable ine ach script, ensuring flexibility in managing multiple tables.
    - A connection to the MySQL database is established using SQLAlchemy, and if the database connection fails, the scripts log the error and exit.

5. **Clean-Up**
    - After processing, the scripts remove the downloaded CSV files.
    - If the temporary directories are empty after cleaning, they are also removed.

6. **Closing Connection and Final Log**
    - The MySQL database connection is closed, and the scripts log a message indicating that they completed successfully.

### Logging
The scripts include comprehensive logging to help track each stage of execution, including:

- **File Logging**: All actions and errors are logged to a timestamped log file in the working directory.
- **Console Logging**: If run in verbose mode (`-v`), logs are also displayed in real time on the console.
- **Error Handling**: Errors, such as failed downloads, database connection issues, or CSV processing problems, are logged to facilitate troubleshooting.

### Database Design
- **Primary Key**: The `symbol` column is the primary key to ensure that each stock or entity is uniquely identifiable.
- **Column Prefixes**: All column names have a prefix (`columns_prefix`) to help prevent naming conflicts when integrating with other tables or databases.
- **Tracking Changes**: The `last_changed` column lists the columns that were updated in the most recent update run, making it easier to track what changed and when.
- **Timestamps**: `last_polled` records when the data was last fetched from the API, while `last_updated` shows when any of the data in that row was last modified.

### .env Configuration
The scripts require a `.env` file for setting API keys and database credentials. An example `.env` file looks like:

```env
# Alpha Vantage API Key
ALPHAVANTAGE_API_KEY=your_alphavantage_api_key_here

# Database credentials
DB_NAME=markets_db
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=localhost
DB_CHARSET=utf8

```

The `.env` file helps keep sensitive credentials out of the source code, and allows easy configuration without editing the script directly.

To install and set up the scripts from your GitHub repository into `/home/user/python/earnings-calendar`, create a virtual environment, run the scripts manually, and set it up with a cronjob to run every night at 2 AM, follow these steps:

### 1. Clone the Repository
First, you need to clone the repository to the desired directory.

```bash
cd /home/user/python
git clone https://github.com/drhdev/earnings-calendar.git
```

This command will clone the repository into `/home/user/python/earnings-calendar`.

### 2. Create a Virtual Environment
Navigate to the project directory and create a virtual environment to isolate dependencies.

```bash
cd /home/user/python/earnings-calendar
python3 -m venv venv
```

This will create a virtual environment named `venv` inside the `/home/user/python/earnings-calendar` directory.

### 3. Activate the Virtual Environment
To install dependencies and run the script, first activate the virtual environment.

```bash
source venv/bin/activate
```

You should now see `(venv)` at the beginning of your terminal prompt, indicating that the virtual environment is active.

### 4. Install Dependencies
Next, install the dependencies listed in `requirements.txt`:

```bash
pip install -r requirements.txt
```

This command will install all the necessary packages required by the script, such as `requests`, `SQLAlchemy`, `pymysql`, and `python-dotenv`.

### 5. Set Up the `.env` File
Create an `.env` file to configure your credentials:

```bash
cp .env.example .env
```

Open the `.env` file and update the values for the Alpha Vantage API key and the database credentials:

```env
ALPHAVANTAGE_API_KEY=your_alphavantage_api_key_here
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_HOST=localhost
DB_CHARSET=utf8
```

### 6. Run the Scripts Manually
You can now run the scripts manually to verify that it works as expected:

```bash
python earnings-calendar.py
python companiesmarketcap.py
```

If you want to run them in verbose mode, use:

```bash
python earnings-calendar.py -v
python companiesmarketcap.py -v
```

### 7. Schedule a Cronjob to Run Every Night at 2 AM
To run the scripts every night at 2 AM and 2:15 AM, add cronjobs for the user.

First, edit the user's cron jobs:

```bash
crontab -e
```

Add the following line to schedule the script:

```cron
0 2 * * * /home/user/python/earnings-calendar/venv/bin/python /home/user/python/earnings-calendar/earnings-calendar.py >> /home/user/python/earnings-calendar/earnings-calendar-cronjob.log 2>&1
15 2 * * * /home/user/python/earnings-calendar/venv/bin/python /home/user/python/earnings-calendar/companiesmarketcap.py >> /home/user/python/earnings-calendar/companiesmarketcap-cronjob.log 2>&1
```

The firstcronjob does the following:
- **`0 2 * * *`**: Runs the job every day at 2:00 AM.
- **`/home/user/python/earnings-calendar/venv/bin/python`**: Uses the Python interpreter from the virtual environment.
- **`/home/user/python/earnings-calendar/earnings-calendar.py`**: Runs the script.
- **`>> /home/user/python/earnings-calendar/earnings-calendar-cronjob.log 2>&1`**: Redirects output and errors to `earnings-calendar-cronjob.log` for later review.

### Summary of Steps
1. Clone the GitHub repository.
2. Create and activate a virtual environment.
3. Install the required dependencies using `requirements.txt`.
4. Configure the `.env` file with API key and database details.
5. Run the script(s) manually to test it/them.
6. Set up a cronjob to run the scripts every night at 2 AM/2:15 AM.

This approach ensures that the scripts run in an isolated environment, making it easy to manage dependencies and debug any issues that arise. The cronjobs will ensure that the scripts run regularly, keeping the data up to date.
