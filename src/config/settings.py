"""
Configuration settings for the ETL pipeline
"""

# PostgreSQL database settings (change as needed)
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_DB = "fashion_db"
POSTGRES_USER = "alfan"
POSTGRES_PASSWORD = "alfan"
POSTGRES_TABLE = "products"

# Google Sheets settings
GOOGLE_CREDENTIALS_PATH = "<path_to_your_google_credentials>.json"
GOOGLE_SPREADSHEET_ID = "<your_spreadsheet_id>"

# CSV settings
CSV_OUTPUT_PATH = "products.csv"

# Scraping settings
TARGET_URL = "https://fashion-studio.dicoding.dev/"
MAX_PAGES = 50
MAX_PRODUCTS = 1000
PAGE_DELAY = 0.5  # seconds between page requests

API_KEY = "your_api_key_here"
DATABASE_URL = "your_database_url_here"
LOG_LEVEL = "INFO"
TIMEOUT = 30  # seconds
RETRY_COUNT = 3  # number of retries for network requests