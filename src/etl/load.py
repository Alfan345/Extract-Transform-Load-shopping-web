import pandas as pd
import os
import logging
from datetime import datetime
import json
import psycopg2
from sqlalchemy import create_engine
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def save_to_csv(df, output_path=None):
   
    # Import here to avoid circular imports
    from config.settings import CSV_OUTPUT_PATH
    
    # Use settings if not provided
    output_path = output_path or CSV_OUTPUT_PATH
    
    try:
        # Check if DataFrame is empty
        if df.empty:
            logger.warning("Cannot save empty DataFrame to CSV")
            return False
            
        # Create directory if it doesn't exist
        directory = os.path.dirname(output_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            
        df.to_csv(output_path, index=False)
        logger.info(f"Data saved to CSV: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        return False

def save_to_google_sheets(df, spreadsheet_id=None, credentials_path=None):
    
    # Import here to avoid circular imports
    from config.settings import GOOGLE_CREDENTIALS_PATH, GOOGLE_SPREADSHEET_ID
    
    # Use settings if not provided
    credentials_path = credentials_path or GOOGLE_CREDENTIALS_PATH
    spreadsheet_id = spreadsheet_id or GOOGLE_SPREADSHEET_ID
    
    try:
        # Check if DataFrame is empty
        if df.empty:
            logger.warning("Cannot save empty DataFrame to Google Sheets")
            return False
            
        # Check if credentials file exists
        if not os.path.exists(credentials_path):
            logger.error(f"Google API credentials file not found: {credentials_path}")
            return False
            
        # Authenticate with Google Sheets API
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path, 
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        
        # Create service for Sheets API only (no Drive API needed)
        sheets_service = build('sheets', 'v4', credentials=credentials)
        
        # Prepare data for Google Sheets
        # Convert dataframe to list of lists
        values = [df.columns.tolist()]  # Header
        values.extend(df.values.tolist())  # Data
        
        # Clear existing data
        try:
            sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range='Sheet1'
            ).execute()
            
            # Write data to sheet
            body = {
                'values': values
            }
            result = sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range='Sheet1',
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logger.info(f"Data saved to Google Sheets: {spreadsheet_id}")
            logger.info(f"Spreadsheet URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit")
            
            return True
        except HttpError as e:
            logger.error(f"Error updating Google Sheet: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Error saving to Google Sheets: {e}")
        return False

def save_to_postgresql(df, host=None, port=None, dbname=None, 
                      user=None, password=None, table_name=None):
   
    # Import here to avoid circular imports
    from config.settings import (
        POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
        POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_TABLE
    )
    
    # Use settings if not provided
    host = host or POSTGRES_HOST
    port = port or POSTGRES_PORT
    dbname = dbname or POSTGRES_DB
    user = user or POSTGRES_USER
    password = password or POSTGRES_PASSWORD
    table_name = table_name or POSTGRES_TABLE
    
    try:
        # Check if DataFrame is empty
        if df.empty:
            logger.warning("Cannot save empty DataFrame to PostgreSQL")
            return False
            
        # Create connection string
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        
        # Create engine
        engine = create_engine(connection_string)
        
        # Save data to PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        logger.info(f"Data saved to PostgreSQL: {dbname}.{table_name}")
        return True
    except Exception as e:
        logger.error(f"Error saving to PostgreSQL: {e}")
        return False

def load_data(df, save_csv=True, save_sheets=True, save_postgres=True):
   
    results = {}
    
    # Save to CSV
    if save_csv:
        results['csv'] = save_to_csv(df)
        
    # Save to Google Sheets
    if save_sheets:
        results['google_sheets'] = save_to_google_sheets(df)
        
    # Save to PostgreSQL
    if save_postgres:
        results['postgresql'] = save_to_postgresql(df)
        
    return results

if __name__ == "__main__":
    # For testing purposes
    from extract import extract_data
    from transform import transform_data
    
    raw_data = extract_data()
    transformed_data = transform_data(raw_data)
    load_results = load_data(transformed_data)
    
    print(f"Load results: {load_results}")