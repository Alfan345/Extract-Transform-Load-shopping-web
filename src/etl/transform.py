import pandas as pd
import numpy as np
import re
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_price(price_str):
    
    try:
        # Extract numeric value from the price string
        price_match = re.search(r'[\d.]+', price_str)
        if not price_match:
            logger.warning(f"No numeric value found in price: '{price_str}'")
            return None
            
        price_usd = float(price_match.group())
        
        # Validate the price value (must be positive)
        if price_usd <= 0:
            logger.warning(f"Invalid price value (zero or negative): {price_usd}")
            return None
            
        # Convert to IDR with exchange rate of Rp16,000
        price_idr = price_usd * 16000
        
        # Round to 2 decimal places to avoid floating point issues
        return round(price_idr, 2)
    except Exception as e:
        logger.error(f"Error cleaning price '{price_str}': {e}")
        return None

def clean_rating(rating_str):
   
    try:
        # Handle empty or None rating
        if not rating_str or rating_str == "Invalid Rating":
            return None
            
        # Extract numeric value from the rating string
        rating_match = re.search(r'([\d.]+)', rating_str)
        if not rating_match:
            logger.warning(f"No numeric value found in rating: '{rating_str}'")
            return None
            
        rating = float(rating_match.group(1))
        
        # Validate rating value (typically between 0 and 5)
        if rating < 0 or rating > 5:
            logger.warning(f"Rating value out of range: {rating}")
            return None
            
        return rating
    except Exception as e:
        logger.error(f"Error cleaning rating '{rating_str}': {e}")
        return None

def clean_colors(colors_str):
   
    try:
        # Handle empty or None colors
        if not colors_str or colors_str == "0 Colors":
            return 0
            
        # Extract numeric value from the colors string
        colors_match = re.search(r'(\d+)', colors_str)
        if not colors_match:
            logger.warning(f"No numeric value found in colors: '{colors_str}'")
            return 0
            
        colors = int(colors_match.group(1))
        
        # Validate colors value (must be non-negative)
        if colors < 0:
            logger.warning(f"Invalid colors value (negative): {colors}")
            return 0
            
        return colors
    except Exception as e:
        logger.error(f"Error cleaning colors '{colors_str}': {e}")
        return 0

def clean_size(size_str):
    
    try:
        # Handle empty or None size
        if not size_str or size_str == "Size: Unknown":
            return "Unknown"
            
        # Remove "Size: " prefix
        clean_size = size_str.replace("Size: ", "").strip()
        
        # Validate size value (must not be empty)
        if not clean_size:
            logger.warning(f"Empty size value after cleaning: '{size_str}'")
            return "Unknown"
            
        return clean_size
    except Exception as e:
        logger.error(f"Error cleaning size '{size_str}': {e}")
        return "Unknown"

def clean_gender(gender_str):
    
    try:
        # Handle empty or None gender
        if not gender_str or gender_str == "Gender: Unknown":
            return "Unknown"
            
        # Remove "Gender: " prefix
        clean_gender = gender_str.replace("Gender: ", "").strip()
        
        # Validate gender value (must not be empty)
        if not clean_gender:
            logger.warning(f"Empty gender value after cleaning: '{gender_str}'")
            return "Unknown"
            
        return clean_gender
    except Exception as e:
        logger.error(f"Error cleaning gender '{gender_str}': {e}")
        return "Unknown"

def validate_product_data(row):
   
    # Check if title is valid
    if pd.isna(row['Title']) or row['Title'] == "Unknown Product":
        return False
        
    # Check if price is valid (not zero or None)
    if pd.isna(row['Price']) or row['Price'] <= 0:
        return False
        
    # Check if rating is valid
    if pd.isna(row['Rating']):
        return False
        
    # Check if size is valid
    if pd.isna(row['Size']) or row['Size'] == "Unknown":
        return False
        
    # Check if gender is valid
    if pd.isna(row['Gender']) or row['Gender'] == "Unknown":
        return False
        
    return True

def transform_data(df):
   
    logger.info("Starting data transformation")
    
    try:
        # Check if DataFrame is empty
        if df.empty:
            logger.error("Cannot transform empty DataFrame")
            return pd.DataFrame()
            
        # Make a copy to avoid modifying the original DataFrame
        transformed_df = df.copy()
        
        # Remove invalid products (Title is "Unknown Product")
        invalid_count = transformed_df[transformed_df['Title'] == "Unknown Product"].shape[0]
        if invalid_count > 0:
            logger.info(f"Removing {invalid_count} products with unknown title")
            transformed_df = transformed_df[transformed_df['Title'] != "Unknown Product"]
        
        # Transform Price column (USD to IDR)
        transformed_df['Price'] = transformed_df['Price'].apply(clean_price)
        
        # Transform Rating column
        transformed_df['Rating'] = transformed_df['Rating'].apply(clean_rating)
        
        # Transform Colors column
        transformed_df['Colors'] = transformed_df['Colors'].apply(clean_colors)
        
        # Transform Size column
        transformed_df['Size'] = transformed_df['Size'].apply(clean_size)
        
        # Transform Gender column
        transformed_df['Gender'] = transformed_df['Gender'].apply(clean_gender)
        
        # Log stats before removing null values
        null_counts = transformed_df.isnull().sum()
        logger.info(f"Null value counts before cleaning:\n{null_counts}")
        
        # Remove rows with null values
        null_rows_count = transformed_df.isnull().any(axis=1).sum()
        if null_rows_count > 0:
            logger.info(f"Removing {null_rows_count} rows with null values")
            transformed_df = transformed_df.dropna()
        
        # Remove rows with invalid data using the validation function
        valid_mask = transformed_df.apply(validate_product_data, axis=1)
        invalid_count = (~valid_mask).sum()
        if invalid_count > 0:
            logger.info(f"Removing {invalid_count} rows with invalid data")
            transformed_df = transformed_df[valid_mask]
        
        # Remove duplicate rows
        duplicate_count = transformed_df.duplicated().sum()
        if duplicate_count > 0:
            logger.info(f"Removing {duplicate_count} duplicate rows")
            transformed_df = transformed_df.drop_duplicates()
        
        # Fix any malformatted price values (like "8.322.559.999.999.990")
        # This might occur due to locale formatting issues
        price_mask = transformed_df['Price'] > 1e10  # Unreasonably large prices
        if price_mask.any():
            logger.warning(f"Found {price_mask.sum()} suspiciously large price values, capping them")
            # Either cap the values or exclude the rows
            transformed_df = transformed_df[~price_mask]
        
        # Ensure proper data types
        transformed_df = transformed_df.astype({
            'Title': 'string',
            'Price': 'float64',
            'Rating': 'float64',
            'Colors': 'int64',
            'Size': 'string',
            'Gender': 'string',
            'timestamp': 'string'
        })
        
        # Add a final check to make sure price values are positive
        zero_price_count = (transformed_df['Price'] <= 0).sum()
        if zero_price_count > 0:
            logger.warning(f"Removing {zero_price_count} rows with zero or negative price")
            transformed_df = transformed_df[transformed_df['Price'] > 0]
        
        logger.info(f"Data transformation completed. {len(transformed_df)} products after transformation")
        
        # Print a summary of the data types and sample values
        logger.info(f"Data types:\n{transformed_df.dtypes}")
        logger.info(f"Sample data:\n{transformed_df.head()}")
        
        return transformed_df
        
    except Exception as e:
        logger.error(f"Error during data transformation: {e}", exc_info=True)
        return pd.DataFrame()

if __name__ == "__main__":
    # For testing purposes
    from extract import extract_data
    
    raw_data = extract_data()
    transformed_data = transform_data(raw_data)
    
    print(f"Transformed {len(transformed_data)} products")
    print(transformed_data.head())
    print(transformed_data.dtypes)
    
    # Check for any remaining issues
    zero_prices = transformed_data[transformed_data['Price'] <= 0]
    if not zero_prices.empty:
        print(f"WARNING: Found {len(zero_prices)} products with zero or negative prices")
        print(zero_prices[['Title', 'Price']])
    
    null_values = transformed_data.isnull().sum().sum()
    if null_values > 0:
        print(f"WARNING: Found {null_values} null values in the transformed data")
        print(transformed_data.isnull().sum())