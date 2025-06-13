import pytest
import pandas as pd
import numpy as np
import sys
import os

# Add the src directory to the path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl.transform import (
    clean_price, clean_rating, clean_colors, 
    clean_size, clean_gender, transform_data, validate_product_data
)

def test_clean_price():
    """Test price cleaning and conversion"""
    assert clean_price("$45.99") == 735840.0  # 45.99 * 16000
    assert clean_price("$0") is None  # Invalid price should return None
    assert clean_price("Invalid") is None  # Invalid price should return None
    assert clean_price("") is None  # Empty price should return None

def test_clean_rating():
    """Test rating cleaning"""
    assert clean_rating("⭐ 4.5 / 5") == 4.5
    assert clean_rating("3 / 5") == 3.0
    assert clean_rating("Invalid Rating") is None  # Invalid rating should return None
    assert clean_rating("") is None  # Empty rating should return None

def test_clean_colors():
    """Test colors cleaning"""
    assert clean_colors("3 Colors") == 3
    assert clean_colors("1 Color") == 1
    assert clean_colors("0 Colors") == 0
    assert clean_colors("") == 0

def test_clean_size():
    """Test size cleaning"""
    assert clean_size("Size: M") == "M"
    assert clean_size("Size: XL") == "XL"
    assert clean_size("Size: ") == "Unknown"  # Empty size should return "Unknown"
    assert clean_size("") == "Unknown"  # Empty string should return "Unknown"

def test_clean_gender():
    """Test gender cleaning"""
    assert clean_gender("Gender: Men") == "Men"
    assert clean_gender("Gender: Women") == "Women"
    assert clean_gender("Gender: ") == "Unknown"  # Empty gender should return "Unknown"
    assert clean_gender("") == "Unknown"  # Empty string should return "Unknown"

def test_validate_product_data():
    """Test product data validation"""
    # Valid product
    valid_product = pd.Series({
        'Title': 'Test Product',
        'Price': 100000.0,
        'Rating': 4.5,
        'Colors': 3,
        'Size': 'M',
        'Gender': 'Men',
        'timestamp': '2023-01-01'
    })
    assert validate_product_data(valid_product) is True
    
    # Invalid products
    invalid_title = valid_product.copy()
    invalid_title['Title'] = "Unknown Product"
    assert validate_product_data(invalid_title) is False
    
    invalid_price = valid_product.copy()
    invalid_price['Price'] = 0.0
    assert validate_product_data(invalid_price) is False
    
    invalid_rating = valid_product.copy()
    invalid_rating['Rating'] = np.nan
    assert validate_product_data(invalid_rating) is False
    
    invalid_size = valid_product.copy()
    invalid_size['Size'] = "Unknown"
    assert validate_product_data(invalid_size) is False
    
    invalid_gender = valid_product.copy()
    invalid_gender['Gender'] = "Unknown"
    assert validate_product_data(invalid_gender) is False

def test_transform_data():
    """Test data transformation"""
    # Create a sample DataFrame for testing
    sample_data = {
        'Title': ['Product 1', 'Product 2', 'Unknown Product', 'Product 3'],
        'Price': ['$45.99', '$32.50', '$0', '$25.99'],
        'Rating': ['⭐ 4.5 / 5', '⭐ 3.0 / 5', 'Invalid Rating', '⭐ 4.8 / 5'],
        'Colors': ['3 Colors', '2 Colors', '0 Colors', '5 Colors'],
        'Size': ['Size: M', 'Size: L', 'Size: S', 'Size: XL'],
        'Gender': ['Gender: Men', 'Gender: Women', 'Gender: Unisex', 'Gender: Men'],
        'timestamp': ['2023-01-01', '2023-01-01', '2023-01-01', '2023-01-01']
    }
    df = pd.DataFrame(sample_data)
    
    # Transform the data
    transformed_df = transform_data(df)
    
    # Verify the transformation
    assert len(transformed_df) == 3  # "Unknown Product" should be removed
    assert 'Title' in transformed_df.columns
    assert 'Price' in transformed_df.columns
    assert 'Rating' in transformed_df.columns
    assert 'Colors' in transformed_df.columns
    assert 'Size' in transformed_df.columns
    assert 'Gender' in transformed_df.columns
    
    # Check data types
    assert transformed_df['Title'].dtype == 'string'
    assert transformed_df['Price'].dtype == 'float64'
    assert transformed_df['Rating'].dtype == 'float64'
    assert transformed_df['Colors'].dtype == 'int64'
    assert transformed_df['Size'].dtype == 'string'
    assert transformed_df['Gender'].dtype == 'string'
    
    # Check some transformed values
    assert transformed_df.iloc[0]['Price'] == 735840.0  # 45.99 * 16000
    assert transformed_df.iloc[0]['Rating'] == 4.5
    assert transformed_df.iloc[0]['Colors'] == 3
    assert transformed_df.iloc[0]['Size'] == 'M'
    assert transformed_df.iloc[0]['Gender'] == 'Men'

def test_transform_handles_empty_dataframe():
    """Test that transform_data can handle an empty DataFrame"""
    empty_df = pd.DataFrame()
    result = transform_data(empty_df)
    assert result.empty

def test_transform_handles_malformed_prices():
    """Test handling of malformed price values"""
    # Create a sample DataFrame with a malformed price
    sample_data = {
        'Title': ['Product 1'],
        'Price': ['$8,322,559,999,999,990.00'],
        'Rating': ['⭐ 4.5 / 5'],
        'Colors': ['3 Colors'],
        'Size': ['Size: M'],
        'Gender': ['Gender: Men'],
        'timestamp': ['2023-01-01']
    }
    df = pd.DataFrame(sample_data)
    
    # Transform the data
    transformed_df = transform_data(df)
    
    # Verify the transformation - should extract the numeric part of the price
    assert not transformed_df.empty
    assert transformed_df.iloc[0]['Price'] == 128000.0  # 8 * 16000