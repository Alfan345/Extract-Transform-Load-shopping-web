import pytest
import pandas as pd
import os
import sys
import tempfile

# Add the src directory to the path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl.load import save_to_csv, load_data

def test_save_to_csv():
    """Test saving data to CSV"""
    # Create a sample DataFrame for testing
    sample_data = {
        'Title': ['Product 1', 'Product 2'],
        'Price': [735840.0, 520000.0],  # 45.99 * 16000, 32.50 * 16000
        'Rating': [4.5, 3.0],
        'Colors': [3, 2],
        'Size': ['M', 'L'],
        'Gender': ['Men', 'Women'],
        'timestamp': ['2023-01-01', '2023-01-01']
    }
    df = pd.DataFrame(sample_data)
    
    # Create a temporary file for testing
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
        tmp_path = tmp.name
    
    try:
        # Save the DataFrame to CSV
        result = save_to_csv(df, tmp_path)
        
        # Verify the result
        assert result is True
        assert os.path.exists(tmp_path)
        
        # Read the CSV back and verify its contents
        df_read = pd.read_csv(tmp_path)
        assert len(df_read) == 2
        assert all(col in df_read.columns for col in sample_data.keys())
        
    finally:
        # Clean up
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

def test_load_data_handles_empty_dataframe():
    """Test that load_data can handle an empty DataFrame"""
    empty_df = pd.DataFrame()
    result = load_data(empty_df)
    
    # All loading operations should fail with an empty DataFrame
    assert result['csv'] is False
    assert result['google_sheets'] is False
    assert result['postgresql'] is False

def test_load_data_csv_only(monkeypatch):
    """Test loading data to CSV only"""
    # Create a sample DataFrame for testing
    sample_data = {
        'Title': ['Product 1', 'Product 2'],
        'Price': [735840.0, 520000.0],  # 45.99 * 16000, 32.50 * 16000
        'Rating': [4.5, 3.0],
        'Colors': [3, 2],
        'Size': ['M', 'L'],
        'Gender': ['Men', 'Women'],
        'timestamp': ['2023-01-01', '2023-01-01']
    }
    df = pd.DataFrame(sample_data)
    
    # Mock the other saving functions to return False
    import etl.load
    
    # Create a temporary file for testing
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
        tmp_path = tmp.name
    
    try:
        # Save original functions
        original_save_to_csv = etl.load.save_to_csv
        original_save_to_google_sheets = etl.load.save_to_google_sheets
        original_save_to_postgresql = etl.load.save_to_postgresql
        
        # Create mock functions
        def mock_save_to_csv(df, output_path="products.csv"):
            return original_save_to_csv(df, tmp_path)
            
        def mock_save_to_google_sheets(*args, **kwargs):
            return False
            
        def mock_save_to_postgresql(*args, **kwargs):
            return False
        
        # Apply mocks
        monkeypatch.setattr(etl.load, 'save_to_csv', mock_save_to_csv)
        monkeypatch.setattr(etl.load, 'save_to_google_sheets', mock_save_to_google_sheets)
        monkeypatch.setattr(etl.load, 'save_to_postgresql', mock_save_to_postgresql)
        
        # Test load_data
        result = load_data(df)
        
        # Verify the result
        assert result['csv'] is True
        assert result['google_sheets'] is False
        assert result['postgresql'] is False
        
        # Verify the CSV file
        assert os.path.exists(tmp_path)
        df_read = pd.read_csv(tmp_path)
        assert len(df_read) == 2
        
    finally:
        # Clean up
        if os.path.exists(tmp_path):
            os.remove(tmp_path)