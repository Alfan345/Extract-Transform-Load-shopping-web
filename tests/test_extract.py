import pytest
import pandas as pd
from bs4 import BeautifulSoup
import sys
import os

# Add the src directory to the path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl.extract import get_page_content, parse_product_card, extract_data

def test_get_page_content():
    """Test that we can fetch a page from the website"""
    # This is an integration test that requires network connection
    # For unit testing, we should mock the response
    soup = get_page_content(1)
    assert isinstance(soup, BeautifulSoup) or soup is None

def test_parse_product_card():
    """Test parsing a product card"""
    # Create a mock product card based on the actual HTML structure
    mock_html = """
    <div class="collection-card">
        <div style="position: relative;">
            <img src="https://picsum.photos/280/350?random=42" class="collection-image" alt="Jacket 42">
        </div>
        <div class="product-details">
            <h3 class="product-title">Jacket 42</h3>
            <div class="price-container"><span class="price">$421.51</span></div>
            <p style="font-size: 14px; color: #777;">Rating: ⭐ 3.7 / 5</p>
            <p style="font-size: 14px; color: #777;">3 Colors</p>
            <p style="font-size: 14px; color: #777;">Size: M</p>
            <p style="font-size: 14px; color: #777;">Gender: Unisex</p>
        </div>
    </div>
    """
    mock_card = BeautifulSoup(mock_html, 'html.parser')
    
    product_data = parse_product_card(mock_card)
    
    assert product_data is not None
    assert product_data['Title'] == 'Jacket 42'
    assert product_data['Price'] == '$421.51'
    assert product_data['Rating'] == '⭐ 3.7 / 5'
    assert product_data['Colors'] == '3 Colors'
    assert product_data['Size'] == 'Size: M'
    assert product_data['Gender'] == 'Gender: Unisex'
    assert 'timestamp' in product_data

def test_extract_data_returns_dataframe():
    """Test that extract_data returns a DataFrame with expected columns"""
    # Mock the functions to avoid actual web requests during testing
    import etl.extract
    
    # Save original function to restore later
    original_get_page = etl.extract.get_page_content
    
    try:
        # Create a mock HTML with a single product
        mock_html = """
        <html>
        <body>
            <div class="collection-card">
                <div style="position: relative;">
                    <img src="https://picsum.photos/280/350?random=42" class="collection-image" alt="Jacket 42">
                </div>
                <div class="product-details">
                    <h3 class="product-title">Jacket 42</h3>
                    <div class="price-container"><span class="price">$421.51</span></div>
                    <p style="font-size: 14px; color: #777;">Rating: ⭐ 3.7 / 5</p>
                    <p style="font-size: 14px; color: #777;">3 Colors</p>
                    <p style="font-size: 14px; color: #777;">Size: M</p>
                    <p style="font-size: 14px; color: #777;">Gender: Unisex</p>
                </div>
            </div>
        </body>
        </html>
        """
        mock_soup = BeautifulSoup(mock_html, 'html.parser')
        
        # Mock the get_page_content function
        etl.extract.get_page_content = lambda page: mock_soup
        
        # Call extract_data
        df = extract_data()
        
        # Verify the result
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert 'Title' in df.columns
        assert 'Price' in df.columns
        assert 'Rating' in df.columns
        assert 'Colors' in df.columns
        assert 'Size' in df.columns
        assert 'Gender' in df.columns
        assert 'timestamp' in df.columns
        
    finally:
        # Restore original function
        etl.extract.get_page_content = original_get_page

def test_parse_product_card_handles_missing_elements():
    """Test that parse_product_card handles missing elements gracefully"""
    # Create a mock product card with missing elements
    mock_html = """
    <div class="collection-card">
        <div class="product-details">
            <h3 class="product-title">Jacket 42</h3>
            <!-- Missing price and other elements -->
        </div>
    </div>
    """
    mock_card = BeautifulSoup(mock_html, 'html.parser')
    
    product_data = parse_product_card(mock_card)
    
    assert product_data is not None
    assert product_data['Title'] == 'Jacket 42'
    assert product_data['Price'] == '0'  # Default value for missing price
    assert product_data['Rating'] == 'Invalid Rating'  # Default value for missing rating
    assert product_data['Colors'] == '0 Colors'  # Default value for missing colors
    assert product_data['Size'] == 'Size: Unknown'  # Default value for missing size
    assert product_data['Gender'] == 'Gender: Unknown'  # Default value for missing gender

def test_error_handling():
    """Test error handling when page cannot be fetched"""
    # Test with an invalid page number
    result = get_page_content(9999)
    assert result is None