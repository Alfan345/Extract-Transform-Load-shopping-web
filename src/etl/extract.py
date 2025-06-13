import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import time
from datetime import datetime
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_page_content(page_number):
    """
    Fetch content from a specific page of the fashion-studio website
    
    Args:
        page_number (int): The page number to scrape
        
    Returns:
        BeautifulSoup: Parsed HTML content or None if there was an error
    """
    # First page uses base URL, subsequent pages use /page{number} format
    if page_number == 1:
        url = "https://fashion-studio.dicoding.dev"
    else:
        url = f"https://fashion-studio.dicoding.dev/page{page_number}"
    
    try:
        logger.info(f"Fetching URL: {url}")
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup
    except requests.RequestException as e:
        logger.error(f"Error fetching URL {url}: {e}")
        return None

def parse_product_card(card):
    """
    Extract product information from a product card element
    
    Args:
        card (BeautifulSoup element): HTML element containing product data
        
    Returns:
        dict: Dictionary containing product information
    """
    try:
        # Extract product details from the product-details div
        details_div = card.find('div', class_='product-details')
        
        if not details_div:
            logger.warning("Product details div not found in card")
            return None
        
        # Extract product title - now using h3 instead of h2
        title_element = details_div.find('h3', class_='product-title')
        title = title_element.text.strip() if title_element else "Unknown Product"
        
        # Extract product price - from the price span inside price-container div
        price_element = details_div.find('span', class_='price')
        price = price_element.text.strip() if price_element else "0"
        
        # Extract all paragraph elements that contain the product details
        p_elements = details_div.find_all('p', style=True)
        
        # Initialize default values
        rating = "Invalid Rating"
        colors = "0 Colors"
        size = "Size: Unknown"
        gender = "Gender: Unknown"
        
        # Process each paragraph to extract different details
        for p in p_elements:
            text = p.text.strip()
            
            if "Rating:" in text:
                rating = text.replace("Rating:", "").strip()
            elif "Colors" in text:
                colors = text.strip()
            elif "Size:" in text:
                size = text.strip()
            elif "Gender:" in text:
                gender = text.strip()
        
        # Add timestamp for when this data was scraped
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return {
            "Title": title,
            "Price": price,
            "Rating": rating,
            "Colors": colors,
            "Size": size,
            "Gender": gender,
            "timestamp": timestamp
        }
    except Exception as e:
        logger.error(f"Error parsing product card: {e}")
        return None

def extract_data():
    """
    Main function to extract data from fashion-studio website
    
    Returns:
        pandas.DataFrame: Raw scraped data
    """
    logger.info("Starting data extraction from fashion-studio website")
    
    all_products = []
    total_pages = 50  # Based on the requirements
    
    try:
        for page in range(1, total_pages + 1):
            logger.info(f"Scraping page {page} of {total_pages}")
            
            soup = get_page_content(page)
            if not soup:
                logger.warning(f"Skipping page {page} due to error")
                continue
            
            # Find all product cards on the page - using correct class name 'collection-card'
            product_cards = soup.find_all('div', class_='collection-card')
            
            if not product_cards:
                logger.warning(f"No product cards found on page {page}")
                break  # If no products found, likely reached end of pagination
                
            # Parse each product card
            for card in product_cards:
                product_data = parse_product_card(card)
                if product_data:
                    all_products.append(product_data)
            
            # Add a small delay to avoid overwhelming the server
            time.sleep(0.5)
            
            # Check if we've reached 1000 products as required
            if len(all_products) >= 1000:
                logger.info(f"Reached 1000 products, stopping extraction")
                break
                
        logger.info(f"Data extraction completed. Total products scraped: {len(all_products)}")
        
        # Convert to DataFrame
        df = pd.DataFrame(all_products)
        return df
    
    except Exception as e:
        logger.error(f"Unexpected error during extraction: {e}")
        # Return empty DataFrame in case of error
        return pd.DataFrame()

if __name__ == "__main__":
    # For testing purposes
    data = extract_data()
    print(f"Extracted {len(data)} products")
    print(data.head())