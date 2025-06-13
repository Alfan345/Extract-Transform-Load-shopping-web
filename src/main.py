from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_etl_pipeline():
    """Run the complete ETL pipeline"""
    logger.info("Starting ETL pipeline")
    
    # Extract data
    logger.info("Extracting data...")
    raw_data = extract_data()
    
    # Transform data
    logger.info("Transforming data...")
    transformed_data = transform_data(raw_data)
    
    # Load data
    logger.info("Loading data...")
    results = load_data(transformed_data)
    
    # Log results
    for storage, success in results.items():
        status = "Success" if success else "Failed"
        logger.info(f"{storage.upper()} loading: {status}")
    
    logger.info("ETL pipeline completed")
    return results

if __name__ == "__main__":
    run_etl_pipeline()