def log_message(message):
    # Function to log messages for the ETL process
    print(f"[LOG] {message}")

def handle_error(error):
    # Function to handle errors and log them
    print(f"[ERROR] {error}")

def validate_data(data):
    # Function to validate data before processing
    if not data:
        handle_error("No data provided.")
        return False
    return True

# Additional utility functions can be added here as needed.