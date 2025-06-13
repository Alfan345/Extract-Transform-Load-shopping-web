# ETL Pipeline Project

This project implements an ETL (Extract, Transform, Load) pipeline designed to handle data extraction from various sources, transformation of that data, and loading it into a specified repository.

## Project Structure

```
etl-pipeline
├── src
│   ├── etl
│   │   ├── __init__.py
│   │   ├── extract.py
│   │   ├── transform.py
│   │   ├── load.py
│   │   └── utils.py
│   ├── config
│   │   ├── __init__.py
│   │   └── settings.py
│   └── main.py
├── tests
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── requirements.txt
├── setup.py
├── .gitignore
└── README.md
```

## Installation

To set up the project, clone the repository and install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

To run the ETL pipeline, execute the `main.py` file:

```bash
python src/main.py
```

## Components

- **Extraction**: The `extract.py` file contains functions to extract data from various sources, including web scraping. It includes error handling to manage potential issues during data extraction.
  
- **Transformation**: The `transform.py` file includes functions for cleaning and converting data types. It implements error handling to ensure data quality.

- **Loading**: The `load.py` file is responsible for loading the transformed data into the desired repository, such as saving to a CSV file or uploading to Google Sheets. It includes error handling to manage loading failures.

- **Utilities**: The `utils.py` file contains utility functions that are used across the ETL process, such as logging or helper functions.

## Testing

Unit tests are provided for each component of the ETL pipeline. To run the tests, use:

```bash
pytest tests/
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for details.