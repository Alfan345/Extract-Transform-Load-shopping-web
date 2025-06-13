from setuptools import setup, find_packages

setup(
    name='etl-pipeline',
    version='0.1.0',
    author='Your Name',
    author_email='your.email@example.com',
    description='A modular ETL pipeline for data extraction, transformation, and loading.',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'python-crontab ~= 3.2',
        'sqlalchemy ~= 2.0',
        'psycopg2-binary ~= 2.9',
        'pandas ~= 2.2',
        'requests ~= 2.32',
        'beautifulsoup4 ~= 4.12',
        'google-auth ~= 2.36',
        'google-api-python-client ~= 2.152',
        'pytest-cov ~= 6.0'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)