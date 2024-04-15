# Crash Analysis Case Study

This project analyzes crash data to extract insights using PySpark.

## Overview

The Crash Analysis Case Study project aims to analyze crash data to derive meaningful insights. It processes data stored in CSV files, performs various analyses using PySpark, and generates output in different formats.

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/your_username/crash-analysis-case-study.git
    ```

2. Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

1. Ensure that you have Apache Spark installed on your system.
2. Modify the configuration file (`config.json`) with the appropriate file paths and settings.
3. Run the main script:

    ```bash
    spark-submit --master local[*] main.py path/to/config.json
    ```

## Creating the JSON Config File

To configure the project, you need to create a JSON config file that specifies the data sources and output targets. Follow these steps to create the JSON config file:

Open a text editor.

Copy and paste the JSON structure into the editor from /config/config.json:

1. Replace "path/to/charges.csv", "path/to/damages.csv", etc., with the actual paths to your input CSV files.

2. Replace "path/to/output/directory" with the path to the directory (add file:/// if you want to store the file locally) where you want to save the output files (valid formats - csv, orc, parquet, json).

3. Save the file with a .json extension, for example, config.json.

## Project Structure

crash-analysis-case-study/
│
├── src/
│ ├── init.py
│ ├── utils.py
│ ├── df_store.py
│ └── crash_analysis.py
│
├── config/
│ └── config.json
│
├── main.py
└── requirements.txt

- `src/`: Contains Python source code files.
- `config/`: Stores the configuration file.
- `main.py`: Entry point of the application.
- `requirements.txt`: List of Python dependencies.
