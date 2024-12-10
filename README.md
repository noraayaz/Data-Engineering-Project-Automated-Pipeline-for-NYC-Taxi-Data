# Automated Data Pipeline for NYC Yellow Taxi Data

## Project Overview
This project showcases the development of an automated data pipeline for processing and analyzing NYC Yellow Taxi trip data. The pipeline integrates **Azure Data Factory**, **Databricks**, **Azure Data Lake Storage (ADLS)**, and **Azure Synapse Analytics** to automate data ingestion, transformation, and analysis, ensuring efficient batch processing and scalable analytics. The processed data is analyzed in Azure Synapse Analytics, offering advanced insights into taxi trip metrics.

---

## Key Features

- **Automated Batch Processing**  
  Detects and ingests updated NYC Yellow Taxi data on a monthly basis. The system ensures that only new data is ingested, processed, and stored, while maintaining a clean metadata structure for tracking processed files.

- **Dynamic Metadata Management**  
  Tracks and manages processed files to avoid duplication and ensure efficient data updates.

- **Scalable Data Transformation**  
  Cleans, validates, and transforms data using **PySpark** on **Databricks** for analysis-ready datasets.

- **Advanced Analytics with Azure Synapse**  
  Processed data is queried and analyzed in **Azure Synapse Analytics**, leveraging its scalability for large datasets and its ability to deliver actionable insights.

---

## Architecture

### 1. Data Ingestion
- Automated ingestion of NYC Yellow Taxi data using **Azure Data Factory**.
- Raw data is stored in **Azure Data Lake Storage (ADLS)** for further processing.
- Batch frequency aligns with the data provider's monthly update schedule.

### 2. Data Transformation
- Data is cleaned, validated, and processed using **PySpark** in **Databricks**.
- Missing values are handled, invalid data is removed, and additional metrics (e.g., trip duration) are calculated.

### 3. Data Storage
- Processed data is saved back into **ADLS** in a structured format, ensuring scalability and accessibility.

### 4. Data Analysis
- **Azure Synapse Analytics** is used to query and analyze processed data for trends and insights.
- Complex analytical queries are run efficiently, providing detailed reports and visualizations.

---

## Technologies Used

- **Azure Data Factory**: Orchestrates the pipeline workflows and schedules batch processes.
- **Databricks**: Executes data transformation and cleaning tasks with **PySpark**.
- **Azure Data Lake Storage (ADLS)**: Stores raw and processed data for scalable access.
- **Azure Synapse Analytics**: Performs advanced data analysis and interactive querying.
- **Python**: Powers custom data processing and workflow automation.
- **PySpark**: Handles scalable and distributed data transformation.

---

## How It Works

### 1. Automated Batch Processing
- The pipeline detects updated data files monthly.
- A metadata-driven workflow ensures only new files are processed, avoiding duplication.

### 2. Data Cleaning and Transformation
- Missing values in critical columns are dropped.
- Invalid rows (e.g., negative trip distances or fare amounts) are filtered out.
- New columns, such as trip duration, are calculated.

### 3. Data Storage and Analysis
- Processed data is saved in **ADLS** in a structured format.
- **Azure Synapse Analytics** is used to execute SQL-based queries and advanced data analysis.

---

## Sample Insights from Azure Synapse Analytics

- Total trips over time.
- Revenue analysis by month.
- Passenger count distribution.
- Trip duration and distance trends.

---

By combining the power of **Azure Data Factory**, **Databricks**, **Azure Data Lake Storage**, and **Azure Synapse Analytics**, this pipeline ensures seamless processing, scalability, and actionable insights from NYC Yellow Taxi data.
