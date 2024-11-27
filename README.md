# Automated Data Pipeline for NYC Yellow Taxi Data

## Project Overview

This project demonstrates the development of an automated data pipeline for processing and analyzing NYC Yellow Taxi trip data. The pipeline leverages **Azure Data Factory**, **Databricks**, and **Azure Data Lake Storage (ADLS)** to automate data ingestion, transformation, and storage, ensuring efficient batch processing. Processed data is visualized using **Power BI**, enabling actionable insights into taxi trip metrics.

---

## Key Features

- **Automated Batch Processing**:  
  The pipeline detects and ingests updated NYC Yellow Taxi data on a monthly basis. The system ensures that only new data is ingested, processed, and stored, while maintaining a clean metadata structure for tracking processed files.

- **Dynamic Metadata Management**:  
  Tracks and manages processed files to avoid duplication and ensure efficient data updates.

- **Scalable Data Transformation**:  
  Cleans, validates, and transforms data using **PySpark** on **Databricks** for analysis-ready datasets.

- **Interactive Reporting**:  
  Creates interactive **Power BI** dashboards for actionable insights, including metrics like trip duration, revenue, and passenger statistics.

---

## Architecture

The pipeline consists of the following components:

### 1. **Data Ingestion**
- Automated ingestion of NYC Yellow Taxi data using **Azure Data Factory**.
- Raw data is stored in **Azure Data Lake Storage (ADLS)** for further processing.
- Batch frequency aligns with the data provider's monthly update schedule.

### 2. **Data Transformation**
- Data is cleaned, validated, and processed using **PySpark** in **Databricks**.
- Missing values are handled, invalid data is removed, and additional metrics (e.g., trip duration) are calculated.

### 3. **Data Storage**
- Processed data is saved back into **ADLS**, ensuring scalability and accessibility for downstream analytics.

### 4. **Data Visualization**
- **Power BI** connects to the processed data for creating dashboards and visual reports.
- Reports include key metrics such as total trips, fare amounts, and passenger distribution.

---

## Technologies Used

- **Azure Data Factory**: Orchestrates the pipeline workflows and schedules batch processes.
- **Databricks**: Executes data transformation and cleaning tasks with PySpark.
- **Azure Data Lake Storage (ADLS)**: Stores raw and processed data for scalable access.
- **Power BI**: Provides interactive dashboards and analytics.
- **Python**: Powers custom data processing and workflow automation.
- **PySpark**: Handles scalable and distributed data transformation.

---

## How It Works

### 1. **Automated Batch Processing**
- The pipeline detects updated data files monthly.
- A metadata-driven workflow ensures only new files are processed, avoiding duplication.

### 2. **Data Cleaning and Transformation**
- Missing values in critical columns are dropped.
- Invalid rows (e.g., negative trip distances or fare amounts) are filtered out.
- New columns, such as trip duration, are calculated.

### 3. **Data Storage and Visualization**
- Processed data is saved in **ADLS** in a structured format.
- **Power BI** connects to the processed data to generate real-time dashboards.

---

## Sample Insights from Power BI Dashboards

- Total trips over time.
- Revenue analysis by month.
- Passenger count distribution.
- Trip duration and distance trends.

---

By combining the power of Azure services, Databricks, and Power BI, this pipeline ensures seamless processing, scalability, and actionable insights from NYC Yellow Taxi data.
