# Executive Summary

This project involves the creation of a comprehensive data pipeline, implementing ELT (Extract, Load, Transform) processes, and designing a structured data warehouse. The goal is to demonstrate the ability to handle large volumes of data, process and clean it, and integrate it into a meaningful business intelligence model.

The project consists of three main layers in the data warehouse:

1. **Bronze Layer (Raw Data)**: This layer stores the raw data as it is loaded from the source files. It serves as the foundation for the ETL process.
2. **Silver Layer (Cleansed Data)**: The data is cleaned, validated, and normalized in this layer. Inconsistencies, duplicates, and invalid values are addressed here to prepare the data for integration.
3. **Gold Layer (Integrated Data)**: This layer integrates and aggregates the cleansed data, applying business logic to form the final dimensions and fact tables for reporting.

The project also incorporates the use of a star schema model for data visualization and reporting, focusing on ease of access to key metrics. The final product includes an interactive dashboard and reports, offering valuable insights for decision-making.

---

### Key Features:
- **Data Cleaning and Validation**: Applied consistent rules across the data to standardize values, handle missing data, and remove duplicates.
- **Data Modeling**: Used a star schema to integrate the data from multiple sources into structured dimensions and fact tables.
- **Business Logic**: Transformed raw data into meaningful insights by aggregating data, applying business rules, and creating new calculated columns.
- **ETL Pipeline**: Built a fully automated pipeline to extract, load, and transform data from the source to the final data warehouse layer.

This project demonstrates a comprehensive understanding of data engineering concepts, including data extraction, transformation, and load processes, data quality assurance, and data modeling.



# Project Overview

## Problem Definition

The project aims to build a robust data pipeline and a data warehouse to support business decision-making processes. The data warehouse consists of three layers: Bronze, Silver, and Gold, each representing different stages of data processing. The challenge was to integrate data from multiple sources, clean and validate it, and organize it in a way that supports efficient querying and analysis.

## Data Sources

The data comes from several sources, including:
- **CRM System**: Customer and sales data, including customer details, sales transactions, and product information.
- **ERP System**: Product category and customer location information.

These datasets were loaded into the Bronze layer (raw data), cleaned and normalized in the Silver layer, and finally integrated into the Gold layer for reporting and analysis.

## Data Warehouse Architecture

The data warehouse is structured into three main layers:
1. **Bronze Layer (Raw Data)**: This layer stores unmodified raw data extracted from source files (CSV). The data is loaded into staging tables before being inserted into the Bronze layer.
2. **Silver Layer (Cleansed Data)**: Data from the Bronze layer is cleansed, normalized, and standardized. This involves handling missing values, removing duplicates, and applying business rules for data consistency.
3. **Gold Layer (Integrated Data)**: The data is integrated into a star schema, with dimensions representing customers, products, and other business entities. The fact table aggregates sales data, linking it to the customer and product dimensions.

The project is designed to handle large volumes of data, ensure data quality, and provide a scalable solution for business intelligence and reporting.

## Data Model

The data model is based on a **star schema** for simplicity and performance. It includes:
- **Fact Table**: `fact_sales` stores transactional data related to sales, linked to customer and product dimensions.
- **Dimension Tables**:
  - `dim_customer`: Contains customer information such as customer ID, name, gender, and marital status.
  - `dim_product`: Stores product information, including product ID, name, category, and cost.

---

## Process Overview

1. **Data Extraction**: Raw data is extracted from CSV files and loaded into staging tables.
2. **Data Loading**: Data is then loaded into the Bronze layer, followed by transformations into the Silver layer.
3. **Data Transformation**: Data is cleaned and normalized in the Silver layer, applying rules for missing values, duplicates, and inconsistencies.
4. **Data Integration**: The final data is integrated into the Gold layer, where dimensional and fact tables are created.

The entire process is automated using SQL-based procedures and views.


# Data Architecture and Methodology

## Architectural Approach

This project follows a modern analytical data warehouse architecture based on layered data processing and clear separation of concerns. The design emphasizes data quality, traceability, and scalability, while remaining simple enough to be easily understood and maintained.

The architecture is organized into four main areas:

- **Staging Area**
- **Bronze Layer (Raw)**
- **Silver Layer (Cleansed)**
- **Gold Layer (Business-Ready)**

Each layer has a well-defined responsibility and enforces progressively stronger data quality and business rules.

---

## Staging Area

The staging area is used as a transient landing zone for raw CSV files. Data is bulk-loaded from the file system into staging tables with minimal transformation.

The purpose of the staging area is to:

- Isolate file-level ingestion from business tables  
- Simplify debugging and reprocessing  
- Provide a controlled entry point for raw data  
- Decouple file formats from the Bronze layer schema  

No data cleansing or business logic is applied at this stage.

---

## Bronze Layer (Raw Data)

The Bronze layer stores raw data in a structured, relational format that closely mirrors the source systems. Columns are stored primarily as text (`NVARCHAR`) to avoid data loss during ingestion.

Key characteristics of the Bronze layer:

- One-to-one representation of source datasets  
- Full-load strategy with `TRUNCATE + INSERT`  
- Lineage metadata included for traceability:
  - Load identifier
  - Source file name
  - Load timestamp
- Minimal transformation logic  

The Bronze layer serves as the immutable historical foundation of the warehouse.

---

## Silver Layer (Cleansed and Standardized Data)

The Silver layer is responsible for enforcing data quality and preparing the data for analytical use. This is where most of the transformation logic is applied.

Key activities performed in this layer include:

- Data type conversion (e.g., text to dates and numerics)
- Standardization of categorical values (e.g., gender, country)
- Deduplication using window functions
- Handling of invalid, missing, or out-of-bound values
- Data enrichment and normalization
- Audit tables to capture anomalous records without losing corrected data

All Silver layer loads are implemented using a full-load strategy and consolidated into a single stored procedure to ensure consistency and repeatability.

---

## Gold Layer (Business and Analytics Layer)

The Gold layer represents the final, business-ready data model. It is designed to support reporting, analytics, and downstream BI tools.

Key design principles of the Gold layer:

- Star schema modeling
- Clear separation between fact and dimension tables
- Surrogate keys for analytical joins
- Business logic applied at the semantic level
- Implemented using SQL Views rather than physical tables

The Gold layer integrates data from multiple Silver tables, applies business rules, and exposes a clean and intuitive model for end users.

---

## Data Modeling Strategy

The final model follows a **star schema** consisting of:

- **Fact table**:
  - `fact_sales`: transactional sales data

- **Dimension tables**:
  - `dim_customer`: integrated customer attributes
  - `dim_product`: current-state product attributes

This approach ensures:

- Simpler queries
- Better performance
- Clear analytical semantics
- Compatibility with BI tools

---

## Visual Documentation

To support clarity and communication, the following diagrams were created using Draw.io and included in the project documentation:

- Data Warehouse Architecture
- Data Flow Diagram
- Integration Model
- Star Schema Diagram

These visuals complement the written documentation and provide an at-a-glance understanding of the system design.

### Data Architecture

![Data Warehouse Architecture](documents/images/architecture.png) 

*Figure 1 – Data Warehouse architecture illustrating the Staging, Bronze, Silver, and Gold layers.*


