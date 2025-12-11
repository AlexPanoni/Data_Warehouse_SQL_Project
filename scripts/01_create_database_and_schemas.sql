/*============================================================================
   Script: 01_create_database_and_schemas.sql
   Purpose: Initializes the Data Warehouse environment by creating the main 
            database and the Medallion Architecture schemas (bronze, silver, gold).
 

   Description:
   This script sets up the foundational structure for the SQL Server Data Warehouse.
   It creates the primary database (DataWarehouse) and the three core schemas 
   representing the layers of the Medallion Architecture:
     - bronze: Raw data ingestion
     - silver: Cleaned and standardized data
     - gold: Business-ready, analytics-focused data

   This is the first step in building the full data pipeline.
 ============================================================================*/


USE master;

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
