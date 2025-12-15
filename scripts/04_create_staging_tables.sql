/*====================================================================================
Script:        04_create_staging_tables.sql
Layer:         Bronze (Staging)

Purpose:       Creates all staging tables used as the initial landing zone for raw CSV data.
                These tables mirror the input file structures and preserve raw schema fidelity.
 
Description:   This script defines the staging schema and its tables, ensuring a stable,
                 consistent structure for subsequent Bronze ingestion. No transformations
                 occur at this stage; data is loaded "as received" from the source files.
 
Notes:         
				- Executed before 05_load_bronze.sql.
                - Staging tables are truncated and reloaded during full refresh operations.
=====================================================================================*/


-- Create staging tables (matches the CSV columns) ----------

-- CRM: stg_crm_cust_info 
IF OBJECT_ID('dbo.stg_crm_cust_info','U') IS NULL
BEGIN
  CREATE TABLE dbo.stg_crm_cust_info(
    cst_id VARCHAR(MAX),
    cst_key VARCHAR(MAX),
    cst_firstname VARCHAR(MAX),
	cst_lastname VARCHAR(MAX),
    cst_marital_status VARCHAR(MAX),
    cst_gndr VARCHAR(MAX),
    cst_create_date VARCHAR(MAX)
  );
  PRINT 'Created dbo.stg_crm_cust_info';
END

-- CRM: stg_crm_prd_info
IF OBJECT_ID('dbo.stg_crm_prd_info','U') IS NULL
BEGIN
  CREATE TABLE dbo.stg_crm_prd_info(
    prd_id VARCHAR(MAX),
    prd_key VARCHAR(MAX),
    prd_nm VARCHAR(MAX),
    prd_cost VARCHAR(MAX),
    prd_line VARCHAR(MAX),
    prd_start_dt VARCHAR(MAX),
    prd_end_dt VARCHAR(MAX)
  );
  PRINT 'Created dbo.stg_crm_prd_info';
END

-- CRM: stg_crm_sales_details
IF OBJECT_ID('dbo.stg_crm_sales_details','U') IS NULL
BEGIN
  CREATE TABLE dbo.stg_crm_sales_details(
    sls_ord_num VARCHAR(MAX),
    sls_prd_key VARCHAR(MAX),
    sls_cust_id VARCHAR(MAX),
    sls_order_dt VARCHAR(MAX),
    sls_ship_dt VARCHAR(MAX),
    sls_due_dt VARCHAR(MAX),
    sls_sales VARCHAR(MAX),
    sls_quantity VARCHAR(MAX),
    sls_price VARCHAR(MAX)
  );
  PRINT 'Created dbo.stg_crm_sales_details';
END

-- ERP: stg_erp_loc_a101
IF OBJECT_ID('dbo.stg_erp_loc_a101','U') IS NULL
BEGIN
  CREATE TABLE dbo.stg_erp_loc_a101(
    cid VARCHAR(MAX),
    cntry VARCHAR(MAX)
  );
  PRINT 'Created dbo.stg_erp_loc_a101';
END

-- ERP: stg_erp_cust_az12
IF OBJECT_ID('dbo.stg_erp_cust_az12','U') IS NULL
BEGIN
  CREATE TABLE dbo.stg_erp_cust_az12(
    cid VARCHAR(MAX),
    bdate VARCHAR(MAX),
    gen VARCHAR(MAX)
  );
  PRINT 'Created dbo.stg_erp_cust_az12';
END

-- ERP: stg_erp_px_cat_g1v2
IF OBJECT_ID('dbo.stg_erp_px_cat_g1v2','U') IS NULL
BEGIN
  CREATE TABLE dbo.stg_erp_px_cat_g1v2(
    id VARCHAR(MAX),
    cat VARCHAR(MAX),
    subcat VARCHAR(MAX),
    maintenance VARCHAR(MAX)
  );
  PRINT 'Created dbo.stg_erp_px_cat_g1v2';
END