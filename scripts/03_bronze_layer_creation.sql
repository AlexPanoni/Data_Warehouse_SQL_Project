/*==============================================================
  SCRIPT 3: 03_bronze_layer_creation
  PROJECT: SQL Data Warehouse (Medallion Architecture)

  DESCRIPTION:
    Creates the raw ingestion tables in the 'bronze' schema.
    These tables persist source-system CSV data exactly as
    received (no parsing, no data type conversion, no validation).
    Technical lineage columns are included to support traceability
    and ETL auditing.

  CONTENTS:
    - bronze.crm_cust_info            (CRM customer raw data)
    - bronze.crm_prd_info             (CRM product raw data)
    - bronze.crm_sales_details        (CRM sales/raw transactions)
    - bronze.erp_loc_a101             (ERP location raw data)
    - bronze.erp_cust_az12            (ERP customer raw data)
    - bronze.erp_px_cat_g1v2          (ERP product/category raw data)

  NOTES / GUIDELINES:
    - Keep all business columns as text (NVARCHAR/VARCHAR) in Bronze
      to avoid load failures and preserve original values.
    - Each table includes the following lineage/metadata columns:
        dwh_load_id INT,
        dwh_file_name VARCHAR(512),
        dwh_received_at DATETIME2
    - Do not add constraints, PKs, or FKs in Bronze.
===============================================================*/


/*==============================================================
 - CRM Customer
===============================================================*/

CREATE TABLE bronze.crm_cust_info(
	cst_id NVARCHAR(50),
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(100),
	cst_marital_status NVARCHAR(10),
	cst_gndr NVARCHAR(10),
	cst_create_date NVARCHAR(50),
-- Technical columns for lineage
	dwh_load_id INT NULL,
	dwh_file_name VARCHAR(512) NULL,
	dwh_received_at DATETIME2 NULL
);

/*==============================================================
 - CRM Product
===============================================================*/

CREATE TABLE bronze.crm_prd_info(
	prd_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(250),
	prd_cost NVARCHAR(50),
	prd_line NVARCHAR(10),
	prd_start_dt NVARCHAR(25),
	prd_end_dt NVARCHAR(25),
-- Technical columns for lineage
	dwh_load_id INT NULL,
	dwh_file_name VARCHAR(512) NULL,
	dwh_received_at DATETIME2 NULL

);

/*==============================================================
 - CRM Sales
===============================================================*/

CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id NVARCHAR(50),
	sls_order_dt NVARCHAR(50),
	sls_ship_dt NVARCHAR(50),
	sls_due_dt NVARCHAR(50),
	sls_sales NVARCHAR(50),
	sls_quantity NVARCHAR(50),
	sls_price NVARCHAR(50),
-- Technical columns for lineage
	dwh_load_id INT NULL,
	dwh_file_name VARCHAR(512) NULL,
	dwh_received_at DATETIME2 NULL
);

/*==============================================================
 - ERP Location
===============================================================*/

CREATE TABLE bronze.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(100),
-- Technical columns for lineage
	dwh_load_id INT NULL,
	dwh_file_name VARCHAR(512) NULL,
	dwh_received_at DATETIME2 NULL
);

/*==============================================================
 - ERP Customer
===============================================================*/

CREATE TABLE bronze.erp_cust_az12(
	cid NVARCHAR(50),
	bdate NVARCHAR(50),
	gen NVARCHAR(10),
-- Technical columns for lineage
	dwh_load_id INT NULL,
	dwh_file_name VARCHAR(512) NULL,
	dwh_received_at DATETIME2 NULL
);

/*==============================================================
 - ERP Product
===============================================================*/

CREATE TABLE bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(100),
	maintenance NVARCHAR(10),
-- Technical columns for lineage
	dwh_load_id INT NULL,
	dwh_file_name VARCHAR(512) NULL,
	dwh_received_at DATETIME2 NULL
);