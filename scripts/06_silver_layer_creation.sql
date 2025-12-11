/*=====================================================================
Script:		06_silver_layer_creation.sql

Purpose:	Creates the Silver Layer tables used for cleaned and typed data.
			Each table mirrors its Bronze counterpart but applies proper
			 datatypes (DATE, DATETIME2, INT, DECIMAL, etc.) and prepares the
			 data for standardized transformations in the Silver processing step.

Prerequisites:
    - Bronze Layer tables created and loaded.
    - Schemas "bronze" and "silver" available.

Output:
    - Six Silver Layer tables ready for transformation.
=====================================================================*/

/*==============================================================
 - CRM Customer
===============================================================*/

CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(100),
	cst_lastname NVARCHAR(100),
	cst_marital_status NVARCHAR(10),
	cst_gndr NVARCHAR(10),
	cst_create_date DATE,
-- Technical columns for lineage
	dwh_load_id INT NULL,
	dwh_file_name VARCHAR(512) NULL,
	dwh_received_at DATETIME2 NULL
);

/*==============================================================
 - CRM Product
===============================================================*/

CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(250),
	prd_cost DECIMAL(10, 2),
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
-- Technical columns for lineage
	dwh_load_id INT NULL,
	dwh_file_name VARCHAR(512) NULL,
	dwh_received_at DATETIME2 NULL

);

/*==============================================================
 - CRM Sales
===============================================================*/

CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales DECIMAL(18, 2),
	sls_quantity INT,
	sls_price DECIMAL(10, 2),
-- Technical columns for lineage
	dwh_load_id INT NULL,
	dwh_file_name VARCHAR(512) NULL,
	dwh_received_at DATETIME2 NULL
);

/*==============================================================
 - ERP Location
===============================================================*/

CREATE TABLE silver.erp_loc_a101(
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

CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(10),
-- Technical columns for lineage
	dwh_load_id INT NULL,
	dwh_file_name VARCHAR(512) NULL,
	dwh_received_at DATETIME2 NULL
);

/*==============================================================
 - ERP Product
===============================================================*/

CREATE TABLE silver.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(100),
	maintenance NVARCHAR(10),
-- Technical columns for lineage
	dwh_load_id INT NULL,
	dwh_file_name VARCHAR(512) NULL,
	dwh_received_at DATETIME2 NULL
);