-- Creating Bronze table 
-- Do not break, do not validate, do not parse.

-- CRM Customer info
CREATE TABLE bronze.crm_cust_info(
	cst_id NVARCHAR(50),
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(100),
	cst_marital_status NVARCHAR(10),
	cst_gndr NVARCHAR(10),
	cst_create_date NVARCHAR(50)
);

-- CRM  Product info
CREATE TABLE bronze.crm_prd_info(
	prd_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(250),
	prd_cost NVARCHAR(50),
	prd_line NVARCHAR(10),
	prd_start_dt NVARCHAR(25),
	prd_end_dt NVARCHAR(25)
);

--CRM Sales info
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id NVARCHAR(50),
	sls_order_dt NVARCHAR(50),
	sls_ship_dt NVARCHAR(50),
	sls_due_dt NVARCHAR(50),
	sls_sales NVARCHAR(50),
	sls_quantity NVARCHAR(50),
	sls_price NVARCHAR(50)
);

--ERP Location info
CREATE TABLE bronze.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(100)
);

--ERP Customer info
CREATE TABLE bronze.erp_cust_az12(
	cid NVARCHAR(50),
	bdate NVARCHAR(50),
	gen NVARCHAR(10)
);

--ERP Product info
CREATE TABLE bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(100),
	maintenance NVARCHAR(10)
);
