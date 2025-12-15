/*=====================================================================
Script:		10_gold_layer_creation.sql

Purpose:
   Creates the Gold layer analytical views following a star schema design.
   This script defines the final dimensional and fact views used for reporting
   and BI consumption
     
Notes:
   - Views integrate and apply business rules to Silver layer data.
   - No data is physically stored; all logic is implemented via views

Output:
    Three tables ready to be consumed by Business Analysts
		- dim_customer
		- dim_product
		- fact_sales
=====================================================================*/


-- Create dim_customer

CREATE VIEW gold.dim_customer AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cst_create_date) AS customer_key,
	cci.cst_id AS customer_id,
	cci.cst_key AS customer_number,
	cci.cst_firstname AS first_name,
	cci.cst_lastname AS last_name,
	CASE
		WHEN cci.cst_gndr != 'Unknown' THEN cci.cst_gndr
		WHEN cci.cst_gndr = 'Unknown' AND eca.gen != 'Unknown' THEN eca.gen
		ELSE 'Unknown'
	END AS gender,
	ela.cntry AS country,
	eca.bdate AS birthdate,
	cci.cst_marital_status AS marital_status,
	cci.cst_create_date AS create_date
FROM
	silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca
	ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
	ON cci.cst_key = ela.cid


-- Create dim_product

CREATE VIEW gold.dim_product AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cpi.prd_id) AS product_key,
	cpi.prd_id AS product_id,
	cpi.prd_key AS product_number,
	cpi.cat_id AS category_id,
	cpi.prd_nm AS product_name,
	epcg.cat AS category,
	epcg.subcat AS subcategory,
	cpi.prd_line AS product_line,
	cpi.prd_cost AS cost,
	epcg.maintenance,
	cpi.prd_start_dt AS start_date
FROM
	silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epcg
	ON cpi.cat_id = epcg.id
WHERE
	cpi.prd_end_dt IS NULL       -- Filter out historical data


-- Create fact_sales

CREATE VIEW gold.fact_sales AS
SELECT
	csd.sls_ord_num AS order_number,
	dc.customer_key AS customer_key,
	dp.product_key AS product_key,
	csd.sls_cust_id AS customer_id,
	csd.sls_prd_key AS product_number,
	csd.sls_order_dt AS order_date,
	csd.sls_ship_dt AS ship_date,
	csd.sls_due_dt AS due_date,
	csd.sls_sales AS gross_sales_amount,
	csd.sls_quantity AS quantity,
	csd.sls_price AS price
FROM
	silver.crm_sales_details csd
LEFT JOIN gold.dim_customer dc
	ON csd.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_product dp
	ON csd.sls_prd_key = dp.product_number


-- Check for Foreign Key Integrity

SELECT
	*
FROM
	gold.fact_sales fs
LEFT JOIN gold.dim_customer dc
	ON fs.customer_key = dc.customer_key
WHERE
	dc.customer_key IS NULL

-- >>>> No result

SELECT
	*
FROM
	gold.fact_sales fs
LEFT JOIN gold.dim_product dp
	ON fs.product_key = dp.product_key
WHERE
	dp.product_key IS NULL

-- >>>> No result
