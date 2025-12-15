/*=====================================================================
Script:		09_gold_layer_checks.sql

Purpose:
   Performs data validation and analytical checks required for the Gold layer.
   This includes integrity verification, duplicate analysis, business-rule
   validation, and cross-table consistency checks to support dimensional
   modeling decisions prior to view creation.

Notes:
   - No objects are created in this script.
   - Used exclusively for analysis and validation.
=====================================================================*/

USE DataWarehouse

/*==============================================================
 - Customer
===============================================================*/

SELECT TOP(5) * FROM silver.crm_cust_info
SELECT TOP(5) * FROM silver.erp_cust_az12
SELECT TOP(5) * FROM silver.erp_loc_a101

-- Integrated Table

SELECT
	cci.cst_id,
	cci.cst_key,
	cci.cst_firstname,
	cci.cst_lastname,
	cci.cst_marital_status,
	cci.cst_gndr,
	cci.cst_create_date,
	eca.bdate,
	eca.gen,
	ela.cntry
FROM
	silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca
	ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
	ON cci.cst_key = ela.cid

-- Check for duplicates

SELECT
	cst_key,
	COUNT(*)
FROM (
SELECT
	cci.cst_id,
	cci.cst_key,
	cci.cst_firstname,
	cci.cst_lastname,
	cci.cst_marital_status,
	cci.cst_gndr,
	cci.cst_create_date,
	eca.bdate,
	eca.gen,
	ela.cntry
FROM
	silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca
	ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
	ON cci.cst_key = ela.cid
)t

GROUP BY
	cst_key
HAVING
	COUNT(*) > 1

-- >>>> No duplicates

SELECT
	cst_id,
	COUNT(*)
FROM (
SELECT
	cci.cst_id,
	cci.cst_key,
	cci.cst_firstname,
	cci.cst_lastname,
	cci.cst_marital_status,
	cci.cst_gndr,
	cci.cst_create_date,
	eca.bdate,
	eca.gen,
	ela.cntry
FROM
	silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca
	ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
	ON cci.cst_key = ela.cid
)t
GROUP BY
	cst_id
HAVING
	COUNT(*) > 1

-- >>>> No duplicates

-- Check for inconsistencies between the two gender columns from the two different sources

SELECT DISTINCT
	cci.cst_gndr,
	eca.gen
FROM
	silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca
	ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
	ON cci.cst_key = ela.cid
WHERE
	cci.cst_gndr != eca.gen OR (cci.cst_gndr = eca.gen AND cci.cst_gndr = 'Unknown')
	OR cci.cst_gndr IS NULL OR eca.gen IS NULL

-- >>>> NULL result in eca.gen and mismatches found

SELECT
	cci.cst_gndr,
	eca.gen
FROM
	silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca
	ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
	ON cci.cst_key = ela.cid
WHERE
	cci.cst_gndr != eca.gen AND cci.cst_gndr != 'Unknown' AND eca.gen != 'Unknown'	

-- >>>> 57 rows with mismatched gender

SELECT
	cci.cst_gndr,
	eca.gen
FROM
	silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca
	ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
	ON cci.cst_key = ela.cid
WHERE
	cci.cst_gndr = 'Unknown' OR eca.gen = 'Unknown'	

-- >>>> 6,030 rows with at least one "Unknown" value

SELECT
	cci.cst_gndr,
	eca.gen
FROM
	silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca
	ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
	ON cci.cst_key = ela.cid
WHERE
	cci.cst_gndr = 'Unknown' AND eca.gen = 'Unknown'	

-- >>>> 14 rows where both columns show "Unknown" value

SELECT
	cci.cst_gndr,
	eca.gen
FROM
	silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca
	ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
	ON cci.cst_key = ela.cid
WHERE
	cci.cst_gndr IS NULL OR eca.gen IS NULL

-- >>>> 1 NULL value

-- ==> crm_cust_info is the master table

SELECT DISTINCT
	cci.cst_gndr,
	eca.gen,
	CASE
		WHEN cci.cst_gndr != 'Unknown' THEN cci.cst_gndr
		WHEN cci.cst_gndr = 'Unknown' AND eca.gen != 'Unknown' THEN eca.gen
		ELSE 'Unknown'
	END AS new_gen
FROM
	silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca
	ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
	ON cci.cst_key = ela.cid


/*==============================================================
 - Product
===============================================================*/


SELECT TOP(5) * FROM silver.crm_prd_info
SELECT TOP(5) * FROM silver.erp_px_cat_g1v2

-- Integrated Table

SELECT
	cpi.prd_id,
	cpi.cat_id,
	cpi.prd_key,
	cpi.prd_nm,
	cpi.prd_cost,
	cpi.prd_line,
	cpi.prd_start_dt,
	cpi.prd_end_dt,
	epcg.cat,
	epcg.subcat,
	epcg.maintenance
FROM
	silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epcg
	ON cpi.cat_id = epcg.id

-- Check for duplicates

SELECT
	prd_id,
	COUNT(*)
FROM (
SELECT
	cpi.prd_id,
	cpi.cat_id,
	cpi.prd_key,
	cpi.prd_nm,
	cpi.prd_cost,
	cpi.prd_line,
	cpi.prd_start_dt,
	cpi.prd_end_dt,
	epcg.cat,
	epcg.subcat,
	epcg.maintenance
FROM
	silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epcg
	ON cpi.cat_id = epcg.id
)t
GROUP BY
	prd_id
HAVING
	COUNT(*) > 1

-- No duplicates

-- Check Inconsistences

SELECT DISTINCT
	cpi.prd_nm
FROM
	silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epcg
	ON cpi.cat_id = epcg.id

-- >>>> No inconsistencies

SELECT DISTINCT
	cpi.prd_line
FROM
	silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epcg
	ON cpi.cat_id = epcg.id

-- 'n/a' value found

SELECT DISTINCT
	epcg.cat
FROM
	silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epcg
	ON cpi.cat_id = epcg.id

-- NULL value found

SELECT DISTINCT
	epcg.subcat
FROM
	silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epcg
	ON cpi.cat_id = epcg.id

-- NULL value found

SELECT DISTINCT
	epcg.maintenance
FROM
	silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epcg
	ON cpi.cat_id = epcg.id

-- NULL value found

SELECT
	cpi.prd_id,
	cpi.cat_id,
	cpi.prd_key,
	cpi.prd_nm,
	cpi.prd_cost,
	cpi.prd_line,
	cpi.prd_start_dt,
	cpi.prd_end_dt,
	epcg.cat,
	epcg.subcat,
	epcg.maintenance
FROM
	silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epcg
	ON cpi.cat_id = epcg.id
WHERE
	epcg.cat IS NULL OR epcg.subcat IS NULL OR epcg.maintenance IS NULL OR cpi.prd_line = 'n/a'

-- >>>> 7 rows where product exists in crm_prd_info but not in erp_px_cat_g1v2 and 17 rows where prd_line is n/a

-- Check uniqueness of prd_key (column related to the sales table)

SELECT
	prd_key,
	COUNT(*)
FROM (
SELECT
	cpi.prd_id,
	cpi.cat_id,
	cpi.prd_key,
	cpi.prd_nm,
	cpi.prd_cost,
	cpi.prd_line,
	cpi.prd_start_dt,
	cpi.prd_end_dt,
	epcg.cat,
	epcg.subcat,
	epcg.maintenance
FROM
	silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epcg
	ON cpi.cat_id = epcg.id
)t
GROUP BY
	prd_key
HAVING
	COUNT(*) > 1


SELECT
	cpi.prd_key,
	cpi.prd_start_dt,
	cpi.prd_end_dt
FROM
	silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epcg
	ON cpi.cat_id = epcg.id

	
-- >>>> Table has historical data, so each product, when updated, will have a previous entry with an end_date
--			Decision: Use only the current version of each product in final table

SELECT
	cpi.prd_id,
	cpi.cat_id,
	cpi.prd_key,
	cpi.prd_nm,
	cpi.prd_cost,
	cpi.prd_line,
	cpi.prd_start_dt,
	cpi.prd_end_dt,
	epcg.cat,
	epcg.subcat,
	epcg.maintenance
FROM
	silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epcg
	ON cpi.cat_id = epcg.id
WHERE
	cpi.prd_end_dt IS NULL

-- Note: When only current information about the products are selected, NULL results in cat, subcat and maintenance disappear

/*==============================================================
 - Fact Sales
===============================================================*/

SELECT TOP(5) * FROM gold.dim_customer
SELECT TOP(5) * FROM gold.dim_product
SELECT TOP(5) * FROM silver.crm_sales_details


SELECT
	csd.sls_ord_num,
	csd.sls_cust_id,
	csd.sls_prd_key,
	csd.sls_order_dt,
	csd.sls_ship_dt,
	csd.sls_due_dt,
	csd.sls_sales,
	csd.sls_quantity,
	csd.sls_price,
	dc.customer_key,
	dp.product_key
FROM
	silver.crm_sales_details csd
LEFT JOIN gold.dim_customer dc
	ON csd.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_product dp
	ON csd.sls_prd_key = dp.product_number
	

