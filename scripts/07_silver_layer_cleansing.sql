/*=====================================================================
 Script:	    07_silver_layer_cleansing.sql
 Layer:         Silver (Cleansing & Diagnostics)

 Purpose:       Perform all Silver Layer data quality checks, including null analysis,
                duplicate detection, referential integrity validation, domain checks,
                and identification of anomalous records to support subsequent cleansing
                and standardization steps.

=====================================================================*/

/*==============================================================
 - CRM Customer
===============================================================*/

SELECT TOP(10) * FROM bronze.crm_cust_info

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No result

SELECT 
	cst_id,
	COUNT(*)
FROM 
	bronze.crm_cust_info
GROUP BY
	cst_id
HAVING
	COUNT(*) > 1 OR cst_id IS NULL

-- >>>> Result: 6 values

-- Check for unwanted spaces

SELECT
	cst_firstname,
	cst_lastname
FROM
	bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) OR cst_lastname != TRIM(cst_lastname)

-- >> Result: 26 values

SELECT
	cst_marital_status,
	cst_gndr
FROM
	bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status) OR cst_gndr != TRIM(cst_gndr)

-- >> Result: No Values

-- Check for Standardization and Consistency

SELECT DISTINCT
	cst_marital_status
FROM
	bronze.crm_cust_info

SELECT DISTINCT
	cst_gndr
FROM
	bronze.crm_cust_info

/*==============================================================
 - CRM Product
===============================================================*/

SELECT TOP(10) * FROM bronze.crm_prd_info

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No result

SELECT 
	prd_id,
	COUNT(*)
FROM 
	bronze.crm_prd_info
GROUP BY
	prd_id
HAVING
	COUNT(*) > 1 OR prd_id IS NULL

-- >>>> Result: 0 values

SELECT TOP(10)
	prd_key
FROM
	bronze.crm_prd_info

-- >>>> First 5 digits represent Product Category, the rest is the Product Key. This column will be split when inserted into Silver layer

-- Check for unwanted spaces

SELECT
	prd_nm
FROM
	bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- >>>>> Result: 0 values

-- Check for NULL or negative values

SELECT
	prd_cost
FROM
	bronze.crm_prd_info
WHERE
	prd_cost < 0 OR prd_cost IS NULL

-- >>>> Result: 2 NULL values

SELECT DISTINCT
	prd_line
FROM
	bronze.crm_prd_info

-- >>>> Column will be normalized with full name of categories

-- Check for invalid date orders

SELECT
	*
FROM
	bronze.crm_prd_info
WHERE
	prd_end_dt < prd_start_dt

-- >>>> End date column has dates earlier than start date. Final end date column shall be derived from the start_date column

/*==============================================================
 - CRM Sales
===============================================================*/

SELECT TOP(10) * FROM bronze.crm_sales_details

-- Check for Nulls in Order Number and Primary Key

SELECT 
	sls_ord_num,
	COUNT(*)
FROM 
	bronze.crm_sales_details
GROUP BY
	sls_ord_num
HAVING
	 sls_ord_num IS NULL

SELECT 
	sls_prd_key,
	COUNT(*)
FROM 
	bronze.crm_sales_details
GROUP BY
	sls_prd_key
HAVING
	sls_prd_key IS NULL

-- >>>> No result

-- Check if Product key and Customer ID matches the other tables

SELECT
	*
FROM
	bronze.crm_sales_details
WHERE
	sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

SELECT
	*
FROM
	bronze.crm_sales_details
WHERE
	sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

-- >>>> No mismatch

-- Check dates

SELECT
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details
WHERE
	sls_order_dt <= 0 OR sls_ship_dt <= 0 OR sls_due_dt <= 0 

SELECT
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details
WHERE
	LEN(sls_order_dt) != 8 OR LEN(sls_ship_dt) != 8 OR LEN(sls_due_dt) != 8 

-- >>>> 17 "0"s and 2 wrong dates in sls_order_dt

SELECT
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details
WHERE
	sls_order_dt NOT BETWEEN 20000101 AND 20250101
    OR sls_ship_dt NOT BETWEEN 20000101 AND 20250101
    OR sls_due_dt NOT BETWEEN 20000101 AND 20250101

-- >>>> No dates out of boundaries

-- Check for Invalid Date Orders

SELECT
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details
WHERE
	sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt OR sls_ship_dt > sls_due_dt

-- >>>> No dates out of order

-- Check Consistency in Sales

WITH casted_data AS (
    SELECT
        CAST(sls_sales AS INT) AS sls_sales,
        CAST(sls_quantity AS INT) AS sls_quantity,
        CAST(sls_price AS INT) AS sls_price
    FROM bronze.crm_sales_details
)
SELECT
    sls_sales,
    sls_quantity,
    sls_price
FROM casted_data
WHERE
    sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
    OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
    OR sls_sales != sls_quantity * sls_price

-- >>>> Result: NULLs, "0"s, negative numbers and wrong calculation found (35 results overall)
--   --->> Negative values can be converted to positive, NULL values can be calculated from the other 2 columns

/*==============================================================
 - ERP Customer
===============================================================*/

SELECT TOP(10) * FROM bronze.erp_cust_az12

-- Check for NULLs

SELECT
	*
FROM
	bronze.erp_cust_az12
WHERE
	cid IS NULL OR bdate IS NULL OR gen IS NULL

-- >>>> Result: 1471 rows where gen is NULL

-- Check for Duplicates

SELECT
	cid
FROM
	bronze.erp_cust_az12
GROUP BY
	cid
HAVING
	COUNT(*) > 1

-- >>>> No result

-- Check if cid matches its correspond column in crm_cust_info

SELECT TOP(10)
	cid
FROM
	bronze.erp_cust_az12

SELECT TOP(10)
	cst_key
FROM
	bronze.crm_cust_info

-- >>>> cid has 3 extra characters (NAS) at the beginning

-- Check gen column

SELECT DISTINCT
	gen
FROM
	bronze.erp_cust_az12

-- >>>> 13 rows with 'F', 'M' or empty value

-- Check for out of boundaries date

SELECT
	bdate
FROM
	bronze.erp_cust_az12
WHERE
	bdate NOT BETWEEN '1900-01-01' AND '2025-01-01'

-- >>>> 16 results with people born in the future (time travelers?)

/*==============================================================
 - ERP Location
===============================================================*/

SELECT TOP(10) * FROM bronze.erp_loc_a101

-- Check for NULL

SELECT 
	*
FROM
	bronze.erp_loc_a101
WHERE
	cid IS NULL OR cntry IS NULL

-- >>>> 332 NULLs in the country colunm

-- Check for Duplicates

SELECT
	cid
FROM
	bronze.erp_loc_a101
GROUP BY
	cid
HAVING
	COUNT(*) > 1

-- >>>> No duplicates

-- Check if cid matches its correspond column in crm_cust_info

SELECT TOP(10)
	cid
FROM
	bronze.erp_loc_a101

SELECT TOP(10)
	cst_key
FROM
	bronze.crm_cust_info

-- >>>> cid has a "-" after second character

-- Check for Standatization and Consistency

SELECT DISTINCT
	cntry
FROM
	bronze.erp_loc_a101

-- >>>> DE and Germany used; US, USA and United States; NULL and empty values

/*==============================================================
 - ERP Product
===============================================================*/

SELECT TOP(10) * FROM bronze.erp_px_cat_g1v2

-- Check for NULLs

SELECT
	*
FROM
	bronze.erp_px_cat_g1v2
WHERE
	id IS NULL OR cat IS NULL OR subcat IS NULL or maintenance IS NULL

-- >>>> No NULL values

-- Check for id Duplicates

SELECT
	id
FROM
	bronze.erp_px_cat_g1v2
GROUP BY
	id
HAVING
	COUNT(*) > 1

-- >>>> No duplicates

-- Match id with correspondent column in crm_prd_info

SELECT TOP(10) id FROM bronze.erp_px_cat_g1v2
SELECT TOP(10) cat_id FROM silver.crm_prd_info    -- column created during silver insert by spliting original prd_key column

-- >>>> All matchs

-- Check inconsistency

SELECT DISTINCT TRIM(cat) FROM bronze.erp_px_cat_g1v2 ORDER BY 1;
SELECT DISTINCT TRIM(subcat) FROM bronze.erp_px_cat_g1v2 ORDER BY 1;
SELECT DISTINCT TRIM(Maintenance) FROM bronze.erp_px_cat_g1v2 ORDER BY 1;

-- >>> No inconsistency found