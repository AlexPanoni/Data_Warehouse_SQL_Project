/*=====================================================================
 Script:		08_silver_layer_load.sql
 Layer:         Silver (Load)
 Purpose:       Load all cleansed datasets into the Silver layer using TRUNCATE + INSERT 
				 and create audit tables for data quality tracking. 
				Defines the silver.load_silver procedure for full Silver layer execution.

 Load Type:     Full
=====================================================================*/

CREATE OR ALTER PROCEDURE silver.load_silver 
AS
BEGIN

  DECLARE 
    @start_ts     DATETIME2,
    @end_ts       DATETIME2,
	@start_time DATETIME2,
	@end_time DATETIME2,
	@batch_start_time DATETIME2,
	@batch_end_time DATETIME2;

  PRINT '================================================';
  PRINT 'Starting silver.load_silver ...';
  PRINT '================================================';
  SET @batch_start_time = GETDATE();
  SET @start_ts = SYSUTCDATETIME();

/*==============================================================
 - CRM Customer
===============================================================*/

-- -> TRIM() to remove unwanted spaces, Case to normalize columns, Window function to remove duplicate and TRY_CONVERT to transform data into Date type

BEGIN
	PRINT '>>> Inserting data into dbo.audit_bad_dates_cust_create ...';
	SET @start_time = GETDATE();

-- 1) Audit table for bad dates 
	IF OBJECT_ID('dbo.audit_bad_dates_cust_create','U') IS NULL
	BEGIN
	  CREATE TABLE dbo.audit_bad_dates_cust_create (
		audit_id INT IDENTITY(1,1) PRIMARY KEY,
		source_cst_id NVARCHAR(200),
		raw_cst_create_date NVARCHAR(400),
		dwh_file_name VARCHAR(512),
		dwh_received_at DATETIME2,
		created_at DATETIME2 DEFAULT SYSUTCDATETIME()
	  );
	END

-- 2) Insert bad-date rows into audit (if any)

	INSERT INTO dbo.audit_bad_dates_cust_create (source_cst_id, raw_cst_create_date, dwh_file_name, dwh_received_at)
	SELECT
	  cst_id,
	  cst_create_date,
	  dwh_file_name,
	  dwh_received_at
	FROM bronze.crm_cust_info
	WHERE
	  cst_create_date IS NOT NULL
	  AND TRY_CONVERT(DATE, NULLIF(TRIM(cst_create_date),'')) IS NULL;

	SET @end_time = GETDATE();
		  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		  PRINT '-------------------------------'
END

-- 3) Main insert into silver (canonical rows only)

BEGIN
	PRINT '>>> Inserting data into silver.crm_cust_info ...';
	SET @start_time = GETDATE();

	TRUNCATE TABLE silver.crm_cust_info         -- Since it's a Full Load

	;WITH ranked AS (
	  SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY dwh_received_at DESC,
			CASE WHEN TRY_CONVERT(date, NULLIF(TRIM(cst_create_date),'')) IS NOT NULL THEN 0 ELSE 1 END,
			TRY_CONVERT(date, NULLIF(TRIM(cst_create_date),'')) DESC,
			cst_key DESC
		) AS rn
	  FROM bronze.crm_cust_info
	  WHERE cst_id IS NOT NULL
	)

	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date,
		dwh_load_id,
		dwh_file_name,
		dwh_received_at
	)
	SELECT
	  cst_id,
	  NULLIF(TRIM(cst_key),'') AS cst_key,
	  NULLIF(TRIM(cst_firstname),'') AS cst_firstname,
	  NULLIF(TRIM(cst_lastname),'')  AS cst_lastname,
	  CASE 
		WHEN UPPER(NULLIF(TRIM(cst_marital_status),'')) = 'M' THEN 'Married'
		WHEN UPPER(NULLIF(TRIM(cst_marital_status),'')) = 'S' THEN 'Single'
		ELSE 'Unknown'
	  END AS cst_marital_status,
	  CASE
		WHEN UPPER(NULLIF(TRIM(cst_gndr),'')) = 'M' THEN 'Male'
		WHEN UPPER(NULLIF(TRIM(cst_gndr),'')) = 'F' THEN 'Female'
		ELSE 'Unknown'
	  END AS cst_gndr,
	  TRY_CONVERT(DATE, NULLIF(TRIM(cst_create_date),'')) AS cst_create_date,
	  dwh_load_id,
	  dwh_file_name,
	  dwh_received_at
	FROM ranked
	WHERE rn = 1;

	SET @end_time = GETDATE();
			  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
			  PRINT '-------------------------------'
END

/*==============================================================
 - CRM Product
===============================================================*/

-- -> TRIM() to remove unwanted spaces, Case to normalize columns, REPLACE and SUBSTRING to split product key column, 
--                              Window function to produce "end date" column and TRY_CONVERT to transform data into Date type

BEGIN
	PRINT '>>> Inserting data into dbo.audit_bad_prd_rows ...';
	SET @start_time = GETDATE();

-- 1) Audit table for rows with conversion problems

	IF OBJECT_ID('dbo.audit_bad_prd_rows','U') IS NULL
	BEGIN
	  CREATE TABLE dbo.audit_bad_prd_rows (
		audit_id INT IDENTITY(1,1) PRIMARY KEY,
		prd_id       NVARCHAR(200),
		prd_key      NVARCHAR(500),
		raw_prd_cost NVARCHAR(400),
		raw_prd_start_dt NVARCHAR(400),
		raw_prd_end_dt   NVARCHAR(400),
		reason       NVARCHAR(400),
		dwh_file_name VARCHAR(512),
		dwh_received_at DATETIME2,
		created_at DATETIME2 DEFAULT SYSUTCDATETIME()
	  );
	  PRINT 'Created dbo.audit_bad_prd_rows';
	END

-- 2) Populate audit table with rows that have parsing errors
--     - prd_cost non-empty but cannot convert to DECIMAL(10,2)
--     - prd_start_dt non-empty but cannot convert to DATE

	INSERT INTO dbo.audit_bad_prd_rows (
				prd_id, prd_key, raw_prd_cost, raw_prd_start_dt, raw_prd_end_dt, reason, dwh_file_name, dwh_received_at
				)
	SELECT
	  prd_id,
	  prd_key,
	  prd_cost,
	  prd_start_dt,
	  prd_end_dt, 
	  RTRIM(
			CASE WHEN prd_cost IS NOT NULL AND TRY_CONVERT(decimal(10,2), NULLIF(TRIM(prd_cost),'')) IS NULL AND TRIM(prd_cost) <> '' 
			THEN 'bad_cost' ELSE '' END
			+ 
			CASE WHEN prd_start_dt IS NOT NULL AND TRY_CONVERT(date, NULLIF(TRIM(prd_start_dt),'')) IS NULL AND TRIM(prd_start_dt) <> ''
				 THEN IIF(LEN(RTRIM(COALESCE(prd_cost,'')))>0, ';', '') + 'bad_start_dt' ELSE '' END
			) AS reason,         -- build a short reason text
	  dwh_file_name,
	  dwh_received_at
	FROM bronze.crm_prd_info
	WHERE (
		   (prd_cost IS NOT NULL AND TRIM(prd_cost) <> '' AND TRY_CONVERT(decimal(10,2), NULLIF(TRIM(prd_cost),'')) IS NULL)
		   OR
		   (prd_start_dt IS NOT NULL AND TRIM(prd_start_dt) <> '' AND TRY_CONVERT(date, NULLIF(TRIM(prd_start_dt),'')) IS NULL)
		  );   -- only capture rows where at least one conversion fails and the raw field is non-empty

	  SET @end_time = GETDATE();
		  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		  PRINT '-------------------------------'
END

-- 3) Main insert into silver (canonical rows)

BEGIN
	PRINT '>>> Inserting data into silver.crm_prd_info ...';
	SET @start_time = GETDATE();

	TRUNCATE TABLE silver.crm_prd_info;

	;WITH pr AS (
	  SELECT
		prd_id,
		prd_key,
		prd_nm,
		TRY_CONVERT(DECIMAL(10,2), NULLIF(TRIM(prd_cost),'')) AS prd_cost,
		UPPER(NULLIF(TRIM(prd_line),'')) AS prd_line,
		TRY_CONVERT(DATE, NULLIF(TRIM(prd_start_dt), '')) AS prd_start_dt,
		TRY_CONVERT(DATE, NULLIF(TRIM(prd_end_dt),   '')) AS prd_end_dt,
		dwh_load_id,
		dwh_file_name,
		dwh_received_at
	  FROM bronze.crm_prd_info
	),
	pr_good AS (
	  SELECT p.*
	  FROM pr p
	  LEFT JOIN dbo.audit_bad_prd_rows a
		ON p.prd_id = a.prd_id
		AND p.dwh_file_name = a.dwh_file_name
		AND p.dwh_received_at = a.dwh_received_at
	  WHERE a.audit_id IS NULL
	)   -- exclude rows that were captured by the audit above

	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt,
		dwh_load_id,
		dwh_file_name,
		dwh_received_at
	)

	SELECT
		prd_id,
		REPLACE(LEFT(NULLIF(prd_key,''), 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		prd_cost,
		CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'T' THEN 'Touring'
		WHEN 'S' THEN 'Other Sales'
		ELSE 'n/a'
		END AS prd_line	,
		prd_start_dt,
		DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC)) AS prd_end_dt,
		dwh_load_id,
		dwh_file_name,
		dwh_received_at
	FROM pr_good
	ORDER BY prd_key, prd_start_dt;

	SET @end_time = GETDATE();
			  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
			  PRINT '-------------------------------'
END


/*==============================================================
 - CRM Sales
===============================================================*/

-- -> TRIM() to remove unwanted spaces, CASE to normalize columns with correct values, REPLACE and SUBSTRING to split product key column, 
--           Window function to produce "end date" column and TRY_CONVERT to transform data into Date type

BEGIN
	PRINT '>>> Inserting data into dbo.audit_bad_sales_rows ...';
	SET @start_time = GETDATE();

-- 1) Audit table for rows with conversion problems

	IF OBJECT_ID('dbo.audit_bad_sales_rows','U') IS NULL
	BEGIN
	  CREATE TABLE dbo.audit_bad_sales_rows (
		audit_id INT IDENTITY(1,1) PRIMARY KEY,
		sls_ord_num   NVARCHAR(200),
		sls_prd_key   NVARCHAR(400),
		sls_cust_id   NVARCHAR(400),
		raw_sls_order_dt NVARCHAR(200),
		raw_sls_ship_dt  NVARCHAR(200),
		raw_sls_due_dt   NVARCHAR(200),
		raw_sls_sales NVARCHAR(400),
		raw_sls_quantity NVARCHAR(200),
		raw_sls_price NVARCHAR(400),
		reason NVARCHAR(500),
		dwh_file_name VARCHAR(512),
		dwh_received_at DATETIME2,
		created_at DATETIME2 DEFAULT SYSUTCDATETIME()
	  );
	  PRINT 'Created dbo.audit_bad_sales_rows';
	END


-- 2) Populate audit table with rows that are problematic:
--     - conversion failures for date/number when raw value is non-empty
--     - business inconsistencies (zero/negative values or sales != qty * price)

	INSERT INTO dbo.audit_bad_sales_rows (
	  sls_ord_num, sls_prd_key, sls_cust_id,
	  raw_sls_order_dt, raw_sls_ship_dt, raw_sls_due_dt,
	  raw_sls_sales, raw_sls_quantity, raw_sls_price,
	  reason,
	  dwh_file_name, dwh_received_at
	)
	SELECT
	  s.sls_ord_num,
	  s.sls_prd_key,
	  s.sls_cust_id,
	  s.sls_order_dt,
	  s.sls_ship_dt,
	  s.sls_due_dt,
	  s.sls_sales,
	  s.sls_quantity,
	  s.sls_price,
	  -- reason builder: list of all problems found
	  RTRIM(
		CASE WHEN s.sls_order_dt IS NOT NULL AND TRIM(s.sls_order_dt) <> '' 
			   AND TRY_CONVERT(DATE, NULLIF(TRIM(s.sls_order_dt),''), 112) IS NULL THEN 'bad_order_dt' 
			 ELSE '' 
		END
		+ CASE WHEN s.sls_ship_dt IS NOT NULL AND TRIM(s.sls_ship_dt) <> '' 
			   AND TRY_CONVERT(DATE, NULLIF(TRIM(s.sls_ship_dt),''), 112) IS NULL THEN IIF(LEN(RTRIM(COALESCE(s.sls_order_dt,'')))>0, ';','') + 'bad_ship_dt' 
			   ELSE '' 
		END
		+ CASE WHEN s.sls_due_dt IS NOT NULL AND TRIM(s.sls_due_dt) <> '' 
			   AND TRY_CONVERT(DATE, NULLIF(TRIM(s.sls_due_dt),''), 112) IS NULL THEN IIF(LEN(RTRIM(COALESCE(s.sls_order_dt,'')))>0 OR LEN(RTRIM(COALESCE(s.sls_ship_dt,'')))>0, ';','') + 'bad_due_dt' 
			   ELSE '' 
		END
		+ CASE WHEN s.sls_sales IS NOT NULL AND TRIM(s.sls_sales) <> '' 
			   AND TRY_CONVERT(DECIMAL(18,2), NULLIF(TRIM(s.sls_sales),'')) IS NULL THEN IIF(LEN(RTRIM(COALESCE(s.sls_order_dt,'')))>0 OR LEN(RTRIM(COALESCE(s.sls_price,'')))>0, ';','') + 'bad_sales' 
			   ELSE '' 
		END
		+ CASE WHEN s.sls_quantity IS NOT NULL AND TRIM(s.sls_quantity) <> '' 
			   AND TRY_CONVERT(INT, NULLIF(TRIM(s.sls_quantity),'')) IS NULL THEN IIF(LEN(RTRIM(COALESCE(s.sls_sales,'')))>0, ';','') + 'bad_qty' 
			   ELSE '' 
		END
		+ CASE WHEN s.sls_price IS NOT NULL AND TRIM(s.sls_price) <> '' 
			   AND TRY_CONVERT(DECIMAL(10,2), NULLIF(TRIM(s.sls_price),'')) IS NULL THEN IIF(LEN(RTRIM(COALESCE(s.sls_sales,'')))>0 OR LEN(RTRIM(COALESCE(s.sls_quantity,'')))>0, ';','') + 'bad_price' 
			   ELSE '' 
		END
		+ CASE 
			WHEN ( (TRY_CONVERT(DECIMAL(18,2), NULLIF(TRIM(s.sls_sales),'')) IS NOT NULL AND TRY_CONVERT(DECIMAL(18,2), NULLIF(TRIM(s.sls_sales),'')) <= 0)
				 OR (TRY_CONVERT(INT, NULLIF(TRIM(s.sls_quantity),'')) IS NOT NULL AND TRY_CONVERT(INT, NULLIF(TRIM(s.sls_quantity),'')) <= 0)
				 OR (TRY_CONVERT(DECIMAL(18,2), NULLIF(TRIM(s.sls_price),'')) IS NOT NULL AND TRY_CONVERT(DECIMAL(18,2), NULLIF(TRIM(s.sls_price),'')) <= 0)
				 ) THEN IIF(LEN(RTRIM(COALESCE(s.sls_sales,'')))>0 OR LEN(RTRIM(COALESCE(s.sls_quantity,'')))>0 OR LEN(RTRIM(COALESCE(s.sls_price,'')))>0, ';','') + 'non_positive_value' 
			ELSE '' 
		END        -- business inconsistencies: negative or zero where it shouldn't be or mismatch
		+ CASE 
			WHEN (TRY_CONVERT(DECIMAL(18,4), NULLIF(TRIM(s.sls_sales),'')) IS NOT NULL
				AND TRY_CONVERT(INT, NULLIF(TRIM(s.sls_quantity),'')) IS NOT NULL
				AND TRY_CONVERT(DECIMAL(10,4), NULLIF(TRIM(s.sls_price),'')) IS NOT NULL
				AND ABS( TRY_CONVERT(DECIMAL(18,4), NULLIF(TRIM(s.sls_sales),'') ) - ( TRY_CONVERT(DECIMAL(10,4), NULLIF(TRIM(s.sls_price),'') ) 
																	   * TRY_CONVERT(DECIMAL(18,4), NULLIF(TRIM(s.sls_quantity),'') ) ) ) > 0.01) 
			THEN ';inconsistent_sales' 
			ELSE '' 
		END       -- mismatch: all three convertible AND sales differs from qty*price (allow small rounding tolerance)
	  ) AS reason,
	  s.dwh_file_name,
	  s.dwh_received_at
	FROM bronze.crm_sales_details s
	WHERE (        -- include row if any of the checked problems is true
			(s.sls_order_dt IS NOT NULL AND TRIM(s.sls_order_dt) <> '' AND TRY_CONVERT(DATE, NULLIF(TRIM(s.sls_order_dt),''), 112) IS NULL)
			OR (s.sls_ship_dt IS NOT NULL AND TRIM(s.sls_ship_dt) <> '' AND TRY_CONVERT(DATE, NULLIF(TRIM(s.sls_ship_dt),''), 112) IS NULL)
			OR (s.sls_due_dt IS NOT NULL AND TRIM(s.sls_due_dt) <> '' AND TRY_CONVERT(DATE, NULLIF(TRIM(s.sls_due_dt),''), 112) IS NULL)
			OR (s.sls_sales IS NOT NULL AND TRIM(s.sls_sales) <> '' AND TRY_CONVERT(DECIMAL(18,2), NULLIF(TRIM(s.sls_sales),'')) IS NULL)
			OR (s.sls_quantity IS NOT NULL AND TRIM(s.sls_quantity) <> '' AND TRY_CONVERT(INT, NULLIF(TRIM(s.sls_quantity),'')) IS NULL)
			OR (s.sls_price IS NOT NULL AND TRIM(s.sls_price) <> '' AND TRY_CONVERT(DECIMAL(10,2), NULLIF(TRIM(s.sls_price),'')) IS NULL)
			OR 
			   (TRY_CONVERT(DECIMAL(18,4), NULLIF(TRIM(s.sls_sales),'')) IS NOT NULL
				AND TRY_CONVERT(INT, NULLIF(TRIM(s.sls_quantity),'')) IS NOT NULL
				AND TRY_CONVERT(DECIMAL(10,4), NULLIF(TRIM(s.sls_price),'')) IS NOT NULL
				AND ABS( TRY_CONVERT(DECIMAL(18,4), NULLIF(TRIM(s.sls_sales),'')) 
						 - ( TRY_CONVERT(DECIMAL(10,4), NULLIF(TRIM(s.sls_price),'')) 
							 * TRY_CONVERT(DECIMAL(18,4), NULLIF(TRIM(s.sls_quantity),'')) ) ) > 0.01
			  )
			OR
			  (
				(TRY_CONVERT(DECIMAL(18,2), NULLIF(TRIM(s.sls_sales),'')) IS NOT NULL AND TRY_CONVERT(DECIMAL(18,2), NULLIF(TRIM(s.sls_sales),'')) <= 0)
				OR (TRY_CONVERT(INT, NULLIF(TRIM(s.sls_quantity),'')) IS NOT NULL AND TRY_CONVERT(INT, NULLIF(TRIM(s.sls_quantity),'')) <= 0)
				OR (TRY_CONVERT(DECIMAL(10,2), NULLIF(TRIM(s.sls_price),'')) IS NOT NULL AND TRY_CONVERT(DECIMAL(10,2), NULLIF(TRIM(s.sls_price),'')) <= 0)
			  )
			);

	SET @end_time = GETDATE();
			  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
			  PRINT '-------------------------------'
END

-- 3) Main insert into silver (all rows)

BEGIN
	PRINT '>>> Inserting data into silver.crm_sales_details ...';
	SET @start_time = GETDATE();

	TRUNCATE TABLE silver.crm_sales_details;

	;WITH sls_converted AS (
	  SELECT
		s.sls_ord_num,
		s.sls_prd_key,
		s.sls_cust_id,
		-- date stored as YYYYMMDD in bronze: convert if length=8, else NULL
		CASE WHEN s.sls_order_dt IS NULL OR TRIM(s.sls_order_dt) = '' OR TRIM(s.sls_order_dt) = '0' THEN NULL
			 ELSE TRY_CONVERT(DATE, NULLIF(TRIM(s.sls_order_dt),''), 112) 
		END AS sls_order_dt,
		CASE WHEN s.sls_ship_dt IS NULL OR TRIM(s.sls_ship_dt) = '' OR TRIM(s.sls_ship_dt) = '0' THEN NULL
			 ELSE TRY_CONVERT(DATE, NULLIF(TRIM(s.sls_ship_dt),''), 112) 
		END AS sls_ship_dt,
		CASE WHEN s.sls_due_dt IS NULL OR TRIM(s.sls_due_dt) = '' OR TRIM(s.sls_due_dt) = '0' THEN NULL
			 ELSE TRY_CONVERT(DATE, NULLIF(TRIM(s.sls_due_dt),''), 112) 
		END AS sls_due_dt,
		TRY_CONVERT(DECIMAL(18,2), NULLIF(TRIM(s.sls_sales),'')) AS sls_sales,
		TRY_CONVERT(INT, NULLIF(TRIM(s.sls_quantity),'')) AS sls_quantity,
		TRY_CONVERT(DECIMAL(10,2), NULLIF(TRIM(s.sls_price),'')) AS sls_price,
		s.dwh_load_id,
		s.dwh_file_name,
		s.dwh_received_at
	  FROM bronze.crm_sales_details s
	)

	--  4) Insert the cleaned / enriched rows into silver
	--    - apply business fixes: make negatives positive, recalc sales when inconsistent

	INSERT INTO silver.crm_sales_details(
	  sls_ord_num,
	  sls_prd_key,
	  sls_cust_id,
	  sls_order_dt,
	  sls_ship_dt,
	  sls_due_dt,
	  sls_sales,
	  sls_quantity,
	  sls_price,
	  dwh_load_id,
	  dwh_file_name,
	  dwh_received_at
	)

	SELECT
	  sls_ord_num,
	  sls_prd_key,
	  sls_cust_id,
	  sls_order_dt,
	  sls_ship_dt,
	  sls_due_dt,

	  -- sales: if sales is NULL or <=0 or inconsistent -> recompute from qty*price
	  CASE
		WHEN sls_sales IS NULL
		  OR sls_sales <= 0
		  OR (sls_quantity IS NOT NULL AND sls_price IS NOT NULL
			  AND ABS(sls_sales - (sls_quantity * sls_price)) > 0.01)
		THEN
		  -- compute from ABS values (fallback); if division by zero would happen it results in NULL
		  CASE WHEN sls_quantity IS NOT NULL AND sls_price IS NOT NULL THEN ABS(sls_quantity) * ABS(sls_price) ELSE sls_sales END
		ELSE sls_sales
	  END AS sls_sales,

	  -- quantity: if missing or <=0 try to infer from sales/price, else make positive
	  CASE
		WHEN sls_quantity IS NULL OR sls_quantity <= 0 THEN
		  CASE WHEN sls_price IS NOT NULL AND sls_price <> 0 THEN CAST(ROUND(ABS(COALESCE(sls_sales,0)) / NULLIF(ABS(sls_price),0),0) AS INT) ELSE NULL END
		ELSE ABS(sls_quantity)
	  END AS sls_quantity,

	  -- price: if missing or <=0 try to infer from sales/quantity, else make positive
	  CASE
		WHEN sls_price IS NULL OR sls_price <= 0 THEN
		  CASE WHEN sls_quantity IS NOT NULL AND sls_quantity <> 0 THEN TRY_CONVERT(DECIMAL(10,2), ROUND(ABS(COALESCE(sls_sales,0)) / NULLIF(ABS(CAST(sls_quantity AS DECIMAL(18,6))),0),2) ) ELSE NULL END
		ELSE TRY_CONVERT(DECIMAL(10,2), ABS(sls_price))
	  END AS sls_price,
	  dwh_load_id,
	  dwh_file_name,
	  dwh_received_at
	FROM sls_converted
	/*  WHERE
			sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
			OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0                         -- this block used for check
			OR sls_sales != sls_quantity * sls_price
			OR sls_order_dt IS NULL
	*/
	SET @end_time = GETDATE();
				  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
				  PRINT '-------------------------------'
END


/*==============================================================
 - ERP Customer
===============================================================*/

--       'NAS' removed from cid, standardized gender, wrong dates removed

BEGIN
	PRINT '>>> Inserting data into dbo.audit_erp_cust_az12 ...';
	SET @start_time = GETDATE();

-- 1) Audit table for rows with conversion problems

	IF OBJECT_ID('dbo.audit_erp_cust_az12','U') IS NULL
	BEGIN
	  CREATE TABLE dbo.audit_erp_cust_az12 (
		audit_id INT IDENTITY(1,1) PRIMARY KEY,
		raw_cid         NVARCHAR(400),
		raw_bdate       NVARCHAR(200),
		raw_gen         NVARCHAR(100),
		reason          NVARCHAR(500),
		dwh_file_name   VARCHAR(512),
		dwh_received_at DATETIME2,
		created_at      DATETIME2 DEFAULT SYSUTCDATETIME()
	  );
	  PRINT 'Created dbo.audit_erp_cust_az12';
	END


-- 2) Populate audit table with problematic rows

	INSERT INTO dbo.audit_erp_cust_az12 (
	  raw_cid, raw_bdate, raw_gen, reason, dwh_file_name, dwh_received_at
	)
	SELECT
	  cid,
	  bdate,
	  gen,
	  RTRIM(CASE 
				WHEN (gen IS NULL OR NULLIF(TRIM(gen),'') IS NULL) THEN IIF(LEN(TRIM(COALESCE(cid,'')))>0,';','') + ';gen_missing' 
				WHEN UPPER(NULLIF(TRIM(gen),'')) NOT IN ('M','F','MALE','FEMALE') THEN ';gen_unexpected' 
				ELSE '' 
			END
		+
			CASE 
			  WHEN NULLIF(TRIM(bdate),'') IS NULL THEN ''
			  WHEN (TRY_CONVERT(DATE, NULLIF(TRIM(bdate),''), 112) IS NULL AND TRY_CONVERT(DATE, NULLIF(TRIM(bdate),'')) IS NULL )
				   THEN ';bdate_unparseable'
			  WHEN COALESCE(TRY_CONVERT(DATE, NULLIF(TRIM(bdate),''), 112),
							 TRY_CONVERT(DATE, NULLIF(TRIM(bdate),'')) ) > CAST(GETDATE() AS DATE)
				   THEN ';bdate_in_future'
			  ELSE '' 
			END
		   ) AS reason,
	  dwh_file_name,
	  dwh_received_at
	FROM bronze.erp_cust_az12
	WHERE(
		  (gen IS NULL OR NULLIF(TRIM(gen),'') IS NULL)
		  OR
		  (UPPER(NULLIF(TRIM(gen),'')) NOT IN ('M','F','MALE','FEMALE'))
		  OR
		  (
		   NULLIF(TRIM(bdate),'') IS NOT NULL
		   AND TRY_CONVERT(date, NULLIF(TRIM(bdate),''), 112) IS NULL
		   AND TRY_CONVERT(date, NULLIF(TRIM(bdate),'')) IS NULL
		   )
		  OR
		  (
		   NULLIF(TRIM(bdate),'') IS NOT NULL
		   AND COALESCE( TRY_CONVERT(date, NULLIF(TRIM(bdate),''), 112),
						TRY_CONVERT(date, NULLIF(TRIM(bdate),'')) ) > CAST(GETDATE() AS date)
		   )
		 );

		 SET @end_time = GETDATE();
				  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
				  PRINT '-------------------------------'
END

-- 3) Main insert into silver

BEGIN
	PRINT '>>> Inserting data into silver.erp_cust_az12 ...';
	SET @start_time = GETDATE();

	TRUNCATE TABLE silver.erp_cust_az12;

	INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen,
		dwh_load_id,
		dwh_file_name,
		dwh_received_at
	)

	SELECT
		CASE
			WHEN LEFT(TRIM(cid),3) = 'NAS' THEN SUBSTRING(TRIM(cid), 4, LEN(TRIM(cid)) - 3)
			ELSE TRIM(cid)
		END AS cid,
		CASE
			WHEN TRY_CONVERT(DATE, NULLIF(TRIM(bdate),'')) >  GETDATE() THEN NULL
			ELSE TRY_CONVERT(DATE, NULLIF(TRIM(bdate),''))
		END AS bdate,
		CASE
			WHEN UPPER(NULLIF(TRIM(gen),'')) IN ('M','MALE') THEN 'Male'
			WHEN UPPER(NULLIF(TRIM(gen),'')) IN ('F','FEMALE') THEN 'Female'
			ELSE 'Unknown'
		END AS gen,
		dwh_load_id,
		dwh_file_name,
		dwh_received_at
	FROM
		bronze.erp_cust_az12

	SET @end_time = GETDATE();
				  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
				  PRINT '-------------------------------'
END

/*==============================================================
 - ERP Location
===============================================================*/

--       '-' removed from cid, standardized countries

BEGIN
	PRINT '>>> Inserting data into dbo.audit_erp_loc_a101 ...';
	SET @start_time = GETDATE();

-- 1) Audit table for rows with conversion problems

	IF OBJECT_ID('dbo.audit_erp_loc_a101','U') IS NULL
	BEGIN
	  CREATE TABLE dbo.audit_erp_loc_a101 (
		audit_id INT IDENTITY(1,1) PRIMARY KEY,
		raw_cid     NVARCHAR(400),
		norm_cid    NVARCHAR(400),
		raw_cntry   NVARCHAR(400),
		norm_cntry  NVARCHAR(200),
		reason      NVARCHAR(400),
		dwh_file_name VARCHAR(512),
		dwh_received_at DATETIME2,
		created_at  DATETIME2 DEFAULT SYSUTCDATETIME()
	  );
	  PRINT 'Created dbo.audit_erp_loc_a101';
	END



-- 2) Populate audit table with problematic rows

	INSERT INTO dbo.audit_erp_loc_a101 (
	  raw_cid, norm_cid, raw_cntry, norm_cntry, reason, dwh_file_name, dwh_received_at
	)
	SELECT
	  br.cid AS raw_cid,
	  -- normalize cid for comparison: trim + remove hyphens
	  REPLACE(NULLIF(TRIM(br.cid),''), '-', '') AS norm_cid,
	  br.cntry AS raw_cntry,
	  UPPER(NULLIF(TRIM(br.cntry),'')) AS norm_cntry,
	  RTRIM(
		CASE WHEN NULLIF(TRIM(br.cid),'') IS NULL THEN 'missing_cid' ELSE '' END
		+ CASE WHEN NULLIF(TRIM(br.cntry),'') IS NULL THEN IIF(LEN(TRIM(COALESCE(br.cid,'')))>0,';','') + 'missing_cntry' ELSE '' END
		+ CASE WHEN NOT EXISTS (
					SELECT 1
					FROM silver.crm_cust_info c
					WHERE REPLACE(NULLIF(TRIM(c.cst_key),''),'-','') = REPLACE(NULLIF(TRIM(br.cid),''),'-','')
				) 
				AND NULLIF(TRIM(br.cid),'') IS NOT NULL THEN 
			IIF(LEN(RTRIM(COALESCE(br.cid,'')))>0,';','') + 'cid_not_in_customers' ELSE '' END
	  ) AS reason,
	  br.dwh_file_name,
	  br.dwh_received_at
	FROM bronze.erp_loc_a101 br
	WHERE
		  (
			NULLIF(TRIM(br.cid),'') IS NULL
			OR NULLIF(TRIM(br.cntry),'') IS NULL
			OR NOT EXISTS (
			   SELECT 1
			   FROM silver.crm_cust_info c
			   WHERE REPLACE(NULLIF(TRIM(c.cst_key),''),'-','') = REPLACE(NULLIF(TRIM(br.cid),''),'-','')
			)
		  );

	SET @end_time = GETDATE();
					  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
					  PRINT '-------------------------------'
END


-- 3) Main insert into silver

BEGIN
	PRINT '>>> Inserting data into silver.erp_loc_a101 ...';
	SET @start_time = GETDATE();

	TRUNCATE TABLE silver.erp_loc_a101;

	INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry,
		dwh_load_id,
		dwh_file_name,
		dwh_received_at
	)

	SELECT
		NULLIF(REPLACE(TRIM(cid), '-', ''), '') AS cid,
		CASE
			WHEN cntry IS NULL THEN NULL
			WHEN UPPER(TRIM(cntry)) IN ('US','USA','UNITED STATES','UNITED STATES OF AMERICA') THEN 'United States'
			WHEN UPPER(TRIM(cntry)) IN ('FR','FRA','FRANCE') THEN 'France'
			WHEN UPPER(TRIM(cntry)) IN ('DE','DEU','GERMANY') THEN 'Germany'
			WHEN UPPER(TRIM(cntry)) IN ('UK','GB','GBR','UNITED KINGDOM') THEN 'United Kingdom'
			WHEN UPPER(TRIM(cntry)) IN ('CA','CAN','CANADA') THEN 'Canada'
			WHEN UPPER(TRIM(cntry)) IN ('AU','AUS','Australia') THEN 'Australia'
			ELSE TRIM(cntry)
		END AS cntry,
		dwh_load_id,
		dwh_file_name,
		dwh_received_at
	FROM
		bronze.erp_loc_a101

	SET @end_time = GETDATE();
					  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
					  PRINT '-------------------------------'
END

/*==============================================================
 - ERP Product
===============================================================*/

--        Since no inconsistency was found, just keep regular general procedures

BEGIN
	PRINT '>>> Inserting data into dbo.audit_px_cat_g1v2 ...';
	SET @start_time = GETDATE();

-- 1) Create audit table
	IF OBJECT_ID('dbo.audit_px_cat_g1v2','U') IS NULL
	BEGIN
	  CREATE TABLE dbo.audit_px_cat_g1v2 (
		audit_id INT IDENTITY(1,1) PRIMARY KEY,
		raw_id       NVARCHAR(400),
		raw_cat      NVARCHAR(400),
		raw_subcat   NVARCHAR(400),
		raw_maintenance NVARCHAR(200),
		norm_id      NVARCHAR(400),
		norm_cat     NVARCHAR(400),
		norm_subcat  NVARCHAR(400),
		norm_maintenance NVARCHAR(50),
		reason       NVARCHAR(500),
		dwh_file_name VARCHAR(512),
		dwh_received_at DATETIME2,
		created_at   DATETIME2 DEFAULT SYSUTCDATETIME()
	  );
	  PRINT 'Created dbo.audit_px_cat_g1v2';
	END


-- 2) Populate audit table for rows that fail expected patterns
INSERT INTO dbo.audit_px_cat_g1v2 (
  raw_id, raw_cat, raw_subcat, raw_maintenance,
  norm_id, norm_cat, norm_subcat, norm_maintenance,
  reason, dwh_file_name, dwh_received_at
)
SELECT
  br.id,
  br.cat,
  br.subcat,
  br.Maintenance,
  NULLIF(TRIM(br.id),'') AS norm_id,
  NULLIF(TRIM(br.cat),'') AS norm_cat,
  NULLIF(TRIM(br.subcat),'') AS norm_subcat,
  CASE 
    WHEN UPPER(NULLIF(TRIM(br.Maintenance),'')) IN ('Y','YES') THEN 'Yes'
    WHEN UPPER(NULLIF(TRIM(br.Maintenance),'')) IN ('N','NO') THEN 'No'
    WHEN NULLIF(TRIM(br.Maintenance),'') IS NULL THEN NULL
    ELSE TRIM(br.Maintenance)
  END AS norm_maintenance,
  RTRIM(
    CASE 
		WHEN NULLIF(TRIM(br.id),'') IS NULL THEN 'missing_id' 
		ELSE '' 
	END
    + CASE 
		WHEN NULLIF(TRIM(br.cat),'') IS NULL THEN ';missing_cat' 
		ELSE '' 
	END
    + CASE 
		WHEN NULLIF(TRIM(br.subcat),'') IS NULL THEN ';missing_subcat' 
		ELSE '' 
	END
    + CASE 
		WHEN NULLIF(TRIM(br.Maintenance),'') IS NOT NULL AND UPPER(NULLIF(TRIM(br.Maintenance),'')) NOT IN ('Y','YES','N','NO')
           THEN ';unexpected_maintenance' 
		ELSE '' 
	END
  ) AS reason,
  br.dwh_file_name,
  br.dwh_received_at
FROM bronze.erp_px_cat_g1v2 br
WHERE
  (
    NULLIF(TRIM(br.id),'') IS NULL
    OR NULLIF(TRIM(br.cat),'') IS NULL
    OR NULLIF(TRIM(br.subcat),'') IS NULL
    OR ( NULLIF(TRIM(br.Maintenance),'') IS NOT NULL
         AND UPPER(NULLIF(TRIM(br.Maintenance),'')) NOT IN ('Y','YES','N','NO') )
  );

	SET @end_time = GETDATE();
					  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
					  PRINT '-------------------------------'
END


-- 3) Main insert into silver

BEGIN
	PRINT '>>> Inserting data into silver.erp_px_cat_g1v2 ...';
	SET @start_time = GETDATE();

	TRUNCATE TABLE silver.erp_px_cat_g1v2;


	INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance,
		dwh_load_id,
		dwh_file_name,
		dwh_received_at
	)

	SELECT
		id,
		NULLIF(TRIM(cat), '') AS cat,
		NULLIF(TRIM(subcat), '') AS subcat,
		CASE
			WHEN UPPER(TRIM(maintenance)) IN ('Y', 'YES') THEN 'Yes'
			WHEN UPPER(TRIM(maintenance)) IN ('N', 'NO') THEN 'No'
			ELSE NULL
		END AS maintenance,
		dwh_load_id,
		dwh_file_name,
		dwh_received_at
	FROM
		bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE();
					  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
					  PRINT '-------------------------------'
END

  -- Final summary
  SET @batch_end_time = GETDATE();
      PRINT '================================================';
	  PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds.';
  SET @end_ts = SYSUTCDATETIME();
  PRINT 'silver.load_silver completed at ' + CONVERT(VARCHAR(30), @end_ts, 121);
  PRINT '================================================';
END;

