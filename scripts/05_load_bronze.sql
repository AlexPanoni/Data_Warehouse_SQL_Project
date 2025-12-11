/*==============================================================
Script:        05_load_bronze.sql
Layer:         Bronze
Purpose:       Implements the bronze.bronze_load stored procedure, responsible for loading
                raw CSV data into staging tables and subsequently into Bronze tables.

Description:   The procedure performs a two-step load:
                  (1) Import data from CSV files into staging tables.
                  (2) Transfer data from staging into Bronze tables using TRUNCATE + INSERT
                      to preserve a fully reloadable ingestion model.
 
Notes:         - This script centralizes Bronze ingestion logic.
               - No cleansing or validation happens here; all quality checks occur in Silver.
               - Must be executed after 04_create_staging_tables.sql.
===============================================================*/



-- Create Procedure to load each CSV into staging tables, then move to bronze with lineage ----------

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN

  DECLARE 
    @file_path    NVARCHAR(1000),
    @start_ts     DATETIME2,
    @end_ts       DATETIME2,
    @load_id      INT,
    @rowcount     BIGINT,
    @err_msg      NVARCHAR(2000),
	@errorfile_base NVARCHAR(400),
	@bulk_sql NVARCHAR(MAX),
    @source_system VARCHAR(50),
	@start_time DATETIME2,
	@end_time DATETIME2,
	@batch_start_time DATETIME2,
	@batch_end_time DATETIME2;

  PRINT '================================================';
  PRINT 'Starting bronze.load_bronze ...';
  PRINT '================================================';
  SET @batch_start_time = GETDATE();

  /* --------------------------
     1) crm_cust_info
     -------------------------- */

  BEGIN
	PRINT '>>> Inserting data into bronze.crm_cust_info ...';
	SET @start_time = GETDATE();
    SET @file_path = N'C:\SQLData\datasets\source_crm\cust_info.csv';
    SET @start_ts = SYSUTCDATETIME();
    SET @source_system = 'crm';

    INSERT INTO ctl.load_control (source_system, file_name, start_ts, status)
    VALUES (@source_system, @file_path, @start_ts, 'RUNNING');
    SET @load_id = SCOPE_IDENTITY();

    BEGIN TRY
      BEGIN TRAN;

      TRUNCATE TABLE dbo.stg_crm_cust_info;
      TRUNCATE TABLE bronze.crm_cust_info;

-- Build a unique errorfile base using load_id (no extension needed)
		SET @errorfile_base = N'C:\SQLData\bulk_errors\cust_info_err_' + CAST(@load_id AS NVARCHAR(20));

-- Build dynamic BULK INSERT statement
		SET @bulk_sql = N'
		  BULK INSERT dbo.stg_crm_cust_info
		  FROM ''' + REPLACE(@file_path,'''','''''') + '''
		  WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '','',
			ROWTERMINATOR = ''0x0d0a'',
			CODEPAGE = ''65001'',
			TABLOCK,
			BATCHSIZE = 10000,
			ERRORFILE = ''' + REPLACE(@errorfile_base,'''','''''') + '''
		  );';

-- Execute Bulk Insert into staging table
		EXEC sp_executesql @bulk_sql;

-- Load data into Bronze from staging table
      INSERT INTO bronze.crm_cust_info (
        cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date,
        dwh_load_id, dwh_file_name, dwh_received_at
      )
      SELECT cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date,
             @load_id, @file_path, @start_ts
      FROM dbo.stg_crm_cust_info;

      SELECT @rowcount = COUNT(*) FROM bronze.crm_cust_info;

      COMMIT TRAN;
	  SET @end_time = GETDATE();
	  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
	  PRINT '-------------------------------'

-- Add control info
      UPDATE ctl.load_control
      SET file_rows = @rowcount, end_ts = SYSUTCDATETIME(), status = 'SUCCESS', message = NULL
      WHERE load_id = @load_id;

      INSERT INTO ctl.load_log (load_id, step, detail)
      VALUES (@load_id, 'BULK_INSERT', CONCAT('Inserted ', @rowcount, ' rows into bronze.crm_cust_info'));

      SELECT 'crm_cust_info loaded' AS info, @rowcount AS rows;
    END TRY
    BEGIN CATCH
      IF XACT_STATE() <> 0
        ROLLBACK TRAN;

      SET @err_msg = LEFT(ERROR_MESSAGE(), 2000);

      UPDATE ctl.load_control
      SET end_ts = SYSUTCDATETIME(), status = 'FAILED', message = @err_msg
      WHERE load_id = @load_id;

      INSERT INTO ctl.load_log (load_id, step, detail)
      VALUES (@load_id, 'ERROR', @err_msg);

      PRINT 'ERROR on crm_cust_info: ' + @err_msg;
    END CATCH;
  END

  /* --------------------------
     2) crm_prd_info
     -------------------------- */
  BEGIN
	PRINT '>>> Inserting data into bronze.crm_prd_info ...';
	SET @start_time = GETDATE();
    SET @file_path = N'C:\SQLData\datasets\source_crm\prd_info.csv';
    SET @start_ts = SYSUTCDATETIME();
    SET @source_system = 'crm';

    INSERT INTO ctl.load_control (source_system, file_name, start_ts, status)
    VALUES (@source_system, @file_path, @start_ts, 'RUNNING');
    SET @load_id = SCOPE_IDENTITY();

    BEGIN TRY
      BEGIN TRAN;

      TRUNCATE TABLE dbo.stg_crm_prd_info;
      TRUNCATE TABLE bronze.crm_prd_info;

-- Build a unique errorfile base using load_id (no extension needed)
		SET @errorfile_base = N'C:\SQLData\bulk_errors\cust_info_err_' + CAST(@load_id AS NVARCHAR(20));

-- Build dynamic BULK INSERT statement
		SET @bulk_sql = N'
		  BULK INSERT dbo.stg_crm_prd_info
		  FROM ''' + REPLACE(@file_path,'''','''''') + '''
		  WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '','',
			ROWTERMINATOR = ''0x0d0a'',
			CODEPAGE = ''65001'',
			TABLOCK,
			BATCHSIZE = 10000,
			ERRORFILE = ''' + REPLACE(@errorfile_base,'''','''''') + '''
		  );';

-- Execute Bulk Insert into staging table
		EXEC sp_executesql @bulk_sql;

-- Load data into Bronze from staging table
      INSERT INTO bronze.crm_prd_info (
        prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt,
        dwh_load_id, dwh_file_name, dwh_received_at
      )
      SELECT prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt,
             @load_id, @file_path, @start_ts
      FROM dbo.stg_crm_prd_info;

      SELECT @rowcount = COUNT(*) FROM bronze.crm_prd_info;

      COMMIT TRAN;
	  SET @end_time = GETDATE();
	  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
	  PRINT '-------------------------------'

-- Add control info
      UPDATE ctl.load_control
      SET file_rows = @rowcount, end_ts = SYSUTCDATETIME(), status = 'SUCCESS', message = NULL
      WHERE load_id = @load_id;

      INSERT INTO ctl.load_log (load_id, step, detail)
      VALUES (@load_id, 'BULK_INSERT', CONCAT('Inserted ', @rowcount, ' rows into bronze.crm_prd_info'));

      SELECT 'crm_prd_info loaded' AS info, @rowcount AS rows;
    END TRY
    BEGIN CATCH
      IF XACT_STATE() <> 0
        ROLLBACK TRAN;

      SET @err_msg = LEFT(ERROR_MESSAGE(), 2000);

      UPDATE ctl.load_control
      SET end_ts = SYSUTCDATETIME(), status = 'FAILED', message = @err_msg
      WHERE load_id = @load_id;

      INSERT INTO ctl.load_log (load_id, step, detail)
      VALUES (@load_id, 'ERROR', @err_msg);

      PRINT 'ERROR on crm_prd_info: ' + @err_msg;
    END CATCH;
  END

  /* --------------------------
     3) crm_sales_details
     -------------------------- */
  BEGIN
    PRINT '>>> Inserting data into bronze.crm_sales_details ...';
	SET @start_time = GETDATE();
    SET @file_path = N'C:\SQLData\datasets\source_crm\sales_details.csv';
    SET @start_ts = SYSUTCDATETIME();
    SET @source_system = 'crm';

    INSERT INTO ctl.load_control (source_system, file_name, start_ts, status)
    VALUES (@source_system, @file_path, @start_ts, 'RUNNING');
    SET @load_id = SCOPE_IDENTITY();

    BEGIN TRY
      BEGIN TRAN;

      TRUNCATE TABLE dbo.stg_crm_sales_details;
      TRUNCATE TABLE bronze.crm_sales_details;

-- Build a unique errorfile base using load_id (no extension needed)
		SET @errorfile_base = N'C:\SQLData\bulk_errors\cust_info_err_' + CAST(@load_id AS NVARCHAR(20));

-- Build dynamic BULK INSERT statement
		SET @bulk_sql = N'
		  BULK INSERT dbo.stg_crm_sales_details
		  FROM ''' + REPLACE(@file_path,'''','''''') + '''
		  WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '','',
			ROWTERMINATOR = ''0x0d0a'',
			CODEPAGE = ''65001'',
			TABLOCK,
			BATCHSIZE = 10000,
			ERRORFILE = ''' + REPLACE(@errorfile_base,'''','''''') + '''
		  );';

-- Execute Bulk Insert into staging table
		EXEC sp_executesql @bulk_sql;

-- Load data into Bronze from staging table

      INSERT INTO bronze.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price,
        dwh_load_id, dwh_file_name, dwh_received_at
      )
      SELECT sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price,
             @load_id, @file_path, @start_ts
      FROM dbo.stg_crm_sales_details;

      SELECT @rowcount = COUNT(*) FROM bronze.crm_sales_details;

      COMMIT TRAN;
	  SET @end_time = GETDATE();
	  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
	  PRINT '-------------------------------'

-- Add control info
      UPDATE ctl.load_control
      SET file_rows = @rowcount, end_ts = SYSUTCDATETIME(), status = 'SUCCESS', message = NULL
      WHERE load_id = @load_id;

      INSERT INTO ctl.load_log (load_id, step, detail)
      VALUES (@load_id, 'BULK_INSERT', CONCAT('Inserted ', @rowcount, ' rows into bronze.crm_sales_details'));

      SELECT 'crm_sales_details loaded' AS info, @rowcount AS rows;
    END TRY
    BEGIN CATCH
      IF XACT_STATE() <> 0
        ROLLBACK TRAN;

      SET @err_msg = LEFT(ERROR_MESSAGE(), 2000);

      UPDATE ctl.load_control
      SET end_ts = SYSUTCDATETIME(), status = 'FAILED', message = @err_msg
      WHERE load_id = @load_id;

      INSERT INTO ctl.load_log (load_id, step, detail)
      VALUES (@load_id, 'ERROR', @err_msg);

      PRINT 'ERROR on crm_sales_details: ' + @err_msg;
    END CATCH;
  END

  /* --------------------------
     4) erp_loc_a101
     -------------------------- */
  BEGIN
    PRINT '>>> Inserting data into bronze.erp_loc_a101 ...';
	SET @start_time = GETDATE();
    SET @file_path = N'C:\SQLData\datasets\source_erp\loc_a101.csv';
    SET @start_ts = SYSUTCDATETIME();
    SET @source_system = 'erp';

    INSERT INTO ctl.load_control (source_system, file_name, start_ts, status)
    VALUES (@source_system, @file_path, @start_ts, 'RUNNING');
    SET @load_id = SCOPE_IDENTITY();

    BEGIN TRY
      BEGIN TRAN;

      TRUNCATE TABLE dbo.stg_erp_loc_a101;
      TRUNCATE TABLE bronze.erp_loc_a101;

-- Build a unique errorfile base using load_id (no extension needed)
		SET @errorfile_base = N'C:\SQLData\bulk_errors\cust_info_err_' + CAST(@load_id AS NVARCHAR(20));

-- Build dynamic BULK INSERT statement
		SET @bulk_sql = N'
		  BULK INSERT dbo.stg_erp_loc_a101
		  FROM ''' + REPLACE(@file_path,'''','''''') + '''
		  WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '','',
			ROWTERMINATOR = ''0x0d0a'',
			CODEPAGE = ''65001'',
			TABLOCK,
			BATCHSIZE = 10000,
			ERRORFILE = ''' + REPLACE(@errorfile_base,'''','''''') + '''
		  );';

-- Execute Bulk Insert into staging table
		EXEC sp_executesql @bulk_sql;

-- Load data into Bronze from staging table

      INSERT INTO bronze.erp_loc_a101 (
        cid, cntry,
        dwh_load_id, dwh_file_name, dwh_received_at
      )
      SELECT cid, cntry, @load_id, @file_path, @start_ts
      FROM dbo.stg_erp_loc_a101;

      SELECT @rowcount = COUNT(*) FROM bronze.erp_loc_a101;

      COMMIT TRAN;
	  SET @end_time = GETDATE();
	  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
	  PRINT '-------------------------------'

-- Add control info
      UPDATE ctl.load_control
      SET file_rows = @rowcount, end_ts = SYSUTCDATETIME(), status = 'SUCCESS', message = NULL
      WHERE load_id = @load_id;

      INSERT INTO ctl.load_log (load_id, step, detail)
      VALUES (@load_id, 'BULK_INSERT', CONCAT('Inserted ', @rowcount, ' rows into bronze.erp_loc_a101'));

      SELECT 'erp_loc_a101 loaded' AS info, @rowcount AS rows;
    END TRY
    BEGIN CATCH
      IF XACT_STATE() <> 0
        ROLLBACK TRAN;

      SET @err_msg = LEFT(ERROR_MESSAGE(), 2000);

      UPDATE ctl.load_control
      SET end_ts = SYSUTCDATETIME(), status = 'FAILED', message = @err_msg
      WHERE load_id = @load_id;

      INSERT INTO ctl.load_log (load_id, step, detail)
      VALUES (@load_id, 'ERROR', @err_msg);

      PRINT 'ERROR on erp_loc_a101: ' + @err_msg;
    END CATCH;
  END

  /* --------------------------
     5) erp_cust_az12
     -------------------------- */
  BEGIN
    PRINT '>>> Inserting data into bronze.erp_cust_az12 ...';
	SET @start_time = GETDATE();
    SET @file_path = N'C:\SQLData\datasets\source_erp\cust_az12.csv';
    SET @start_ts = SYSUTCDATETIME();
    SET @source_system = 'erp';

    INSERT INTO ctl.load_control (source_system, file_name, start_ts, status)
    VALUES (@source_system, @file_path, @start_ts, 'RUNNING');
    SET @load_id = SCOPE_IDENTITY();

    BEGIN TRY
      BEGIN TRAN;

      TRUNCATE TABLE dbo.stg_erp_cust_az12;
      TRUNCATE TABLE bronze.erp_cust_az12;

-- Build a unique errorfile base using load_id (no extension needed)
		SET @errorfile_base = N'C:\SQLData\bulk_errors\cust_info_err_' + CAST(@load_id AS NVARCHAR(20));

-- Build dynamic BULK INSERT statement
		SET @bulk_sql = N'
		  BULK INSERT dbo.stg_erp_cust_az12
		  FROM ''' + REPLACE(@file_path,'''','''''') + '''
		  WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '','',
			ROWTERMINATOR = ''0x0d0a'',
			CODEPAGE = ''65001'',
			TABLOCK,
			BATCHSIZE = 10000,
			ERRORFILE = ''' + REPLACE(@errorfile_base,'''','''''') + '''
		  );';

-- Execute Bulk Insert into staging table
		EXEC sp_executesql @bulk_sql;

-- Load data into Bronze from staging table

      INSERT INTO bronze.erp_cust_az12 (
        cid, bdate, gen,
        dwh_load_id, dwh_file_name, dwh_received_at
      )
      SELECT cid, bdate, gen, @load_id, @file_path, @start_ts
      FROM dbo.stg_erp_cust_az12;

      SELECT @rowcount = COUNT(*) FROM bronze.erp_cust_az12;

      COMMIT TRAN;
	  SET @end_time = GETDATE();
	  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
	  PRINT '-------------------------------'

-- Add control info
      UPDATE ctl.load_control
      SET file_rows = @rowcount, end_ts = SYSUTCDATETIME(), status = 'SUCCESS', message = NULL
      WHERE load_id = @load_id;

      INSERT INTO ctl.load_log (load_id, step, detail)
      VALUES (@load_id, 'BULK_INSERT', CONCAT('Inserted ', @rowcount, ' rows into bronze.erp_cust_az12'));

      SELECT 'erp_cust_az12 loaded' AS info, @rowcount AS rows;
    END TRY
    BEGIN CATCH
      IF XACT_STATE() <> 0
        ROLLBACK TRAN;

      SET @err_msg = LEFT(ERROR_MESSAGE(), 2000);

      UPDATE ctl.load_control
      SET end_ts = SYSUTCDATETIME(), status = 'FAILED', message = @err_msg
      WHERE load_id = @load_id;

      INSERT INTO ctl.load_log (load_id, step, detail)
      VALUES (@load_id, 'ERROR', @err_msg);

      PRINT 'ERROR on erp_cust_az12: ' + @err_msg;
    END CATCH;
  END

  /* --------------------------
     6) erp_px_cat_g1v2
     -------------------------- */
  BEGIN
    PRINT '>>> Inserting data into bronze.erp_px_cat_g1v2 ...';
	SET @start_time = GETDATE();
    SET @file_path = N'C:\SQLData\datasets\source_erp\px_cat_g1v2.csv';
    SET @start_ts = SYSUTCDATETIME();
    SET @source_system = 'erp';

    INSERT INTO ctl.load_control (source_system, file_name, start_ts, status)
    VALUES (@source_system, @file_path, @start_ts, 'RUNNING');
    SET @load_id = SCOPE_IDENTITY();

    BEGIN TRY
      BEGIN TRAN;

      TRUNCATE TABLE dbo.stg_erp_px_cat_g1v2;
      TRUNCATE TABLE bronze.erp_px_cat_g1v2;

-- Build a unique errorfile base using load_id (no extension needed)
		SET @errorfile_base = N'C:\SQLData\bulk_errors\cust_info_err_' + CAST(@load_id AS NVARCHAR(20));

-- Build dynamic BULK INSERT statement
		SET @bulk_sql = N'
		  BULK INSERT dbo.stg_erp_px_cat_g1v2
		  FROM ''' + REPLACE(@file_path,'''','''''') + '''
		  WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '','',
			ROWTERMINATOR = ''0x0d0a'',
			CODEPAGE = ''65001'',
			TABLOCK,
			BATCHSIZE = 10000,
			ERRORFILE = ''' + REPLACE(@errorfile_base,'''','''''') + '''
		  );';

-- Execute Bulk Insert into staging table
		EXEC sp_executesql @bulk_sql;

-- Load data into Bronze from staging table

      INSERT INTO bronze.erp_px_cat_g1v2 (
        id, cat, subcat, maintenance,
        dwh_load_id, dwh_file_name, dwh_received_at
      )
      SELECT id, cat, subcat, maintenance, @load_id, @file_path, @start_ts
      FROM dbo.stg_erp_px_cat_g1v2;

      SELECT @rowcount = COUNT(*) FROM bronze.erp_px_cat_g1v2;

      COMMIT TRAN;
	  SET @end_time = GETDATE();
	  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
	  PRINT '-------------------------------'

-- Add control info
      UPDATE ctl.load_control
      SET file_rows = @rowcount, end_ts = SYSUTCDATETIME(), status = 'SUCCESS', message = NULL
      WHERE load_id = @load_id;

      INSERT INTO ctl.load_log (load_id, step, detail)
      VALUES (@load_id, 'BULK_INSERT', CONCAT('Inserted ', @rowcount, ' rows into bronze.erp_px_cat_g1v2'));

      SELECT 'erp_px_cat_g1v2 loaded' AS info, @rowcount AS rows;
    END TRY
    BEGIN CATCH
      IF XACT_STATE() <> 0
        ROLLBACK TRAN;

      SET @err_msg = LEFT(ERROR_MESSAGE(), 2000);

      UPDATE ctl.load_control
      SET end_ts = SYSUTCDATETIME(), status = 'FAILED', message = @err_msg
      WHERE load_id = @load_id;

      INSERT INTO ctl.load_log (load_id, step, detail)
      VALUES (@load_id, 'ERROR', @err_msg);

      PRINT 'ERROR on erp_px_cat_g1v2: ' + @err_msg;
    END CATCH;
  END

  -- Final summary
  SET @batch_end_time = GETDATE();
      PRINT '================================================';
	  PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds.';
  SET @end_ts = SYSUTCDATETIME();
  PRINT 'bronze.load_bronze_all completed at ' + CONVERT(VARCHAR(30), @end_ts, 121);
  PRINT '================================================';
END;
GO

EXEC bronze.load_bronze
