/*==============================================================
 Script:		02_control_schema_and_tables
 Project:		SQL Data Warehouse (Medallion Architecture)

 Description:	Creates the 'ctl' schema and all supporting metadata tables
				 for ETL orchestration. These objects manage data lineage,
				 load execution, file tracking, error logging, and auditing.

 Contents:
    - Creation of 'ctl' schema
    - ETL load_control table
    - Step-by-step load_log table
    - Row-level load_errors table
    - source_system lookup table
    - Constraints, defaults, and indexes
===============================================================*/

USE DataWarehouse;
GO

/*==============================================================
1. Create Control Schema
===============================================================*/

CREATE SCHEMA ctl;
GO

/*==============================================================
2. Create Source System Lookup (Defines valid systems like CRM and ERP)
===============================================================*/

CREATE TABLE ctl.source_system (
    source_system VARCHAR(100) PRIMARY KEY,
    description   VARCHAR(1000)
);
GO

INSERT INTO ctl.source_system (source_system, description)
VALUES 
    ('crm', 'Customer Relationship Management exports'),
    ('erp', 'Enterprise Resource Planning exports');
GO


/*==============================================================
3. Create Load Control Table (Tracks high-level file ingestion and ETL execution)
===============================================================*/
CREATE TABLE ctl.load_control (
    load_id        INT IDENTITY(1,1) PRIMARY KEY,
    source_system  VARCHAR(100) NOT NULL,
    file_name      VARCHAR(512) NOT NULL,
    file_size_bigint BIGINT NULL,
    file_checksum  VARCHAR(256) NULL,
    file_rows      BIGINT NULL,

    start_ts       DATETIME2 DEFAULT SYSUTCDATETIME(),
    end_ts         DATETIME2 NULL,

    status         VARCHAR(50) NOT NULL 
                     CONSTRAINT df_load_control_status DEFAULT ('PENDING'),

    message        VARCHAR(2000) NULL,

    CONSTRAINT fk_loadcontrol_sourcesystem 
        FOREIGN KEY (source_system) REFERENCES ctl.source_system(source_system),

    CONSTRAINT ck_load_control_status 
        CHECK (status IN ('PENDING','RUNNING','SUCCESS','FAILED','RETRY'))
);
GO


/*==============================================================
4. Create Load Log Table (Stores step-by-step load execution details)
===============================================================*/
CREATE TABLE ctl.load_log (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    load_id INT NOT NULL,
    step VARCHAR(100) NOT NULL,
    detail VARCHAR(2000) NULL,
    ts DATETIME2 DEFAULT SYSUTCDATETIME(),

    CONSTRAINT fk_loadlog_loadcontrol 
        FOREIGN KEY (load_id) REFERENCES ctl.load_control(load_id)
);
GO


/*==============================================================
5. Create Load Errors Table (Tracks row-level parsing, validation, or transformation errors)
===============================================================*/
CREATE TABLE ctl.load_errors (
    error_id INT IDENTITY(1,1) PRIMARY KEY,
    load_id INT NOT NULL,
    file_row_number BIGINT NULL,
    raw_row_text VARCHAR(MAX) NULL,
    error_code VARCHAR(100) NULL,
    error_message VARCHAR(2000) NULL,
    created_at DATETIME2 DEFAULT SYSUTCDATETIME(),

    CONSTRAINT fk_loaderrors_loadcontrol 
        FOREIGN KEY (load_id) REFERENCES ctl.load_control(load_id)
);
GO


/*==============================================================
6. Add Indexes for Performance
===============================================================*/

-- Quick search by load start time
CREATE NONCLUSTERED INDEX ix_load_control_start_ts 
    ON ctl.load_control(start_ts DESC);

-- Search/filter by load status
CREATE NONCLUSTERED INDEX ix_load_control_status 
    ON ctl.load_control(status);

-- Speed joins between load_control and load_log
CREATE NONCLUSTERED INDEX ix_load_log_load_id 
    ON ctl.load_log(load_id);

GO


