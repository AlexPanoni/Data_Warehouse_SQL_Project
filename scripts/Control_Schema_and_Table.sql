USE DataWarehouse

-- Create control schema and tables
CREATE SCHEMA ctl;
GO

CREATE TABLE ctl.load_control (
  load_id INT IDENTITY(1,1) PRIMARY KEY,
  source_system VARCHAR(100),
  file_name VARCHAR(512),
  file_size_bigint BIGINT,
  file_checksum VARCHAR(256),
  file_rows BIGINT,
  start_ts DATETIME2,
  end_ts DATETIME2,
  status VARCHAR(50),
  message VARCHAR(2000)
);
GO

CREATE TABLE ctl.load_log (
  log_id INT IDENTITY(1,1) PRIMARY KEY,
  load_id INT,
  step VARCHAR(100),
  detail VARCHAR(2000),
  ts DATETIME2 DEFAULT SYSUTCDATETIME(),
  CONSTRAINT fk_loadlog_loadcontrol FOREIGN KEY (load_id) REFERENCES ctl.load_control(load_id)
);
GO
