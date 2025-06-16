CREATE DATABASE IF NOT EXISTS lakehouse_metadata;

SELECT 'Running init.sql...';

USE lakehouse_metadata;

-- ==========================
-- 1. BRONZE_METADATA TABLE
-- ==========================
CREATE TABLE bronze_metadata (
    batch_id      VARCHAR(50) PRIMARY KEY,
    start_time    DATETIME NOT NULL,
    end_time      DATETIME,
    status        VARCHAR(20),
    source_path   TEXT
);

-- ==========================
-- 2. SILVER_METADATA TABLE
-- ==========================
CREATE TABLE silver_metadata (
    batch_id         VARCHAR(50) PRIMARY KEY,
    bronze_batch_id  VARCHAR(50),
    start_time       DATETIME NOT NULL,
    end_time         DATETIME,
    status           VARCHAR(20),
    source_path      TEXT,
    FOREIGN KEY (bronze_batch_id) REFERENCES bronze_metadata(batch_id)
);

-- ==========================
-- 3. GOLD_METADATA TABLE
-- ==========================
CREATE TABLE gold_metadata (
    batch_id         VARCHAR(50) PRIMARY KEY,
    silver_batch_id  VARCHAR(50),
    start_time       DATETIME NOT NULL,
    end_time         DATETIME,
    status           VARCHAR(20),
    source_path      TEXT,
    table_type       VARCHAR(50),
    FOREIGN KEY (silver_batch_id) REFERENCES silver_metadata(batch_id)
);
