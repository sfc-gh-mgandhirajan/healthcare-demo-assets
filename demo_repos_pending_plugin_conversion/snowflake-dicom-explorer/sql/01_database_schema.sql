-- =============================================================================
-- 01: Database & Schema Setup
-- Snowflake DICOM Explorer - Cardiac Imaging Intelligence Platform
-- =============================================================================

USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS EW_IMAGING_DB
    COMMENT = 'Snowflake DICOM Explorer - Cardiac Imaging Intelligence Platform. Demonstrates end-to-end unstructured DICOM processing on Snowflake.';

CREATE SCHEMA IF NOT EXISTS EW_IMAGING_DB.EXPLORER
    COMMENT = 'Primary schema for DICOM metadata, UDFs, AI enrichments, and governance objects.';

USE DATABASE EW_IMAGING_DB;
USE SCHEMA EXPLORER;
USE WAREHOUSE COMPUTE_WH;
