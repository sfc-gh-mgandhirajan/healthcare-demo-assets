----------------------------------------------------------------------
-- ER Patient Admissions: Interactive Tables & Warehouses Demo
-- Step 6: Cleanup — Drop all demo objects
----------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

DROP STREAMLIT IF EXISTS ER_INTERACTIVE_DEMO.ER_DATA.ER_COMMAND_CENTER;

ALTER COMPUTE POOL ER_DEMO_POOL STOP ALL;
DROP COMPUTE POOL IF EXISTS ER_DEMO_POOL;

ALTER WAREHOUSE ER_INTERACTIVE_WH SUSPEND;
DROP WAREHOUSE IF EXISTS ER_INTERACTIVE_WH;
DROP WAREHOUSE IF EXISTS ER_STANDARD_WH;

DROP DATABASE IF EXISTS ER_INTERACTIVE_DEMO;
