-- Stages and file formats (run after 00_setup)
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO_LAB_DB;
USE SCHEMA DEMO_LAB_DB.RAW;

-- Internal stage for quick uploads
CREATE OR REPLACE STAGE raw_stage;

-- File formats
CREATE OR REPLACE FILE FORMAT ff_csv_orders TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"';
CREATE OR REPLACE FILE FORMAT ff_json_events TYPE = JSON STRIP_OUTER_ARRAY = TRUE;
CREATE OR REPLACE FILE FORMAT ff_parquet_inventory TYPE = PARQUET;

-- Storage integration for S3 (fill in role and allowed location)
-- AWS side setup (summary):
--   1) Create S3 bucket/prefix, e.g. s3://demo-lab-landing/raw/
--   2) Create IAM role (e.g. SnowflakeExternalStageRole) with:
--        - Trust policy: allow Snowflake AWS account ID with external ID from Snowflake (see DESC INTEGRATION below).
--        - Permissions policy: s3:ListBucket on the bucket (prefix raw/), s3:GetObject on bucket/raw/*; add Put/Delete if unload needed.
--   3) Get the role ARN from AWS IAM console or AWS CLI (aws iam get-role --role-name SnowflakeExternalStageRole) and place it in STORAGE_AWS_ROLE_ARN.
--
-- FOR AN EXAMPLE OF STEP-BY-STEP configuration of an S3 bucket, Role, and Policy for Snowflake, see:
-- https://www.snowflake.com/en/developers/guides/getting-started-with-snowpipe/#4
CREATE OR REPLACE STORAGE INTEGRATION lab_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<iam_role_arn>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://demo-lab-landing/raw/');

-- Note: run DESC INTEGRATION lab_s3_int to retrieve EXTERNAL_ID for IAM trust.
-- Then configure AWS IAM trust with Snowflake AWS account and external ID.

-- External stage pointing to S3 bucket/prefix
CREATE OR REPLACE STAGE raw_ext_stage
  URL='s3://demo-lab-landing/raw/'
  STORAGE_INTEGRATION=lab_s3_int;

-- Raw tables
CREATE OR REPLACE TABLE ORDERS_RAW (
  ORDER_ID STRING,
  CUSTOMER_ID STRING,
  ORDER_TS TIMESTAMP_NTZ,
  AMOUNT NUMBER(10,2),
  STATUS STRING
);

CREATE OR REPLACE TABLE EVENTS_RAW (
  EVENT_TS TIMESTAMP_NTZ,
  USER_ID STRING,
  EVENT_TYPE STRING,
  EVENT_ATTR VARIANT
);

CREATE OR REPLACE TABLE INVENTORY_RAW (
  SKU STRING,
  WAREHOUSE STRING,
  QTY NUMBER,
  UPDATED_AT TIMESTAMP_NTZ
);

