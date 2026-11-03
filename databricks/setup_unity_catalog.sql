-- Create catalog
CREATE CATALOG IF NOT EXISTS retail_sales_lakehouse;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS retail_sales_lakehouse.bronze;
CREATE SCHEMA IF NOT EXISTS retail_sales_lakehouse.silver;
CREATE SCHEMA IF NOT EXISTS retail_sales_lakehouse.gold;
CREATE SCHEMA IF NOT EXISTS retail_sales_lakehouse.landing;

-- Create managed landing volume
CREATE volume IF NOT EXISTS retail_sales_lakehouse.landing.raw_files;