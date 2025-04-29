/*
===============================================================================
Full Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored sql queries for loading data into the 'bronze' database from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `LOAD DATA` command to load data from csv Files to bronze tables.
===============================================================================
*/

TRUNCATE TABLE bronze.crm_cust_info;

LOAD DATA INFILE '/var/lib/mysql-files/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.crm_prd_info;

LOAD DATA INFILE '/var/lib/mysql-files/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.crm_sales_details;

LOAD DATA INFILE '/var/lib/mysql-files/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.erp_cust_az12;

LOAD DATA INFILE '/var/lib/mysql-files/source_erp/CUST_AZ12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.erp_loc_a101;

LOAD DATA INFILE '/var/lib/mysql-files/source_erp/LOC_A101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.erp_px_cat_g1v2;

LOAD DATA INFILE '/var/lib/mysql-files/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;