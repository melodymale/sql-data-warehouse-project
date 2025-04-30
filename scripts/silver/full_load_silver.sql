/*
===============================================================================
Full Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This script stored the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Queries Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
===============================================================================
*/

TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)

SELECT cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE 
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	ELSE 'n/a'
END AS cst_marital_status,
CASE 
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	ELSE 'n/a'
END AS cst_gndr,
cst_create_date
FROM (
	SELECT *,
	RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flat_last
	FROM bronze.crm_cust_info
	WHERE cst_id != 0
) t
WHERE flat_last = 1;

TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)

SELECT prd_id,
REPLACE(SUBSTR(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTR(prd_key, 7, LENGTH(prd_key)) AS prd_key,
prd_nm,
prd_cost,
CASE prd_line
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(DATE_SUB(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY) AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)

SELECT sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE
	WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
	ELSE DATE(sls_order_dt)
END AS sls_order_dt,
CASE
	WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
	ELSE DATE(sls_ship_dt)
END AS sls_ship_dt,
CASE
	WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
	ELSE DATE(sls_due_dt)
END AS sls_due_dt,
CASE
	WHEN sls_sales != sls_quantity * sls_price AND sls_price != 0 THEN sls_quantity * ABS(sls_price)
	ELSE ABS(sls_sales)
END AS sls_sales,
sls_quantity,
CASE
	WHEN sls_price = 0 THEN ABS(sls_sales) / sls_quantity
	ELSE ABS(sls_price)
END AS sls_price
FROM bronze.crm_sales_details;

TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
)

SELECT CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTR(cid, 4, LENGTH(cid))
	ELSE cid	
END AS cid,
CASE
	WHEN bdate > CURRENT_DATE() THEN NULL
	ELSE bdate 
END AS bdate,
CASE
	WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry
)

SELECT REPLACE(cid, '-', '') AS cid,
CASE
	WHEN cntry = 'DE' THEN 'Germany'
	WHEN cntry IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
)

SELECT id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;