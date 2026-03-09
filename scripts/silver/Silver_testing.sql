-- ################## bronze.crm_prd_info ##################
-- Check For nulls or duplicates in PK
-- Expactation: No result
SELECT
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted Spaces
-- Expectation: No results
SELECT
	prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for nulls or negative numbers
-- Expaction: No results
SELECT
	prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL 

-- Data Std. & Consistency
SELECT DISTINCT prd_line FROM silver.crm_prd_info

-- Check for Invalid date orders
SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt
FROM silver.crm_prd_info
WHERE cat_id IN ('AC_HE')

-- ################## bronze.crm_sales_details ##################
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

-- Check for invalid dates
SELECT
	sls_order_dt
FROM silver.crm_sales_details 
WHERE sls_order_dt > '2027-01-01';

-- Check for invalid date orders
SELECT * 
FROM bronze.crm_sales_details
WHERE sls_ship_dt < sls_order_dt OR sls_order_dt > sls_due_dt

-- Check data consistency: between sales, quantity and price
-- >> Sales = Q * P
-- >> Values must not be null, zero or negative

SELECT DISTINCT
	CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity,0)
		 ELSE sls_price
	END AS sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price

-- ################## bronze.erp_cust_az12 ##################
SELECT
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, len(cid))
		ELSE cid
	END AS cid,
	CASE 
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'N/A'
	END AS gen
FROM silver.erp_cust_az12

-- Identify Out-ofRange Dates
SELECT
	bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

--Data Stand. & Consistency
SELECT DISTINCT gen,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'N/A'
	END AS gen
FROM silver.erp_cust_az12

-- ################## bronze.erp_loc_a101 ##################
SELECT
	REPLACE(cid, '-', '') AS cid,
	CASE
		WHEN cntry IN ('United States', 'US', 'USA') THEN 'United States'
		WHEN cntry = 'DE' THEN 'Germany'
		WHEN cntry = '' OR cntry IS NULL THEN 'N/A'
		ELSE TRIM(cntry)
	END AS cntry
FROM silver.erp_loc_a101

SELECT cst_key FROM silver.crm_cust_info

--Data Stand. & Consistency
SELECT 
	DISTINCT cntry,
	CASE
		WHEN cntry IN ('United States', 'US', 'USA') THEN 'United States'
		WHEN cntry = 'DE' THEN 'Germany'
		WHEN cntry = '' OR cntry IS NULL THEN 'N/A'
		ELSE cntry
	END AS cntry
FROM silver.erp_loc_a101

-- ################## bronze.erp_loc_a101 ##################
-- # ID CO_PD NOT IN silver.crm_prd_info?
SELECT
	id,
	cat,
	subcat,
	maintenance
FROM silver.erp_px_cat_g1v2 
ORDER BY id

SELECT
	*
FROM silver.crm_prd_info
ORDER BY cat_id

-- Check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2 
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat)

--Data standard. & Consistency
SELECT
DISTINCT cat
FROM bronze.erp_px_cat_g1v2 

SELECT
DISTINCT subcat
FROM bronze.erp_px_cat_g1v2 

SELECT
DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2 


