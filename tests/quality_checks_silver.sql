/*
=======================================================================
Quality Checks
=======================================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracym
  and standardization across the 'silver' schemas. It includes checks for:
  - Null or duplicate primary keys.
  - Unwanted spaces in string fields. 
  - Data standardization and consistency.
  - Invalid date range and orders.
  - Data consistency between related fields.

Usage Notes: 
  - Run these checks agter data loading silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
=====================================================================
*/


/*======================================================
Quick check for crm
========================================================
*/

/*======================================================
silver.crm_cust_info
========================================================
*/

--Checking for null or duplicate in the Primary Key
--Expectation: No Result

SELECT 
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

------------------------------------------------------------

-- Checking For No Spaces
--For firstname
SELECT 
	cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) -- Check same for lastname and cst_gndr

------------------------------------------------------------

-- Data Standardization & Consistency
-- FOR cst_gndr
SELECT 
	DISTINCT cst_gndr
FROM silver.crm_cust_info;

-- ===============================================
-- For cst_marital_status
SELECT 
	DISTINCT cst_marital_status
FROM silver.crm_cust_info;
-------------------------------------------------------

/*======================================================
silver.crm_prd_info
========================================================
*/

--Checking for null or duplicate in the Primary Key
--Expectation: No Result
SELECT 
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- cat_id
SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-', '_')  AS cat_id --Extracting category ID
FROM silver.crm_prd_info

--prd_key
SELECT 
	prd_id,
	SUBSTRING(prd_key,7,LEN(prd_key))  AS prd_id -- Extracting product key
FROM silver.crm_prd_info
------------------------------------------------------------

-- Checking For No Spaces
--For prd_nm
SELECT 
	prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) 
------------------------------------------------------------
--Checking for null or Negative Numbers
--Expectation: No Result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

------------------------------------------------------------

-- Data Standardization & Consistency
-- FOR cst_gndr
SELECT 
	DISTINCT prd_line
FROM silver.crm_prd_info;

-- ===============================================
-- Checking Invalid Date
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

--------------------------------------------------------
SELECT *
FROM silver.crm_prd_info


/*======================================================
silver.crm_sales_details
========================================================
*/
-- Checking Invalid Date
SELECT 
	sls_order_dt
FROM silver.crm_sales_details 

--------------------------------------------------------

-- Check for Invalid Date Orders
SELECT * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-------------------------------------------------------------

--Checking for Data Consistency Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be Null, zero, or negative.
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0

----------------------------------------------------------

/*======================================================
Quick check for erp
========================================================
*/

/*
====================================================
silver.erp_cust_az12
====================================================
*/
-- Identifying Out-of-Range Dates
SELECT DISTINCT 
	bdate 
FROM silver.erp_cust_az12
WHERE bdate > GETDATE()
---------------------------------------------------

-- Date Standization & Consistency
SELECT DISTINCT 
	gen 
FROM silver.erp_cust_az12

-- Solution
SELECT DISTINCT 
CASE 
	WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
	ELSE 'n/a'
END AS gen 
FROM silver.erp_cust_az12
----------------------------------------------------

/*
====================================================
silver.erp_loc_a101
====================================================
*/

-- Date Standization & Consistency
SELECT DISTINCT
	cntry
FROM silver.erp_loc_a101
-----------------------------------------------------

/*
====================================================
silver.erp_px_cat_g1v2
====================================================
*/
--Check for unwanted spaces
SELECT 
	*
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Date Standization & Consistency
SELECT DISTINCT
	cat
FROM silver.erp_px_cat_g1v2

SELECT DISTINCT
	subcat
FROM silver.erp_px_cat_g1v2

SELECT DISTINCT
	maintenance
FROM silver.erp_px_cat_g1v2
------------------------------------------------------
