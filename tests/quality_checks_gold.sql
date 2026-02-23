/*
===============================================================================
Quality Checks:
===============================================================================
Script Puropse: 
    This script performs quality checks to validate the integrity, consistency,
    and accuracy of the Gold Layer.
    - Uniqueness of surrogate keys in the dimension tables.
    - Referential Integrity between facts and dimension tables. 
    - validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Run these checks after data loading silver layer
    - Investigate and resolve any discrepensies found during the checks.
*/

-------------------------------------------------------------------------
--Checking 'gold.dim_customers'
-------------------------------------------------------------------------
--Checking for Duplicates in Customer ID
SELECT cst_id, COUNT(*) FROM
(SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON		  ci.cst_key = la.cid
)t GROUP BY cst_id
HAVING COUNT(*) > 1


-- Data Integration 
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen, 
	CASE 
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr --CRM is the Master for gender Info
		ELSE COALESCE(ca.gen, 'n/a')
	END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON		  ci.cst_key = la.cid
ORDER BY 1,2

--Checking for distinct customers
SELECT DISTINCT gender FROM gold.dim_customers

-------------------------------------------------------------------------
--Checking 'gold.dim_products'
-------------------------------------------------------------------------

--Checking for duplicate in Product ID
SELECT prd_key, COUNT(*) FROM
(
	SELECT
		pn.prd_id,
		pn.prd_key,
		pn.prd_nm,
		pn.cat_id,
		pc.cat,
		pc.subcat,
		pc.maintenance,
		pn.prd_cost,
		pn.prd_line,
		pn.prd_start_dt
	FROM silver.crm_prd_info pn
	LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON		  pn.cat_id = pc.id
	WHERE pn.prd_end_dt IS NULL
)t GROUP BY prd_key
HAVING COUNT(*) > 1

-------------------------------------------------------------------------
--Checking 'gold.fact_sales'
-------------------------------------------------------------------------

-- Foreign Key Integrity (Dimensions)
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE c.customer_key IS NULL
-- p.product_key IS NULL
