-- EXEC silver.load_silver;

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
DECLARE @start_date DATETIME, @end_date DATETIME, @batch_start_date DATETIME, @batch_end_date DATETIME
	BEGIN TRY
	--------------------------------------------------------------------------
	SET @batch_start_date = GETDATE()
	PRINT '===================================================================';
	PRINT 'Loading Silver Layer'
	PRINT '===================================================================';

	PRINT '-------------------------------------------------------------------';
	PRINT 'Loading CRM Tables'
	PRINT '-------------------------------------------------------------------';
	
	SET @start_date = GETDATE()
	PRINT 'Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;

	PRINT 'Inserting Into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)

	SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END cst_marital_status, -- Normalize marital status values to readable format
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		ELSE 'n/a'
	END cst_gndr, -- Normalize gender values to readable format
	cst_create_date
	FROM (
	-- Removing of Duplicate
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	)t WHERE flag_last = 1;

	SET @end_date = GETDATE()
	PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR) + ' seconds';
	PRINT '-------------------------------------';

	------------------------------------------------------------------------------------
	SET @start_date = GETDATE()

	PRINT '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT 'Inserting Data Into: silver.crm_prd_info'
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

	SELECT 
		prd_id
		,REPLACE(SUBSTRING(TRIM(prd_key),1,5), '-', '_') AS cat_id	-- Extract Category ID
		,SUBSTRING(TRIM(prd_key),7,LEN(prd_key)) AS prd_key			-- Extract Product ID
		,TRIM(prd_nm) AS prd_nm
		,ISNULL(prd_cost, 0) AS prd_cost
		,CASE
			WHEN UPPER(prd_line) = 'R' THEN 'Road'
			WHEN UPPER(prd_line) = 'S' THEN 'Other Sales'
			WHEN UPPER(prd_line) = 'M' THEN 'Mountain'
			WHEN UPPER(prd_line) = 'T' THEN 'Touring'
			ELSE 'n/a'
		END prd_line  -- Map product line codes to descriptive values
		,CAST(prd_start_dt AS DATE)
		,CAST (LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
		--Calculate end date as one day before the next start date
	FROM bronze.crm_prd_info;

	SET @end_date = GETDATE()
	PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR) + ' seconds';
	PRINT '-------------------------------------';

	----------------------------------------------------------------------------------------
	SET @start_date = GETDATE()

	PRINT '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details (
		sls_ord_num	,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)


	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END sls_order_dt,
		CASE 
			WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END sls_ship_dt,
		CASE 
			WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END sls_due_dt,
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales -- Recalculate sales if original value is missing or incorrect
		END sls_sales,
		sls_quantity,
		CASE 
			WHEN sls_price IS NULL OR sls_price <= 0
				THEN ABS(sls_sales) / NULLIF(sls_quantity,0)
			ELSE sls_price -- Derive price if original value is invalid
		END sls_price
	FROM bronze.crm_sales_details;

	SET @end_date = GETDATE()
	PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR) + ' seconds';
	PRINT '-------------------------------------';

	------------------------------------------------------------------------------
	PRINT '-------------------------------------------------------------------';
	PRINT 'Loading ERP Tables'
	PRINT '-------------------------------------------------------------------';
	
	SET @start_date = GETDATE()

	PRINT 'Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;

	PRINT 'Inserting Into: silver.erp_cust_az12'
	INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)

	SELECT
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) --Remove 'NAS' prefix if present
			ELSE cid
		END AS cid,
		CASE
			WHEN bdate > GETDATE() THEN NULL -- Remove date that are more than current date
			ELSE bdate
		END AS bdate,
		CASE 
			WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
			ELSE 'n/a'
		END AS gen
	FROM bronze.erp_cust_az12
	SET @end_date = GETDATE()
	PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR) + ' seconds';
	PRINT '-------------------------------------';

	-----------------------------------------------------------------------------------
	SET @start_date = GETDATE()

	PRINT 'Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT 'Inserting Into: silver.erp_loc_a101'
	INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
	)

	SELECT 
		REPLACE([cid], '-', '') AS cid, -- Replacing '-' with a ''
		CASE
			WHEN UPPER(TRIM(cntry)) IN ('DE', 'Germany') THEN 'Germany'
			WHEN UPPER(TRIM(cntry)) IN ('USA', 'United States', 'US') THEN 'USA'
			WHEN UPPER(TRIM(cntry)) IN ('DE', 'Germany') THEN 'Germany'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
	FROM bronze.erp_loc_a101

	SET @end_date = GETDATE()
	PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR) + ' seconds';
	PRINT '-------------------------------------';

	-------------------------------------------------------------------------------------
	SET @start_date = GETDATE()

	PRINT 'Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT 'Inserting Into: silver.erp_px_cat_g1v2'
	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)

	SELECT 
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;
	SET @end_date = GETDATE()
	PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR) + ' seconds';
	PRINT '-------------------------------------';
	----------------------------------------------------------------------------
	END TRY
	----------------------------------------------------------------------------
	BEGIN CATCH
		PRINT '===========================================================';
		PRINT 'ERROR OCCURED DURING LOADING OF THE SILVER LAYER';
		PRINT 'Error Messeage ' + ERROR_MESSAGE();
		PRINT 'Error Messeage ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Messeage ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===========================================================';
	END CATCH;

	SET @batch_end_date = GETDATE()
	PRINT 'Loading Silver Layer is Completed'
	PRINT '- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_date, @batch_end_date) AS NVARCHAR) + ' seconds';
	PRINT '-------------------------------------';
END;

