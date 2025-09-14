/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT('======================================================');
		PRINT('Loading silver Layer');
		PRINT('======================================================');

		PRINT('------------------------------------------------------');
		PRINT('Loading CRM table data');
		PRINT('------------------------------------------------------');
		
		SET @start_time = GETDATE();
		PRINT('>>Truncate table silver.crm_cust_info');
		TRUNCATE TABLE silver.crm_cust_info;
		
		PRINT('>>Inserting data into table silver.crm_cust_info');
		INSERT INTO [silver].[crm_cust_info] 
		(
			cst_id 
			,cst_key
			,cst_firstname
			,cst_lastname
			,cst_marital_status
			,cst_gndr
			,cst_create_date
		)

		SELECT 
			cst_id
			,cst_key
			,TRIM(REPLACE(cst_firstname,CHAR(160),'')) AS cst_firstname
			,TRIM(REPLACE(cst_lastname,CHAR(160),'')) AS cst_lastname
			,CASE UPPER(TRIM(cst_marital_status))
				WHEN 'M' THEN 'Married'
				WHEN 'S' THEN 'Single'
				ELSE 'n/a'
			END AS cst_marital_status
			,CASE UPPER(TRIM(cst_gndr))
				WHEN 'M' THEN 'Male'
				WHEN 'F' THEN 'Female'
				ELSE 'n/a'
			END AS cst_marital_status
			,cst_create_date
		FROM
		(
			SELECT 
			*
			,Rank() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_latest
			FROM [bronze].crm_cust_info 
			WHERE cst_id is not NULL 
		) AS t 
		where flag_latest=1;
		SET @end_time = GETDATE();
		PRINT('------------------------------------------------------');
		PRINT('Total time to load silver.crm_cust_info: '+CAST(DATEDIFF(millisecond,@start_time,@end_time) AS VARCHAR)+' milliseconds');
		PRINT('------------------------------------------------------');
		
		SET @start_time = GETDATE();
		PRINT('>>Truncate table silver.crm_prd_info');
		TRUNCATE TABLE silver.crm_prd_info;
		
		PRINT('>>Inserting data into table silver.crm_prd_info');
		INSERT INTO [silver].[crm_prd_info] 
		(
			prd_id 
			,cat_id
			,prd_key 
			,prd_nm 
			,prd_cost
			,prd_line
			,prd_start_dt
			,prd_end_dt
		)

		SELECT 
			prd_id 
			,REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id
			,SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key
			,prd_nm 
			,COALESCE(prd_cost,0) AS prd_cost
			,CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line
			,prd_start_dt
			,DATEADD(DAY,-1,LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
		FROM [bronze].crm_prd_info;
		SET @end_time = GETDATE();
		PRINT('------------------------------------------------------');
		PRINT('Total time to load silver.crm_prd_info: '+CAST(DATEDIFF(millisecond,@start_time,@end_time) AS VARCHAR)+' milliseconds');
		PRINT('------------------------------------------------------');
		
		SET @start_time = GETDATE();
		PRINT('>>Truncate table silver.crm_sales_details');
		TRUNCATE TABLE silver.crm_sales_details;
		
		PRINT('>>Inserting data into table silver.crm_sales_details');
		INSERT INTO [silver].[crm_sales_details]
		(
			sls_ord_num
			,sls_prd_key
			,sls_cust_id
			,sls_order_dt
			,sls_ship_dt 
			,sls_due_dt 
			,sls_sales 
			,sls_quantity 
			,sls_price 
		)

		SELECT 
			sls_ord_num
			,sls_prd_key
			,sls_cust_id
			,CASE
				WHEN (sls_order_dt=0 or LEN(sls_order_dt)!=8) THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
				END AS sls_order_dt
			,CASE
				WHEN (sls_ship_dt=0 or LEN(sls_ship_dt)!=8) THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
				END AS sls_ship_dt
			,CASE
				WHEN (sls_due_dt=0 or LEN(sls_due_dt)!=8) THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
				END AS sls_due_dt
			,CASE 
				WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price)-- AND (sls_price<>0 AND sls_price IS  not NULL) 
				THEN sls_quantity*ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales
			,sls_quantity
			,CASE
				WHEN sls_price IS NULL OR sls_price<=0 -- AND sls_sales>0 
				THEN sls_sales/NULLIF(sls_quantity,0)
				ELSE ABS(sls_price)
			END AS sls_price
		FROM [bronze].crm_sales_details;
		SET @end_time = GETDATE();
		PRINT('------------------------------------------------------');
		PRINT('Total time to load silver.sales_details: '+CAST(DATEDIFF(millisecond,@start_time,@end_time) AS VARCHAR)+' milliseconds');
		PRINT('------------------------------------------------------');
		

		PRINT('------------------------------------------------------');
		PRINT('Loading ERP table data');
		PRINT('------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>>Truncate table silver.erp_CUST_AZ12');
		TRUNCATE TABLE silver.erp_CUST_AZ12;
		
		PRINT('>>Inserting data into table silver.erp_CUST_AZ12');
		INSERT INTO silver.[erp_CUST_AZ12]
		(
			CID
			,BDATE 
			,GEN 
		)

		SELECT 
			CASE
				WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
				ELSE CID
			END AS CID
			,CASE
				WHEN BDATE > GETDATE() THEN NULL
				ELSE BDATE
			END AS BDATE 
			,CASE
				WHEN UPPER(TRIM(GEN))IN ('M','MALE') THEN 'Male'
				WHEN UPPER(TRIM(GEN))IN ('F','FEMALE') THEN 'Female'
				ELSE 'n/a'
			END AS GEN
		FROM bronze.[erp_CUST_AZ12];
		SET @end_time = GETDATE();
		PRINT('------------------------------------------------------');
		PRINT('Total time to load silver.CUST_AZ12: '+CAST(DATEDIFF(millisecond,@start_time,@end_time) AS VARCHAR)+' milliseconds');
		PRINT('------------------------------------------------------');
		
		SET @start_time = GETDATE();
		PRINT('>>Truncate table silver.erp_LOC_A101');
		TRUNCATE TABLE silver.erp_LOC_A101;
		
		PRINT('>>Inserting data into table silver.erp_LOC_A101');
		INSERT INTO silver.erp_LOC_A101
		(
			CID 
			,CNTRY 
		)

		SELECT 
			REPLACE(CID,'-','') AS CID
			,CASE
			WHEN UPPER(TRIM(CNTRY))IN ('US','USA','UNITED STATES') THEN 'United States'
			WHEN UPPER(TRIM(CNTRY))IN ('DE','GERMANY') THEN 'Germany'
			WHEN CNTRY IS NULL OR CNTRY='' THEN 'n/a'
			ELSE CNTRY
		END AS CNTRY
		FROM bronze.erp_LOC_A101;
		SET @end_time = GETDATE();
		PRINT('------------------------------------------------------');
		PRINT('Total time to load silver.LOC_A101: '+CAST(DATEDIFF(millisecond,@start_time,@end_time) AS VARCHAR)+' milliseconds');
		PRINT('------------------------------------------------------');
		
		SET @start_time = GETDATE();
		PRINT('>>Truncate table silver.erp_PX_CAT_G1V2');
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2;

		PRINT('>>Inserting data into table silver.erp_PX_CAT_G1V2');
		INSERT INTO silver.erp_PX_CAT_G1V2
		(
			ID 
			,CAT
			,SUBCAT
			,MAINTENANCE 
		)

		SELECT 
			ID 
			,CAT
			,SUBCAT
			,MAINTENANCE 
		FROM bronze.erp_PX_CAT_G1V2;
		SET @end_time = GETDATE();
		PRINT('------------------------------------------------------');
		PRINT('Total time to load silver.PX_CAT_G1V2: '+CAST(DATEDIFF(millisecond,@start_time,@end_time) AS VARCHAR)+' milliseconds');
		PRINT('------------------------------------------------------');
		
		SET @batch_end_time = GETDATE();
		PRINT('------------------------------------------------------');
		PRINT('Total time to load silver data is: '+CAST(DATEDIFF(millisecond,@batch_start_time,@batch_end_time) AS VARCHAR)+' milliseconds');
		PRINT('------------------------------------------------------');
		
		PRINT('======================================================');
		PRINT('Data Successfully Loaded to silver Layer');
		PRINT('======================================================');

	END TRY

	BEGIN CATCH
		PRINT('--------------------------------------------------------');
		PRINT('ERROR OCCURED DURING LOADING SILVER LAYER');
		PRINT('ERROR MESSAGE: '+ERROR_MESSAGE());
		PRINT('ERROR NUMBER: '+CAST(ERROR_NUMBER() AS VARCHAR));
		PRINT('ERROR STATE: '+CAST(ERROR_STATE() AS VARCHAR));
		PRINT('--------------------------------------------------------');
		
	END CATCH
END



--EXEC [silver].[load_silver]


--Select count(*) AS NumberOfRecords from [silver].crm_cust_info;
--Select count(*) AS NumberOfRecords from [silver].crm_prd_info;
--Select count(*) AS NumberOfRecords from [silver].crm_sales_details;
--Select count(*) AS NumberOfRecords from [silver].erp_CUST_AZ12;
--Select count(*) AS NumberOfRecords from [silver].erp_LOC_A101;
--Select count(*) AS NumberOfRecords from [silver].erp_PX_CAT_G1V2;




 
 

