/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT('======================================================');
		PRINT('Loading Bronze Layer');
		PRINT('======================================================');

		PRINT('------------------------------------------------------');
		PRINT('Loading CRM table data');
		PRINT('------------------------------------------------------');
		
		SET @start_time = GETDATE();
		PRINT('>>Truncate table bronze.crm_cust_info');
		TRUNCATE TABLE bronze.crm_cust_info;
		
		PRINT('>>Inserting data into table bronze.crm_cust_info');
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Dolly\Knowledge\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH
		(
			FIRSTROW=2
			,FIELDTERMINATOR=','
			,TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('------------------------------------------------------');
		PRINT('Total time to load bronze.crm_cust_info: '+CAST(DATEDIFF(millisecond,@start_time,@end_time) AS VARCHAR)+' milliseconds');
		PRINT('------------------------------------------------------');
		
		SET @start_time = GETDATE();
		PRINT('>>Truncate table bronze.crm_prd_info');
		TRUNCATE TABLE bronze.crm_prd_info;
		
		PRINT('>>Inserting data into table bronze.crm_prd_info');
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Dolly\Knowledge\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH
		(
			FIRSTROW=2
			,FIELDTERMINATOR=','
			,TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('------------------------------------------------------');
		PRINT('Total time to load bronze.crm_prd_info: '+CAST(DATEDIFF(millisecond,@start_time,@end_time) AS VARCHAR)+' milliseconds');
		PRINT('------------------------------------------------------');
		
		SET @start_time = GETDATE();
		PRINT('>>Truncate table bronze.crm_sales_details');
		TRUNCATE TABLE bronze.crm_sales_details;
		
		PRINT('>>Inserting data into table bronze.crm_sales_details');
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Dolly\Knowledge\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH
		(
			FIRSTROW=2
			,FIELDTERMINATOR=','
			--,ROWTERMINATOR = '\n'
			--,ERRORFILE = 'D:\error_log.txt'
			,TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('------------------------------------------------------');
		PRINT('Total time to load bronze.sales_details: '+CAST(DATEDIFF(millisecond,@start_time,@end_time) AS VARCHAR)+' milliseconds');
		PRINT('------------------------------------------------------');
		

		
		PRINT('------------------------------------------------------');
		PRINT('Loading ERP table data');
		PRINT('------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>>Truncate table bronze.erp_CUST_AZ12');
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		
		PRINT('>>Inserting data into table bronze.erp_CUST_AZ12');
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'D:\Dolly\Knowledge\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH
		(
			FIRSTROW=2
			,FIELDTERMINATOR=','
			,TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('------------------------------------------------------');
		PRINT('Total time to load bronze.CUST_AZ12: '+CAST(DATEDIFF(millisecond,@start_time,@end_time) AS VARCHAR)+' milliseconds');
		PRINT('------------------------------------------------------');
		
		SET @start_time = GETDATE();
		PRINT('>>Truncate table bronze.erp_LOC_A101');
		TRUNCATE TABLE bronze.erp_LOC_A101;
		
		PRINT('>>Inserting data into table bronze.erp_LOC_A101');
		BULK INSERT bronze.erp_LOC_A101
		FROM 'D:\Dolly\Knowledge\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH
		(
			FIRSTROW=2
			,FIELDTERMINATOR=','
			,TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('------------------------------------------------------');
		PRINT('Total time to load bronze.LOC_A101: '+CAST(DATEDIFF(millisecond,@start_time,@end_time) AS VARCHAR)+' milliseconds');
		PRINT('------------------------------------------------------');
		
		SET @start_time = GETDATE();
		PRINT('>>Truncate table bronze.erp_PX_CAT_G1V2');
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

		PRINT('>>Inserting data into table bronze.erp_PX_CAT_G1V2');
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'D:\Dolly\Knowledge\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH
		(
			FIRSTROW=2
			,FIELDTERMINATOR=','
			,TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('------------------------------------------------------');
		PRINT('Total time to load bronze.PX_CAT_G1V2: '+CAST(DATEDIFF(millisecond,@start_time,@end_time) AS VARCHAR)+' milliseconds');
		PRINT('------------------------------------------------------');
		
		SET @batch_end_time = GETDATE();
		PRINT('------------------------------------------------------');
		PRINT('Total time to load bronze data is: '+CAST(DATEDIFF(millisecond,@batch_start_time,@batch_end_time) AS VARCHAR)+' milliseconds');
		PRINT('------------------------------------------------------');
		
		PRINT('======================================================');
		PRINT('Data Successfully Loaded to Bronze Layer');
		PRINT('======================================================');

	END TRY

	BEGIN CATCH
		PRINT('--------------------------------------------------------');
		PRINT('ERROR OCCURED DURING LOADING BRONZE LAYER');
		PRINT('ERROR MESSAGE: '+ERROR_MESSAGE());
		PRINT('ERROR NUMBER: '+CAST(ERROR_NUMBER() AS VARCHAR));
		PRINT('ERROR STATE: '+CAST(ERROR_STATE() AS VARCHAR));
		PRINT('--------------------------------------------------------');
		
	END CATCH
END



--EXEC [bronze].[load_bronze]


--Select count(*) AS NumberOfRecords from [bronze].[crm_cust_info];
--Select count(*) AS NumberOfRecords from [bronze].crm_prd_info;
--Select count(*) AS NumberOfRecords from [bronze].crm_sales_details;
--Select count(*) AS NumberOfRecords from [bronze].erp_CUST_AZ12;
--Select count(*) AS NumberOfRecords from [bronze].erp_LOC_A101;
--Select count(*) AS NumberOfRecords from [bronze].erp_PX_CAT_G1V2;
