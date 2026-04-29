/*
-----------------------------------------------------------------
Stored procedure script - Loads data into bronze schema using the external csv files
	- truncates the table before loading the data
	- uses BULK insert to load the data from external csv

-----------------------------------------------------------------
*/

EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @initial_time DATETIME, @final_time DATETIME
	BEGIN TRY
		SET @initial_time = GETDATE();
		PRINT 'Loading Bronze layer';
		PRINT '====================================================='

		PRINT 'Loading CRM tables';
		
		SET @start_time = GETDATE();
		PRINT '-> Truncating data in bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '-> Inserting data in bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'G:\dataset\Datasets and files\datawarehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK
			  );
		SET @end_time = GETDATE();
		PRINT 'Load duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's'
		PRINT '>> ----------'
		PRINT '              '

		SET @start_time = GETDATE();
		PRINT '-> Truncating data in bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT '-> Inserting data in bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'G:\dataset\Datasets and files\datawarehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK
			  );
		SET @end_time = GETDATE();
		PRINT 'Load duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's'
		PRINT '>> ----------'
		PRINT '              '

		SET @start_time = GETDATE();
		PRINT '-> Truncating data in bronze.crm_sales_details'
		TRUNCATE TABLE  bronze.crm_sales_details

		PRINT '-> Inserting data in bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'G:\dataset\Datasets and files\datawarehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK
			  );
		SET @end_time = GETDATE();
		PRINT 'Load duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's'
		PRINT '>> ----------'
		PRINT '              '

		PRINT '-----------------------------------------------------'
		PRINT 'Loading ERP tables';

		SET @start_time = GETDATE();
		PRINT '-> Truncating data in bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '-> Inserting data in bronze.erp_cust_az12' 
		BULK INSERT bronze.erp_cust_az12
		FROM 'G:\dataset\Datasets and files\datawarehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK
			  );
		SET @end_time = GETDATE();
		PRINT 'Load duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's'
		PRINT '>> ----------'
		PRINT '              '

		SET @start_time = GETDATE();
		PRINT '-> Truncating data in bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101

		PRINT 'Inserting data in erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'G:\dataset\Datasets and files\datawarehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK
			  );
		SET @end_time = GETDATE();
		PRINT 'Load duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's'
		PRINT '>> ----------'
		PRINT '              '

		SET @start_time = GETDATE();
		PRINT '-> Truncating data in bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT 'Inserting data in erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'G:\dataset\Datasets and files\datawarehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK
			  );
		SET @end_time = GETDATE();
		PRINT 'Load duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's'
		PRINT '>> ----------'
		PRINT '              '

		PRINT '-----------------------------------------------------'
		SET @final_time = GETDATE();
		PRINT 'Bronze layer completion time is ' + CAST(DATEDIFF(second, @initial_time, @final_time) AS NVARCHAR) + 's'
	END TRY
	BEGIN CATCH
		PRINT 'Failed to load in Bronze layer due to:' ;
		PRINT ERROR_MESSAGE();
		PRINT ERROR_NUMBER();
		PRINT ERROR_STATE();
	END CATCH
END
