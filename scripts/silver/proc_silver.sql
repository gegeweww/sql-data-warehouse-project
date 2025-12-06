/*
===============================================================================
Stored Procedure: Load silver Layer (Source -> silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from external CSV files. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to silver tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=============================================================================='
		PRINT 'Loading silver Layer'
		PRINT '=============================================================================='

		PRINT '------------------------------------------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '------------------------------------------------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Insert to Table: silver.crm_cust_info'
		BULK INSERT silver.crm_cust_info
		FROM 'D:\LEARN WHATEVER YOU WANT\MYSQL\DATA WITH BARA\DATASET\SOURCE_CRM\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK	
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'second';
		PRINT '---------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Insert to Table: silver.crm_prd_info'
		BULK INSERT silver.crm_prd_info
		FROM 'D:\LEARN WHATEVER YOU WANT\MYSQL\DATA WITH BARA\DATASET\SOURCE_CRM\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK	
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'second';
		PRINT '---------'

		PRINT '>> Truncating Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Insert to Table: silver.crm_sales_details'
		BULK INSERT silver.crm_sales_details
		FROM 'D:\LEARN WHATEVER YOU WANT\MYSQL\DATA WITH BARA\DATASET\SOURCE_CRM\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK	
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'second';
		PRINT '---------'

		PRINT '------------------------------------------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '------------------------------------------------------------------------------'

		PRINT '>> Truncating Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Insert to Table: silver.erp_cust_az12'
		BULK INSERT silver.erp_cust_az12
		FROM 'D:\LEARN WHATEVER YOU WANT\MYSQL\DATA WITH BARA\DATASET\SOURCE_ERP\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK	
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'second';
		PRINT '---------'


		PRINT '>> Truncating Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Insert to Table: silver.erp_loc_a101'
		BULK INSERT silver.erp_loc_a101
		FROM 'D:\LEARN WHATEVER YOU WANT\MYSQL\DATA WITH BARA\DATASET\SOURCE_ERP\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK	
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'second';
		PRINT '---------'
		
		
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Insert to Table: silver.erp_pc_cat_g1v2'
		BULK INSERT silver.erp_px_cat_g1v2
		FROM 'D:\LEARN WHATEVER YOU WANT\MYSQL\DATA WITH BARA\DATASET\SOURCE_ERP\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK	
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'second';
		PRINT '---------'

		SET @batch_end_time = GETDATE();
		PRINT '=============================================================================='
		PRINT 'Loading silver Layer is Completed'
		PRINT '	- Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR) + 'second';
		PRINT '=============================================================================='

	END TRY
	BEGIN CATCH
		PRINT '=============================================================================='
		PRINT 'ERROR OCCURED DURING LOADING silver LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS VARCHAR) ;
		PRINT 'Error Message' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT '=============================================================================='
	END CATCH
END
