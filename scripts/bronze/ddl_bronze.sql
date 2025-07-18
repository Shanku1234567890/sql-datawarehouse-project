create table bronze.crm_cust_info(
cst_id int,

cst_key nvarchar(50),

cst_firstname nvarchar(50),

cst_lastname NVARCHAR (50),

cst_material_status NVARCHAR(50),

cst_gndr NVARCHAR(50),

cst_create_date DATE
);

create table bronze.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime);

create table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt  int,
sls_sales int,
sls_quantity int,
sls_price int
);

create table bronze.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50)
);

create table bronze.erp_cust_az12(
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50)
	);

create table bronze.erp_px_cat_g1v2(
 id nvarchar(50),
 cat  nvarchar(50),
 subcat nvarchar(50),
 maintenance nvarchar(50)
 );

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

 create or alter procedure bronze.load_bronze as

 begin
 DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
 bulk insert bronze.crm_cust_info from 'C:\Users\user\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
 with (
 firstrow=2,
 fieldterminator=',',
 tablock
 );
 SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';

 truncate table bronze.crm_prd_info;
 PRINT '>> Inserting Data Into: bronze.crm_prd_info';
 bulk insert bronze.crm_prd_info from 'C:\Users\user\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
 with (
 firstrow=2,
 fieldterminator=',',
 tablock
 );
 SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
 truncate table bronze.crm_sales_details;
 PRINT '>> Inserting Data Into: bronze.crm_sales_details';
 bulk insert bronze.crm_sales_details from 'C:\Users\user\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
 with (
 firstrow=2,
 fieldterminator=',',
 tablock
 );
 SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
 truncate table bronze.erp_cust_az12;
	print '>>Inserting Data Into: bronze.erp_cust_az12';
 bulk insert bronze.erp_cust_az12 from 'C:\Users\user\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV'
 with (
 firstrow=2,
 fieldterminator=',',
 tablock
 );
 SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';

 truncate table bronze.erp_loc_a101;
 PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
 bulk insert bronze.erp_loc_a101 from 'C:\Users\user\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV'
 with (
 firstrow=2,
 fieldterminator=',', 
 tablock
 );
 SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';

  truncate table bronze.erp_px_cat_g1v2;
  PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
 bulk insert bronze.erp_px_cat_g1v2 from 'C:\Users\user\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
 with (
 firstrow=2,
 fieldterminator=',', 
 tablock
 );
 SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
 end

