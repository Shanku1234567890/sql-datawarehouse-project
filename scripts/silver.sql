create table silver.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname NVARCHAR (50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE,
dwh_create_date datetime2 default getdate()
);

create table silver.crm_prd_info(
prd_id int,
cat_id nvarchar(50),
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date datetime2 default getdate());


create table silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt  date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date datetime2 default getdate()
);

create table silver.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50),
dwh_create_date datetime2 default getdate()
);


create table silver.erp_cust_az12(
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50),
dwh_create_date datetime2 default getdate()
	);
	
	
create table silver.erp_px_cat_g1v2(
 id nvarchar(50),
 cat  nvarchar(50),
 subcat nvarchar(50),
 maintenance nvarchar(50),
dwh_create_date datetime2 default getdate()
 );





create or alter procedure silver.load_silver as
begin
-- main transformation and loading from bronze
PRINT '>> Truncating Table: silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;
PRINT '>> Inserting Data Into: silver.crm_cust_info';
  insert into silver.crm_cust_info(
  cst_id,
  cst_key,
  cst_firstname,
  cst_lastname,
  cst_material_status,
  cst_gndr,
  cst_create_date)

  select cst_id,cst_key,
  trim(cst_firstname) as firstname, --for triming leading and trailing space
  trim(cst_lastname) as lastname, --for triming leading and trailing space

  case upper(trim(cst_material_status))  --changing m to male and f to female and null to n/a
  when'F'then'FEMALE'
  when'M'then 'Male'
  else
  'N/A'
  end cst_material_status,

  case upper(trim(cst_gndr)) --changing m to male and f to female and null to n/a
  when'F'then'FEMALE'
  when'M'then 'Male'
  else
  'N/A'
  end cst_gndr,
  cst_create_date 
  from(
  select *,row_number() over(partition by cst_id order by cst_create_date desc)as rn
  from bronze.crm_cust_info)t
  where rn=1 -- main for ranking
  update silver.crm_cust_info
  set cst_material_status='N/A'
  WHERE cst_material_status='n/a'

   update silver.crm_cust_info
  set cst_gndr='N/A'
  WHERE cst_gndr='n/a'


  -- for crm_prd_info
  PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
INSERT INTO silver.crm_prd_info (
prd_id,
prd_key,
cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt)
SELECT
prd_id,
REPLACE (SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, 
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
isnull(prd_cost,0) as prd_cost,
case upper(trim(prd_line))
 when 'M' then 'mountain'
 when 'R' then 'Road'
 when 'S' then 'Sales'
 when 'T' then 'Touring'
 else 'N/A'
 end prd_line,
CAST (prd_start_dt AS DATE) AS prd_start_dt, 
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

-- for sales details
PRINT '>> Truncating Table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> Inserting Data Into: silver.crm_sales_details';
INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price)
SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;

--for erp1 table
PRINT '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> Inserting Data Into: silver.erp_cust_az12';
insert into silver.erp_cust_az12(
cid,bdate,gen)
select 
case when cid like 'NAS%' then substring(cid,4,len(cid))
else cid
end cid,
case when bdate>getdate() then null
else bdate
end bid,
case when upper(trim(gen))in ('F','Female') then 'Female'
when upper(trim(gen)) in ('M','Male') then 'Male'
else 'N/A'
end gen
from bronze.erp_cust_az12

-- for location table
PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
insert into silver.erp_loc_a101(cid,cntry)
SELECT
REPLACE(cid, '-', '') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
ELSE TRIM(cntry)
end cntry
FROM bronze.erp_loc_a101


-- for cat table last
PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
INSERT INTO silver.erp_px_cat_g1v2 (
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
end

exec silver.load_silver
