--- Store procedure: Load Silver Layer (bronze --> Silver)
Script Purpose:
  -- This store procedure performs the ETL( Extract, Tranform, Load) process to populate the 'silver' schema tables from the'bronze' schema
  

--Silver Table ---

Create or alter PROCEDURE silver.load_silver AS
BEGIN
    declare 
    @start_time Datetime,
    @end_time DATETIME,
    @batch_start_time Datetime,
    @batch_end_time DATETIME
    begin try 
    set  @start_time = GETDATE();
    print'Loading Silver Layer'

    Print 'Loading Crm Tables'
-- Loading silver.crn_cust_info
SET @start_time = getdate();
PRINT'>> Truncating Table:silver.crm_cust_info';
truncate table silver.crm_cust_info;
print '>> Inserting Data Into: silver.crm_cust_info'
-- Insert Silver 
Insert into silver.crm_cust_info(
    cst_id,
    cst_key ,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date 
)
SELECT
 cst_id,
 cst_key ,
 Trim(cst_firstname) as cst_firstname ,
 TRIM(cst_lastname) AS cst_lastname,
 case 
    when UPPER(TRIM( cst_marital_status)) ='S' THEN 'Single'
    when UPPER(TRIM( cst_marital_status)) ='M' then 'Married'
    else 'N/A'
end as cst_marital_status,
 
 case 
    when UPPER(TRIM( cst_gndr)) ='F' THEN 'Female'
    when UPPER(TRIM(cst_gndr)) ='M' then 'Male'
    else 'N/A'
end as cst_gndr,-- Normalize gender values to reable format
cst_create_date 
FROM(
    select 
    *,
    ROW_NUMBER() over(Partition by cst_id order by cst_create_date desc) as flag_last
    from bronze.crm_cust_info
    WHERE cst_id is not NULL
) t  where flag_last=1 --Select the most recent record per customer
set @end_time = getdate();
print'>> Load Duration:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) +' second ';


---------LOADING SILVER CRM_PRD_INFO---
SET @start_time =getdate()
PRINT'>> Truncating Table:silver.crm_prd_info';
truncate table silver.crm_prd_info;
print '>> Inserting Data Into: silver.crm_prd_info'
INSERT into silver.crm_prd_info(
    prd_id,
    prd_key,
    cat_id,
    prd_cost,
    prd_line,
    prd_nm,
    prd_start_dt,
    prd_end_dt)
SELECT
    prd_id,
    Replace(SUBSTRING(prd_key, 1,5),'-','_') as cat_id, -- Extract category ID
    SUBSTRING(prd_key, 7,Len(prd_key)) as prd_key,-- Extract product key
    coalesce(prd_cost,0) as prd_cost,
    case when upper (trim(prd_line)) ='M' then 'Mountain'
        when upper( trim(prd_line)) ='R' then 'Road'
        when upper( trim(prd_line)) ='S' then 'other Sales'
        when upper(trim(prd_line)) ='T' then 'Touring'
        Else 'n/a'
    end as prd_line,-- Map product line codes to descriptive values
    prd_nm,
    prd_start_dt,
    DATEADD(day,-1,LEAD(prd_start_dt)OVER (Partition by prd_key order by prd_start_dt)) 
    As prd_end_dt_test -- Calculate end date as oneday before the next start date
From bronze.crm_prd_info;
set @end_time = getdate();
PRINT'>> Load Duration:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) +' second ';




---Loading SILVER.CRM.SALES_DETAILS--------
set @start_time=getdate()
PRINT '>> Truncating Table:silver.crm_sales_details'
Truncate table  silver.crm_sales_details
print '>> Insert table: silver.crm_sales_details:'
Insert into silver.crm_sales_details(
    sls_order_num,
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
sls_order_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or len (sls_order_dt) != 8 then null
     else CAST (CAST(sls_order_dt as varchar) as date)
end as sls_order_dt,
case when sls_ship_dt = 0 or len (sls_ship_dt) != 8 then null
     else CAST (CAST(sls_ship_dt as varchar) as date)
end as sls_ship_dt,
case when sls_due_dt = 0 or len (sls_due_dt) != 8 then null
     else CAST (CAST(sls_due_dt as varchar) as date)
end as sls_due_dt,
 CASE when sls_sales is null or sls_sales <=0 or sls_sales!= sls_quantity * ABS(sls_price)
        THEN sls_quantity *ABS(sls_price)
    else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price <= 0
    then sls_sales/ nullif(sls_quantity,0)
    else sls_price
end as sls_price
from bronze.crm_sales_details
set @end_time = getdate();
PRINT '>> Load Duration:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) +' second ';



--- Clean and Load ERP_CUST_AZ12-----
set @start_time =Getdate()
Print '>> truncating tables:silver.erp_cust_az12'
truncate table silver.erp_cust_az12
print '>> Insert Table:silver.erp_cust_az12'
insert into silver.erp_cust_az12(
    cid,
    bdate,
    gen
)
SELECT
CASE WHEN cid like 'nas%' then SUBSTRING(cid, 4, len(cid))--- Remove 'NAS' prefix if present
    else cid
end as cid,
Case when bdate > GETDATE() then null
    else bdate
end as bdate,-- Set future birthdates to NULL
case when UPPER(trim(gen)) in ('F','Female') then 'Female'
    when UPPER(trim(gen)) in ('M','Male') then 'Male'
    else 'n/a'
end as gen -- Normalize gender values and handle unknown cases
from bronze.erp_cust_az12
set @end_time =getdate();
PRINT '>>Load Duration:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) +' second ';


--- Clean and Load ERP_LOC_A101--------------
print '>> Truncating tables :silver.erp_loc_a101'
TRUNCATE TABLE silver.erp_loc_a101
print '>> Truncating table : silver.erp_loc_a101'
INSERT Into silver.erp_loc_a101(cid,cntry)
SELECT 
replace (cid,'-','') cid,
case when trim(cntry) ='DE' then 'Germany'
    when trim(cntry) in ('US','USA') then 'United States'
    when trim (cntry) = '' or trim (cntry) is null then 'N/A'
else trim( cntry) 
end as cntry
FROM bronze.erp_loc_a101;
set @end_time = GETDATE();
PRINT '>>Load Duration:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) +' second ';


--- Clean and Load  ERP_PX_CAT_G1V2----------
set @start_time =GETDATE();
Print '>> Truncating Tables:silver.erp_px_cat_g1v2'
Truncate table silver.erp_px_cat_g1v2
Print '>> Inserting Table:silver.erp_px_cat_g1v2 '
insert into silver.erp_px_cat_g1v2(id,
cat,
subcat, 
maintenance)
SELECT 
id,
cat,
subcat, 
maintenance
from bronze.erp_px_cat_g1v2;
Set @end_time =getdate();
Print '>> Load Duration:' + CAST(Datediff(second,@start_time,@end_time) As NVARCHAR) + 'seconds';

set @batch_end_time = getdate();
print 'Loading Silver Layer is Completed';
Print '  - Total Load Duration:' + Cast(Datediff(SECOND,@batch_start_time,@batch_end_time)) As NVARCHAR) + 'second';

end TRY
begin catch
    print '================================================'
    print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
    PRINT 'Error Message' + ERROR_MESSAGE();
    print 'Error Message' +CAST (error_Number() as nvarchar);
    print 'Error Message' + CAST(error_state() as NVARCHAR) ;
end catch

end 
