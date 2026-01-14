Quality Checks
\---Script Purpose-----\
--This script performs various quality chacks for data consistency, accuracy, and standardization across the 'silver' layer.It includes checks for:
        --Null or duplicate primary keys
        -- Unwwanted spaces in string fields.
        -- Data standardization and consistency.
        -- Invalid date ranges and orders.
        -- Data consistency between related fields.


--Checking 'silver.crm_cust_info'
-- Check for Null or duplicate in Primary Key
--- Expectation: No Result
SELECT
cst_id,
Count(*)
from bronze.crm_cust_info
group by cst_id 
having count(*)>1 or cst_id is null

-- Check for unwanted Spaces
-- Expectation: No results
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)

select cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_firstname)
--Data Standardization & Consistency
SELECT DISTINCT cst_gndr
from bronze.crm_cust_info
SELECT DISTINCT cst_marital_status
from bronze.crm_cust_info
EXEC sp_rename 
    'bronze.crm_cust_info.cst_marital_name',
    'cst_marital_status',
    'COLUMN';

EXEC sp_rename 
    'silver.crm_cust_info.cst_material_status',
    'cst_marital_status',


-- ====================================================================
  ---Checking 'silver.crm_prd_info----
  
-- ====================================================================
  --- Check for Null or Duplicates in Primary Key
--- Expection: No Result
Select 
prd_id,
count(*)
from Silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is NULL
-- Check for unwated Spaces
--- Expetatio: No Results
SELECT prd_nm
from bronze.crm_prd_info
where prd_nm!= trim(prd_nm)

SELECT prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is NuLl
--Data Standardization & Consistency
select distinct prd_line
from bronze.crm_prd_info
-- Check for Invalid Date Orders
SELECT 
CounT(*)
from bronze.crm_prd_info

SELECT*
from silver.crm_prd_info
where prd_end_dt<prd_start_dt

WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') not in
    (select distinct id from bronze.erp_px_cat_g1v2)

SELECT
    prd_id,
    prd_key,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt) over (PARTITION  by prd_key order by prd_start_dt) - 1 as prd_end_dt_test
 from bronze.crm_prd_info
 where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509')
    'COLUMN';

-- ====================================================================
--- Checking 'silver.crm_sales_details--------

-- ====================================================================
--- Check for Invalid Date Orders
SELECT
*
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt
or sls_order_dt > sls_due_dt
--- Check Data Consistency : Between Sales, Quantity, and Price
----> Sales = Quantity * price
----> Values must be NULL, zero or negative
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is NULL or sls_price is  NULL
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
ORDER by sls_sales,
sls_quantity,
sls_price
--- 3 Rule
 ---+ If Sales is negative, zero, or null, derive it using Quantity and Price
 ---+ IF Price is zero or null, calculate it using Sales and Quantity
 ---+ If Price is negative, convert it to a positive value
 --- Test tranform value
 select DISTINCT
 sls_sales AS old_sls_sales,
 sls_quantity,
 sls_price as old_sls_price,
 CASE when sls_sales is null or sls_sales <=0 or sls_sales!= sls_quantity * ABS(sls_price)
        THEN sls_quantity *ABS(sls_price)
    else sls_sales
end as sls_sales,
case when sls_price is null or sls_price <= 0
    then sls_sales/ nullif(sls_quantity,0)
    else sls_price
end as sls_price
from  bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is NULL or sls_price is  NULL
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
ORDER by sls_sales,
sls_quantity,
sls_price
-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
--Indentify Out of Range Dates
select distinct 
bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE()
-- Data Standardization & Consistency
SELECT distinct 
gen,
case when UPPER(trim(gen)) in ('F','Female') then 'Female'
    when UPPER(trim(gen)) in ('M','Male') then 'Male'
    else 'n/a'
end as gen
from bronze.erp_cust_az12
-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;
-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;
