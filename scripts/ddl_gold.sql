---DDL scripts: CREATE GOLD TABLE
PURPOSE:
---  This script create views for the Gold layer in Data warehouse.
--- The gold layer represent the final dimension and fact table
--- Each view perform transformation and combines data from the silver to produce a clean,enriched, and business- ready dataset

--- CREATE DIMENSION:: gold.dim_customers-------

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
create view  gold.dim_customers as 
   ( SELECT
    row_number() over (order by cst_id) as customer_key,
    ci.cst_id As customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
        la.cntry as country,
    ci.cst_marital_status as marital_status,
    case when ci.cst_gndr= 'n/a' then ci.cst_gndr --CrM is the Master for gender Info
        else COALESCE(ca.gen,'n/a')
    end as gender,
    ci.cst_create_date as create_date,
    ca.bdate as birthdate

    from silver.crm_cust_info ci 
    LEFT JOIN silver.erp_cust_az12 ca 
    on  ci.cst_key =ca.cid
    LEFT JOIN silver.erp_loc_a101 la 
    ON ci.cst_key = la.cid);
GO
--- CREATE DIMENSION : gold.dim_product---
  
if OBJECT_ID('gold.dim_products','V') is not  NULL
    drop VIEW gold.dim_products;
go
create view gold.dim_products as 
SELECT 
row_number() over(order by pn.prd_start_dt, pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.cat_id as category_id,
pn.prd_nm as product_name,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn 
left join silver.erp_px_cat_g1v2 pc 
on pn.cat_id = pc.id
where prd_end_dt is null -- FIlter out all historical data
GO

--CREATE FACT SALES---

  
if OBJECT_ID('gold.fact_sales','V') IS NOT NULL
    DROP VIEW gold.fact_sales;
go  
create VIEW  gold.fact_sales as 
SELECT
sd.sls_order_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as ship_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quamity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr  
on sd.sls_prd_key = pr.product_number
left  join gold.dim_customers cu  
on sd.sls_cust_id =cu.customer_id
GO
