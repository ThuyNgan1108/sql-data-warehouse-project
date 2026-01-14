
---DDL Script: Create Silver Tables
---- Script Purpose:
----This script create tables in the ' silver' schema, dropping existing tables if they already exist
------Run this script to re-define the ddl structure of 'bronze' Tables

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO
CREATE TABLE silver.crm_cust_info (
    cst_id int,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname nvarchar(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE,
);
GO
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info
CREATE table silver.crm_prd_info(
    prd_id int,
    prd_key nvarchar(50),
    prd_nm nvarchar(50),
    cat_id nvarchar(50),
    prd_cost int,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt Date,
    dwh_create_date DATETIME2 default getdate()
);
GO
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details
create table silver.crm_sales_details(
    sls_order_num NVARCHAR(50),
    sls_prd_key nvarchar(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales int,
    sls_quantity int,
    sls_price int,
    dwh_create_date DATETIME2 default getdate()
);
GO
if OBJECT_ID('silver.erp_loc_a101','U') is NOT NULL
    drop table silver.erp_loc_a101;
create table silver.erp_loc_a101(
    cid NVARCHAR(50),
    cntry nvarchar(50),
    dwh_create_date DATETIME2 default getdate()
);
GO
if OBJECT_ID('silver.erp_cust_az12','U') is NOT NULL
    drop table silver.erp_cust_az12;
create table silver.erp_cust_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50),
    dwh_create_date DATETIME2 default getdate()
);
GO
IF OBJECT_ID('silver.erp_px_cat_g1v2','U') is NOT NULL
    drop table silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat nvarchar(50),
    subcat nvarchar(50),
    maintenance nvarchar(50),
    dwh_create_date DATETIME2 default getdate()
);
GO
