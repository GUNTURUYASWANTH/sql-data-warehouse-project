create database DataWarehouse;


-- Switch to that database
USE DataWarehouse;

-- Create schema
CREATE SCHEMA bronze;

create table if not exists crm_cust_info(
    cst_id int,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_mariatal_staus VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
)

create table if not exists prd_info(
    prd_id int,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost int,
    prd_line VARCHAR(50),
    prd_start_dt datetime,
    prd_end_dt DATETIME
)

create table if not exists sales_details(
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id int,
    sls_order_dt int,
    sls_ship_dt int,
    sls_due_dt int,
    sls_sales int,
    sls_quantity int,
    sls_price int
)

create table if not exists erp_cust_az12(
    cid VARCHAR(50),
    bdate date,
    gen VARCHAR(50)
)

create table if not exists loc_a101(
    cid VARCHAR(50),
    cntry VARCHAR(50)
)

create table if not exists px_cat_g1v2(
    id int,
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
)
