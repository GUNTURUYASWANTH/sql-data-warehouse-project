use datawarehouse

show tables

drop DATABASE datawarehouse

create DATABASE datawarehouse

select * from crm_cust_info

LOAD DATA LOCAL INFILE 'C:/Sql Database Practice/Data Warehouse/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv'
INTO TABLE crm_cust_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(cst_id, cst_key, cst_firstname, cst_lastname, cst_mariatal_staus, cst_gndr, cst_create_date);


select * from crm_cust_info;

LOAD DATA LOCAL INFILE 'C:/Sql Database Practice/Data Warehouse/sql-data-warehouse-project-main/datasets/source_crm/prd_info.csv'
INTO table prd_info
FIELDS TERMINATED by ','
ENCLOSED BY '"'
LINES TERMINATED by '\n'
IGNORE  1 ROWS
(prd_id , prd_key , prd_nm , prd_cost , prd_line, prd_start_dt, prd_end_dt)

select * from prd_info

LOAD DATA LOCAL INFILE 'C:/Sql Database Practice/Data Warehouse/sql-data-warehouse-project-main/datasets/source_crm/sales_details.csv'
into table sales_details
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt,sls_sales, sls_quantity,sls_price)

select * FROM sales_details

LOAD DATA LOCAL INFILE 'C:/Sql Database Practice/Data Warehouse/sql-data-warehouse-project-main/datasets/source_erp/cust_az12.csv'
into table erp_cust_az12
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(cid, bdate, gen)

select * from erp_cust_az12

LOAD DATA LOCAL INFILE 'C:/Sql Database Practice/Data Warehouse/sql-data-warehouse-project-main/datasets/source_erp/loc_a101.csv'
into table loc_a101
fields TERMINATED BY ','
ENCLOSED BY '"'
lines TERMINATED by '\n'
IGNORE 1 ROWS
(cid, cntry)

select * FROM loc_a101

load data local INFILE 'C:/Sql Database Practice/Data Warehouse/sql-data-warehouse-project-main/datasets/source_erp/px_cat_g1v2.csv'
into table px_cat_g1v2
FIELDS TERMINATED by ','
ENCLOSED BY '"'
lines TERMINATED BY '\n'
IGNORE 1 ROWS
(id,cat,subcat,maintenance)

select * from px_cat_g1v2


select count(*) from crm_cust_info


-- Lets create a Stored procedure for the above script
-- Its not working as in stored procedures we cant load data
use datawarehouse;


DELIMITER //
create Procedure Load_DataWarehouse()
begin
    -- Load CRM Customer Info
    LOAD DATA LOCAL INFILE 'C:/Sql Database Practice/Data Warehouse/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv'
    INTO TABLE crm_cust_info
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (cst_id, cst_key, cst_firstname, cst_lastname, cst_mariatal_staus, cst_gndr, cst_create_date);

    -- Load Product Info
    LOAD DATA LOCAL INFILE 'C:/Sql Database Practice/Data Warehouse/sql-data-warehouse-project-main/datasets/source_crm/prd_info.csv'
    INTO TABLE prd_info
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt);

    -- Load Sales Details
    LOAD DATA LOCAL INFILE 'C:/Sql Database Practice/Data Warehouse/sql-data-warehouse-project-main/datasets/source_crm/sales_details.csv'
    INTO TABLE sales_details
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price);

    -- Load ERP Customer
    LOAD DATA LOCAL INFILE 'C:/Sql Database Practice/Data Warehouse/sql-data-warehouse-project-main/datasets/source_erp/cust_az12.csv'
    INTO TABLE erp_cust_az12
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (cid, bdate, gen);

    -- Load Location
    LOAD DATA LOCAL INFILE 'C:/Sql Database Practice/Data Warehouse/sql-data-warehouse-project-main/datasets/source_erp/loc_a101.csv'
    INTO TABLE loc_a101
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (cid, cntry);

    -- Load Product Category
    LOAD DATA LOCAL INFILE 'C:/Sql Database Practice/Data Warehouse/sql-data-warehouse-project-main/datasets/source_erp/px_cat_g1v2.csv'
    INTO TABLE px_cat_g1v2
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (id, cat, subcat, maintenance);

    -- Verification query
    SELECT COUNT(*) AS total_customers FROM crm_cust_info;

END
DELIMITER;



USE datawarehouse;

DELIMITER //

CREATE PROCEDURE load_bronze()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;

    -- Error handler (like CATCH)
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SELECT '==========================================' AS msg;
        SELECT 'ERROR OCCURRED DURING LOADING BRONZE LAYER' AS msg;
        ROLLBACK;
    END;

    -- Start batch
    SET batch_start_time = NOW();
    SELECT '================================================' AS msg;
    SELECT 'Loading Bronze Layer' AS msg;
    SELECT '================================================' AS msg;

    -- Example: CRM Customer Info
    SET start_time = NOW();
    SELECT '>> Truncating Table: crm_cust_info' AS msg;
    TRUNCATE TABLE crm_cust_info;
    -- (LOAD DATA must be run outside procedure)
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;

    -- Example: CRM Product Info
    SET start_time = NOW();
    SELECT '>> Truncating Table: prd_info' AS msg;
    TRUNCATE TABLE prd_info;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;

    -- Example: Sales Details
    SET start_time = NOW();
    SELECT '>> Truncating Table: sales_details' AS msg;
    TRUNCATE TABLE sales_details;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;

    -- ERP Tables
    SET start_time = NOW();
    SELECT '>> Truncating Table: erp_loc_a101' AS msg;
    TRUNCATE TABLE erp_loc_a101;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;

    SET start_time = NOW();
    SELECT '>> Truncating Table: erp_cust_az12' AS msg;
    TRUNCATE TABLE erp_cust_az12;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;

    SET start_time = NOW();
    SELECT '>> Truncating Table: px_cat_g1v2' AS msg;
    TRUNCATE TABLE px_cat_g1v2;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;

    -- End batch
    SET batch_end_time = NOW();
    SELECT '==========================================' AS msg;
    SELECT 'Loading Bronze Layer is Completed' AS msg;
    SELECT CONCAT('   - Total Load Duration: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds') AS msg;
    SELECT '==========================================' AS msg;

END //
DELIMITER ;

call load_bronze;


ROLLBACK;
