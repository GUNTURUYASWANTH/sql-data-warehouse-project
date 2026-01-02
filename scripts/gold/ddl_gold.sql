
--  Here we only makes VIEWS for data presentation
--  But here only the related data will be loaded to views


use silver

CREATE SCHEMA gold

SELECT * from silver.crm_cust_info

SHOW TABLEs

SELECT * FROM silver.erp_loc_a101


SELECT ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_mariatal_staus,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
        FROM silver.crm_cust_info as ci
        LEFT JOIN silver.erp_cust_az12 as ca
        ON  ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 as la
        ON  ci.cst_key = la.cid


-- We will check for duplicates in the above query

SELECT cst_id, COUNT(*) FROM                            
        (SELECT ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_mariatal_staus,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
        FROM silver.crm_cust_info as ci
        LEFT JOIN silver.erp_cust_az12 as ca
        ON  ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 as la
        ON  ci.cst_key = la.cid) t
        GROUP BY cst_id
        HAVING COUNT(*) > 1

-- As they are no duplicates it showing nothing

SELECT ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_mariatal_staus,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
        FROM silver.crm_cust_info as ci
        LEFT JOIN silver.erp_cust_az12 as ca
        ON  ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 as la
        ON  ci.cst_key = la.cid

-- If we observe this query there a column called cst_gender and gen which showing 
-- the geneder, now the task is to make the gen as same as cst_gender
-- And giving preference to cst_gndr

SELECT DISTINCT
        ci.cst_gndr,
        ca.gen,
        CASE 
                WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  
                ELSE  COALESCE(ca.gen,'n/a')
        END new_gen
        FROM silver.crm_cust_info as ci
        LEFT JOIN silver.erp_cust_az12 as ca
        ON  ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 as la
        ON  ci.cst_key = la.cid

-- This new_gen gives better gender valuations 

SELECT ci.cst_id as customer_id,
        ci.cst_key as customer_number,
        ci.cst_firstname as first_name,
        ci.cst_lastname as last_name,
        la.cntry as country,
        ci.cst_mariatal_staus as mariatal_status,
        CASE 
                WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  
                ELSE  COALESCE(ca.gen,'n/a')
        END gender,
        ca.bdate as birthdate,
        ci.cst_create_date as create_date
        FROM silver.crm_cust_info as ci
        LEFT JOIN silver.erp_cust_az12 as ca
        ON  ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 as la
        ON  ci.cst_key = la.cid


-- Surragate key 
-- System-generated unique identifier assigned to each record in a table
-- It can be done by based using window functions(Row_number)


SELECT  ROW_NUMBER() over(ORDER BY cst_id) as customer_key,
        ci.cst_id as customer_id,
        ci.cst_key as customer_number,
        ci.cst_firstname as first_name,
        ci.cst_lastname as last_name,
        la.cntry as country,
        ci.cst_mariatal_staus as mariatal_status,
        CASE 
                WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  
                ELSE  COALESCE(ca.gen,'n/a')
        END gender,
        ca.bdate as birthdate,
        ci.cst_create_date as create_date
        FROM silver.crm_cust_info as ci
        LEFT JOIN silver.erp_cust_az12 as ca
        ON  ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 as la
        ON  ci.cst_key = la.cid

-- We will create a view

CREATE VIEW gold.dim_customers as 
SELECT ci.cst_id as customer_id,
        ci.cst_key as customer_number,
        ci.cst_firstname as first_name,
        ci.cst_lastname as last_name,
        la.cntry as country,
        ci.cst_mariatal_staus as mariatal_status,
        CASE 
                WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  
                ELSE  COALESCE(ca.gen,'n/a')
        END gender,
        ca.bdate as birthdate,
        ci.cst_create_date as create_date
        FROM silver.crm_cust_info as ci
        LEFT JOIN silver.erp_cust_az12 as ca
        ON  ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 as la
        ON  ci.cst_key = la.cid

--- Checking for the invalid data in gold.dim_customers
SELECT DISTINCT gender from gold.dim_customers

-------------------------------------------------------------

-- Now we will create a view for the second object

SELECT * FROM silver.prd_info

SELECT * FROM silver.erp_px_cat_g1v2

SELECT pn.prd_id,
        pn.cat_id,
        pn.prd_key,
        pn.prd_nm,
        pn.prd_cost,
        pn.prd_line,
        pn.prd_start_dt,
        pc.cat,pc.subcat,
        pc.maintenance
        FROM silver.prd_info pn
        LEFT JOIN silver.erp_px_cat_g1v2 pc
        on pn.cat_id = pc.id

-- Now we need to check for duplicates in the above query

SELECT prd_id, COUNT(*) FROM (
        SELECT pn.prd_id,
        pn.cat_id,
        pn.prd_key,
        pn.prd_nm,
        pn.prd_cost,
        pn.prd_line,
        pn.prd_start_dt,
        pc.cat,pc.subcat,
        pc.maintenance
        FROM silver.prd_info pn
        LEFT JOIN silver.erp_px_cat_g1v2 pc
        on pn.cat_id = pc.id
) t GROUP BY prd_id
HAVING COUNT(*)>1

-- We dont have duplicates
SELECT pn.prd_id as product_id,
        pn.prd_key as product_number,
        pn.prd_nm as product_name,
        pn.cat_id as category_id,
        pc.cat as category,
        pc.subcat as sub_category,  
        pc.maintenance,      
        pn.prd_cost as cost,
        pn.prd_line as product_line,
        pn.prd_start_dt as start_date
        FROM silver.prd_info pn
        LEFT JOIN silver.erp_px_cat_g1v2 pc
        on pn.cat_id = pc.id


-- Surragate key 
-- System-generated unique identifier assigned to each record in a table
-- It can be done by based using window functions(Row_number)

SELECT ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) as product_key,
        pn.prd_id as product_id,
        pn.prd_key as product_number,
        pn.prd_nm as product_name,
        pn.cat_id as category_id,
        pc.cat as category,
        pc.subcat as sub_category,  
        pc.maintenance,      
        pn.prd_cost as cost,
        pn.prd_line as product_line,
        pn.prd_start_dt as start_date
        FROM silver.prd_info pn
        LEFT JOIN silver.erp_px_cat_g1v2 pc
        on pn.cat_id = pc.id

-- Now we will create an view

CREATE View gold.dim_products as 
SELECT ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) as product_key,
        pn.prd_id as product_id,
        pn.prd_key as product_number,
        pn.prd_nm as product_name,
        pn.cat_id as category_id,
        pc.cat as category,
        pc.subcat as sub_category,  
        pc.maintenance,      
        pn.prd_cost as cost,
        pn.prd_line as product_line,
        pn.prd_start_dt as start_date
        FROM silver.prd_info pn
        LEFT JOIN silver.erp_px_cat_g1v2 pc
        on pn.cat_id = pc.id

SELECT * from gold.dim_products

-------------------------------------------------------------------------------

-- As the erp tables are over now we need to do crm_sales_info

SELECT * FROM silver.sales_details

SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
        FROM silver.sales_details

-- Fact tables = numeric measures + foreign keys.
-- Dimension tables = descriptive attributes.

SELECT 
        sd.sls_ord_num as order_number,
        pr.product_key,
        cu.customer_id,
        sd.sls_order_dt as order_date,
        sd.sls_ship_dt as shipping_date,
        sd.sls_due_dt as due_date,
        sd.sls_sales as sales_amount,
        sd.sls_quantity as quantity,
        sd.sls_price
        FROM silver.sales_details sd   
        LEFT JOIN gold.dim_products pr
        on sd.sls_prd_key = pr.product_number
        LEFT JOIN gold.dim_customers cu       
        ON sd.sls_cust_id = cu.customer_id

-- Now we will create an view

create View gold.fact_sales AS
        SELECT 
        sd.sls_ord_num as order_number,
        pr.product_key,
        cu.customer_id,
        sd.sls_order_dt as order_date,
        sd.sls_ship_dt as shipping_date,
        sd.sls_due_dt as due_date,
        sd.sls_sales as sales_amount,
        sd.sls_quantity as quantity,
        sd.sls_price
        FROM silver.sales_details sd   
        LEFT JOIN gold.dim_products pr
        on sd.sls_prd_key = pr.product_number
        LEFT JOIN gold.dim_customers cu       
        ON sd.sls_cust_id = cu.customer_id

--  Now we check for the quality 
