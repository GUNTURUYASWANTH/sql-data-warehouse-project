
-- Here we use bronze level data we check for duplicates, null values 

-- what have will we do in this query
-- Remove unwanted spaces , Data Normalization , Handling Missing Values, Removing Duplicates


use datawarehouse

select * from datawarehouse.crm_cust_info

-- Check for duplicates
select cst_id, count(*) from datawarehouse.crm_cust_info GROUP BY cst_id having COUNT(*)>1 or cst_id is NULL

-- This gives the duplicate values or null values by same cst_id
select * from datawarehouse.crm_cust_info where cst_id = 29433

-- By giving them the priority like flag we can get the recent date row 
-- Like getting the recent create_date have the prority

select * ,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_Last
    from datawarehouse.crm_cust_info 


-- Lets seperate the duplicate rows 
select * from (select * ,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_Last
    from datawarehouse.crm_cust_info ) as t where flag_last != 1;


-- Cleaned data without the duplicates 

select * from (select * ,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_Last
    from datawarehouse.crm_cust_info ) as t where flag_last = 1;



-- Checking for unwanted spaces in table

-- if orginal name is not equal to trim name then the name has spaces in it
select cst_firstname from datawarehouse.crm_cust_info where cst_firstname != TRIM(cst_firstname)

select cst_firstname from datawarehouse.crm_cust_info where cst_lastname != TRIM(cst_lastname)


-- Cleaned data without duplicates and white spaces in names

select cst_id, cst_key,
    TRIM(cst_firstname) as cst_firstname, 
    TRIM(cst_lastname) as cst_lastname, 
    cst_mariatal_staus, cst_gndr, cst_create_date from 
    (select * ,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_Last
    from datawarehouse.crm_cust_info ) as t where flag_last = 1;


-- Making the marital_status, cst_gndr for better understanding
-- Data Standardization & consistency

select DISTINCT cst_mariatal_staus FROM datawarehouse.crm_cust_info;
-- As this query indicates three distinct words M, S, NULL

select DISTINCT cst_gndr from datawarehouse.crm_cust_info
-- As this query indicates three distinct words M, F, NULL

-- We will make them better data standardization using case statement

select cst_id, cst_key,
    TRIM(cst_firstname) as cst_firstname, 
    TRIM(cst_lastname) as cst_lastname, 
    CASE 
        WHEN cst_mariatal_staus = 'M' THEN 'Married'
        WHEN cst_mariatal_staus = 'S' THEN 'Single'  
        ELSE  'n/a'
    END cst_mariatal_staus,
    CASE 
        WHEN cst_gndr = 'M' THEN 'Male'
        WHEN cst_gndr = 'F' THEN 'Female'  
        ELSE  'n/a'
    END cst_gndr, 
    cst_create_date from 
    (select * ,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_Last
    from datawarehouse.crm_cust_info ) as t where flag_last = 1;

-- In some cases the word can be will be in lower case to sovle that we need to do uppercase of the name 
-- And also remove those spaces we will use TRIM() function

select cst_id, cst_key,
    TRIM(cst_firstname) as cst_firstname, 
    TRIM(cst_lastname) as cst_lastname, 
    CASE 
        WHEN UPPER(TRIM(cst_mariatal_staus)) = 'M' THEN 'Married'
        WHEN UPPER(TRIM(cst_mariatal_staus)) = 'S' THEN 'Single'  
        ELSE  'n/a'
    END cst_mariatal_staus,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'  
        ELSE  'n/a'
    END cst_gndr, 
    cst_create_date from 
    (select * ,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_Last
    from datawarehouse.crm_cust_info ) as t where flag_last = 1;

use silver

select * from crm_cust_info

-- Inserting the cleaned data into silver.crm_cust_info
insert into silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_mariatal_staus,
        cst_gndr,
        cst_create_date
        )
    select cst_id, cst_key,
    TRIM(cst_firstname) as cst_firstname, 
    TRIM(cst_lastname) as cst_lastname, 
    CASE 
        WHEN UPPER(TRIM(cst_mariatal_staus)) = 'M' THEN 'Married'
        WHEN UPPER(TRIM(cst_mariatal_staus)) = 'S' THEN 'Single'  
        ELSE  'n/a'
    END cst_mariatal_staus,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'  
        ELSE  'n/a'
    END cst_gndr, 
    cst_create_date from 
    (select * ,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_Last
    from datawarehouse.crm_cust_info ) as t where flag_last = 1;

 
select * from silver.crm_cust_info

-- To cross check this we can do the before sql queries

select * from (select * ,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_Last
    from silver.crm_cust_info ) as t where flag_last !=1;
-- As this query show empty data which means the data in silver.crm_cust_info is cleaned


-- ====================================================================================

-- Lets do the same for  prd_info

-- For the table prd_info 

select * from datawarehouse.prd_info

select prd_id , 
    prd_key,
-- In the prd_key the first 5 characters are for id in erp_cust_az12 talbe
    SUBSTRING(prd_key,1,5) as cat_id,
    prd_nm,
    prd_cost ,
    prd_line,
    prd_start_dt,
    prd_end_dt
    from datawarehouse.prd_info


-- The id from the table erp_cust_az12 are 


-- On the loc_a101 the data is cid which is subsequent to erp_cust_az12
select DISTINCT(id) from datawarehouse.px_cat_g1v2

-- As these id look like as AC_BR where underscore is there to get the id from 
-- We need to replace AC-BR to AC_BR by replace() function

select prd_id , 
    prd_key,
-- In the prd_key the first 5 characters are for id in erp_cust_az12 table
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
    prd_nm,
    prd_cost ,
    prd_line,
    prd_start_dt,
    prd_end_dt
    from datawarehouse.prd_info

-- Filtering out the unmatched data from the table px_cat_g1v2 with prd_info

select prd_id , 
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
    prd_nm,
    prd_cost ,
    prd_line,
    prd_start_dt,
    prd_end_dt
    from datawarehouse.prd_info
    WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN (select DISTINCT(id) from datawarehouse.px_cat_g1v2)

-- We need to get the remaining substring which is the prd_key
select prd_id , 
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
    SUBSTRING(prd_key,7,LENGTH(prd_key)) as prd_key,
    prd_nm,
    prd_cost ,
    prd_line,
    prd_start_dt,
    prd_end_dt
    from datawarehouse.prd_info
    
    --WHERE prd_key in (select DISTINCT(sls_prd_key) from datawarehouse.sales_details)

-- These prd_key which comes after substring can join to the sales_details
select DISTINCT(sls_prd_key) from datawarehouse.sales_details

select * from datawarehouse.prd_info

-- Check for unwanted spaces
select prd_nm from datawarehouse.prd_info
    where prd_nm != TRIM(prd_nm)

-- Check quality of numbers for NULLS or Negative Values
select prd_cost from datawarehouse.prd_info WHERE prd_cost is NULL or prd_cost < 0
-- If null present we can replace it with 0 by COALESCE and ifnull FUNCTION

select prd_id , 
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
    SUBSTRING(prd_key,7,LENGTH(prd_key)) as prd_key,
    prd_nm,
    IFNULL(prd_cost,0) as prd_cost ,
    prd_line,
    prd_start_dt,
    prd_end_dt
    from datawarehouse.prd_info

-- Making the prd_line into data normalization

select DISTINCT prd_line FROM datawarehouse.prd_info
-- Rename them

select prd_id , 
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
    SUBSTRING(prd_key,7,LENGTH(prd_key)) as prd_key,
    prd_nm,
    IFNULL(prd_cost,0) as prd_cost ,
    case 
        WHEN prd_line = 'R' THEN 'Road'
        WHEN prd_line = 'M' THEN 'Mountain'
        WHEN prd_line = 'S' THEN 'Other Sales'
        WHEN prd_line = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
    prd_start_dt,
    prd_end_dt
    from datawarehouse.prd_info


-- Lets get the data standardizatinon for prd_start_dt and prd_end_dt

-- Check for Invalid Order dates

select * from datawarehouse.prd_info WHERE prd_start_dt > prd_end_dt;

-- Solutions
-- 1 : The starting date not be NULL
-- 2 : End date = start date of next month - 1 by using the next start date we need to 
--     create the end date to the present row

-- Now we will create a simple query for the 2 nd solution on data
select prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
    from datawarehouse.prd_info
    where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509')

-- We use lead() and lag() functions to set the prd_end_dt from prd_start_dt 
-- At first we take some amount of data to analyse after that we apply to table because
-- to decrease the database transactions time
select prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt,
    DATE_SUB(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt),INTERVAL 1 DAY) as prd_end_dt_test
    from datawarehouse.prd_info
    where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509')

-- To decrease one day here we used Date_sub() function

select prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt,
    DATE_SUB(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt),INTERVAL 1 DAY) as prd_end_dt_test
    from datawarehouse.prd_info

----------------- Main query ---------------------



select prd_id , 
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
    SUBSTRING(prd_key,7,LENGTH(prd_key)) as prd_key,
    prd_nm,
    IFNULL(prd_cost,0) as prd_cost ,
    case 
        WHEN prd_line = 'R' THEN 'Road'
        WHEN prd_line = 'M' THEN 'Mountain'
        WHEN prd_line = 'S' THEN 'Other Sales'
        WHEN prd_line = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
    prd_start_dt,
    prd_end_dt,
    DATE_SUB(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt),INTERVAL 1 DAY) as prd_end_dt_test
    from datawarehouse.prd_info


-- We will cast it into date as the time is getting as 00:00:00 to set it.

select prd_id , 
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
    SUBSTRING(prd_key,7,LENGTH(prd_key)) as prd_key,
    prd_nm,
    IFNULL(prd_cost,0) as prd_cost ,
    case 
        WHEN prd_line = 'R' THEN 'Road'
        WHEN prd_line = 'M' THEN 'Mountain'
        WHEN prd_line = 'S' THEN 'Other Sales'
        WHEN prd_line = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
    CAST(prd_start_dt as DATE) as prd_start_dt,
    
    CAST(DATE_SUB(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt),INTERVAL 1 DAY) as DATE) as prd_end_dt
    from datawarehouse.prd_info

-- Our cleaned data is created we need to insert it into silver layer
-- But here the problem is in bronze layer the prd_start_dt, prd_end_dt are in datetime
-- we need to change it into DATE format
-- And also CatId is not present in silver layer so we delete the catid while creation.

show TABLES

DROP TABLE silver.prd_info

create table if not exists silver.prd_info(
    prd_id int,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost int,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
)

-- We need to insert data into the silver layer

TRUNCATE TABLE silver.prd_info;

INSERT INTO silver.prd_info (
    prd_id,
    prd_key,
    cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
    
)
select prd_id , 
    SUBSTRING(prd_key,7,LENGTH(prd_key)) as prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
    prd_nm,
    IFNULL(prd_cost,0) as prd_cost ,
    case 
        WHEN prd_line = 'R' THEN 'Road'
        WHEN prd_line = 'M' THEN 'Mountain'
        WHEN prd_line = 'S' THEN 'Other Sales'
        WHEN prd_line = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
    CAST(prd_start_dt as DATE) as prd_start_dt,
    
    CAST(DATE_SUB(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt),INTERVAL 1 DAY) as DATE) as prd_end_dt
    from datawarehouse.prd_info

SELECT * from silver.prd_info


------------------------- Checking for the Quality ---------------------------

-- Check for duplicates
select cst_id, count(*) from silver.crm_cust_info GROUP BY cst_id having COUNT(*)>1 or cst_id is NULL

-- Check for unwanted spaces
select prd_nm from silver.prd_info
    where prd_nm != TRIM(prd_nm)

-- Check quality of numbers for NULLS or Negative Values
select prd_cost from silver.prd_info WHERE prd_cost is NULL or prd_cost < 0
-- If null present we can replace it with 0 by COALESCE and ifnull FUNCTION

select prd_id , 
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
    SUBSTRING(prd_key,7,LENGTH(prd_key)) as prd_key,
    prd_nm,
    IFNULL(prd_cost,0) as prd_cost ,
    prd_line,
    prd_start_dt,
    prd_end_dt
    from silver.prd_info

SELECT * from silver.prd_info

-------------------- Sales_details table ------------------


select * from datawarehouse.sales_details

select sls_ord_num ,
    sls_prd_key ,
    sls_cust_id ,
    sls_order_dt ,
    sls_ship_dt ,
    sls_due_dt ,
    sls_sales ,
    sls_quantity ,
    sls_price 
    from datawarehouse.sales_details

-- We will check one by on column 

select sls_ord_num ,
    sls_prd_key ,
    sls_cust_id ,
    sls_order_dt ,
    sls_ship_dt ,
    sls_due_dt ,
    sls_sales ,
    sls_quantity ,
    sls_price 
    from datawarehouse.sales_details
    where sls_ord_num != TRIM(sls_ord_num)
-- No spaces in sls_ord_num COLUMN

-- Connecting sales_detials to prd_info by sls_prd_key = prd_key
select sls_ord_num ,
    sls_prd_key ,
    sls_cust_id ,
    sls_order_dt ,
    sls_ship_dt ,
    sls_due_dt ,
    sls_sales ,
    sls_quantity ,
    sls_price 
    from datawarehouse.sales_details
    where sls_prd_key NOT IN (select prd_key from silver.prd_info)

-- Connecting sales_details  with crm_cust_info on sls_cust_id = cst_id

select sls_ord_num ,
    sls_prd_key ,
    sls_cust_id ,
    sls_order_dt ,
    sls_ship_dt ,
    sls_due_dt ,
    sls_sales ,
    sls_quantity ,
    sls_price 
    from datawarehouse.sales_details
    WHERE sls_cust_id NOT IN (select cst_id from silver.crm_cust_info)

-- As both queries are not showing any values then the keys values are equal
-- There will be no issue on conecting the three tables

-- Check for Invalid Dates
SELECT NULLIF(sls_order_dt,0)
    from datawarehouse.sales_details
    WHERE sls_order_dt <= 0;

-- Date is in integer type so we need to change it
-- We will add another comparision like len() on sls_order_dt

SELECT NULLIF(sls_order_dt,0)
    from datawarehouse.sales_details
    WHERE sls_order_dt <= 0 OR LENGTH(sls_order_dt) !=8

-- Checking for outliers by validating the boundaries of the data range

SELECT NULLIF(sls_due_dt,0) sls_due_dt
    from datawarehouse.sales_details
    WHERE sls_due_dt <= 0 OR LENGTH(sls_due_dt) !=8
    OR
    sls_due_dt > 20500101
    OR
    sls_due_dt <19000101

-- As by this query there will be values like 32154 and 5489 they need to be reomved

SELECT * FROM datawarehouse.sales_details;

select sls_ord_num ,
    sls_prd_key ,
    sls_cust_id ,
        CASE 
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt)!=8 THEN NULL   
            ELSE  CAST(CAST(sls_order_dt AS CHAR) AS DATE)
        END AS sls_order_dt,
        CASE 
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL 
            ELSE  CAST(CAST(sls_ship_dt AS CHAR) AS DATE)
        END as sls_ship_dt ,
        CASE 
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL  
            ELSE CAST(CAST(sls_due_dt as CHAR) AS DATE)
        END AS sls_due_dt ,
    sls_sales ,
    sls_quantity ,
    sls_price 
    from datawarehouse.sales_details
    
-- In case statement in mysql it wont support varchar for casting it needed to be (CHAR)
-- For every date we need to check in before quering it in main query

-- We need to see the sls_order_dt must be less than the sls_ship_dt and sls_due_dt

SELECT * from datawarehouse.sales_details
    WHERE sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- Check Data Consistency: Between Sales, Quanity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, Zero , or negative

SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
    FROM datawarehouse.sales_details
    WHERE sls_sales != sls_quantity * sls_price
    or sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
    or sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0 
    ORDER BY sls_sales,sls_quantity, sls_price

-- Rules for the above query :
-- If Sales is Negative, Zero , Null derive it using Quanitty and Price
-- If price is zero or null, calculate it using Sales and Quantity
-- If price is negative convert it into positive value

SELECT 
    sls_sales as old_sls_sales,
    sls_quantity,
    sls_price as old_sls_price,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_sales = sls_quantity * ABS(sls_price) 
            ELSE sls_sales
        END as sls_sales,
        CASE
            WHEN sls_price IS NULL OR sls_price <=0 
                THEN ABS(sls_sales / IFNULL(sls_quantity,0)) -- To CHECK If any null quantity present in sls_quantity   
            ELSE  ABS(sls_price)
        END as sls_price
    FROM datawarehouse.sales_details
    WHERE sls_sales != sls_quantity * sls_price
    or sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
    or sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0 
    ORDER BY sls_sales, sls_quantity, sls_price

SELECT sls_sales, sls_price, sls_quantity from datawarehouse.sales_details


SELECT sls_price from datawarehouse.sales_details
WHERE sls_price IS NULL or sls_price <=0
-- As there are some zero values and negative VALUES

SELECT  
    CASE 
            WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_sales = sls_quantity * ABS(sls_price) 
            ELSE sls_sales
        END as sls_sales
    from datawarehouse.sales_details

SELECT 
    sls_sales as old_sls_sales,
    sls_quantity,
    sls_price as old_sls_price,
        CASE 
            WHEN sls_sales IS NULL 
                OR sls_quantity IS NULL
                OR sls_price IS NULL
                OR sls_sales <= 0
                or sls_sales <> sls_quantity * ABS(sls_price)
                or sls_quantity <=0
                OR sls_price <=0
             THEN sls_quantity * ABS(sls_price) 
            ELSE  sls_sales
        END as new_sls_sales,
        CASE 
            WHEN sls_price IS NULL 
                OR sls_price <= 0
            THEN  ABS(sls_sales / NULLIF(sls_quantity,0))
            ELSE  sls_price
        END as new_sls_price
    FROM datawarehouse.sales_details
    WHERE sls_sales != sls_quantity * sls_price
    or sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
    or sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0 
    ORDER BY sls_sales, sls_quantity, sls_price

-- Practise it ------


--- This query getting the crct result
SELECT 
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,
    CASE 
        WHEN sls_sales IS NULL 
          OR sls_quantity IS NULL
          OR sls_price IS NULL
          OR sls_sales <= 0
          OR sls_quantity <= 0
          OR sls_price <= 0
          OR sls_sales <> sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(
               CASE 
                 WHEN sls_price IS NULL OR sls_price <= 0
                 THEN CAST(ABS(sls_sales / NULLIF(sls_quantity, 0)) as SIGNED)  -- fallback price
                 ELSE CAST(sls_price as SIGNED)
               END
             )
        ELSE sls_sales
    END AS sls_sales,
        CASE 
            WHEN sls_price IS NULL 
                OR sls_price <= 0
            THEN  CAST(ABS(sls_sales / NULLIF(sls_quantity,0)) as SIGNED)
            ELSE  CAST(sls_price as SIGNED)
        END as sls_price
FROM datawarehouse.sales_details
WHERE sls_sales <> sls_quantity * ABS(sls_price)
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
   OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

------ Main query---------------
select sls_ord_num ,
    sls_prd_key ,
    sls_cust_id ,
        CASE 
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt)!=8 THEN NULL   
            ELSE  CAST(CAST(sls_order_dt AS CHAR) AS DATE)
        END AS sls_order_dt,
        CASE 
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL 
            ELSE  CAST(CAST(sls_ship_dt AS CHAR) AS DATE)
        END as sls_ship_dt ,
        CASE 
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL  
            ELSE CAST(CAST(sls_due_dt as CHAR) AS DATE)
        END AS sls_due_dt ,
        CASE 
            WHEN sls_sales IS NULL 
            OR sls_quantity IS NULL
            OR sls_price IS NULL
            OR sls_sales <= 0
            OR sls_quantity <= 0
            OR sls_price <= 0
            OR sls_sales <> sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(
                CASE 
                    WHEN sls_price IS NULL OR sls_price <= 0
                    THEN CAST(ABS(sls_sales / NULLIF(sls_quantity, 0)) as SIGNED)  -- fallback price
                    ELSE CAST(sls_price as SIGNED)
                END
                )
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
            CASE 
                WHEN sls_price IS NULL 
                    OR sls_price <= 0
                THEN  CAST(ABS(sls_sales / NULLIF(sls_quantity,0)) as SIGNED)
                ELSE  CAST(sls_price as SIGNED)
            END as sls_price
        from datawarehouse.sales_details
        WHERE sls_sales <> sls_quantity * ABS(sls_price)
        OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
        OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0


-------------- Silver layer DDL creation ---------------------------

-- We need to check it with the table of bronze layer to silver layer

-- Before while creation of table sls_order_dt, sls_ship_dt ,sls_due_dt  they are in integer varaible 
-- we changed them into date

use silver

DROP TABLE IF EXISTS silver.sales_details;
create table if not exists sales_details(
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id int,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales int,
    sls_quantity int,
    sls_price int,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
)


-- Inserting data into silver.sales_details table

INSERT INTO silver.sales_details(
    sls_ord_num ,
    sls_prd_key ,
    sls_cust_id ,
    sls_order_dt ,
    sls_ship_dt ,
    sls_due_dt ,
    sls_sales ,
    sls_quantity,
    sls_price
)
select sls_ord_num ,
    sls_prd_key ,
    sls_cust_id ,
        CASE 
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt)!=8 THEN NULL   
            ELSE  CAST(CAST(sls_order_dt AS CHAR) AS DATE)
        END AS sls_order_dt,
        CASE 
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL 
            ELSE  CAST(CAST(sls_ship_dt AS CHAR) AS DATE)
        END as sls_ship_dt ,
        CASE 
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL  
            ELSE CAST(CAST(sls_due_dt as CHAR) AS DATE)
        END AS sls_due_dt ,
        CASE 
            WHEN sls_sales IS NULL 
            OR sls_quantity IS NULL
            OR sls_price IS NULL
            OR sls_sales <= 0
            OR sls_quantity <= 0
            OR sls_price <= 0
            OR sls_sales <> sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(
                CASE 
                    WHEN sls_price IS NULL OR sls_price <= 0
                    THEN CAST(ABS(sls_sales / NULLIF(sls_quantity, 0)) as SIGNED)  -- fallback price
                    ELSE CAST(sls_price as SIGNED)
                END
                )
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
            CASE 
                WHEN sls_price IS NULL 
                    OR sls_price <= 0
                THEN  CAST(ABS(sls_sales / NULLIF(sls_quantity,0)) as SIGNED)
                ELSE  CAST(sls_price as SIGNED)
            END as sls_price
        from datawarehouse.sales_details
        WHERE sls_sales <> sls_quantity * ABS(sls_price)
        OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
        OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0

SELECT * FROM silver.sales_details



show TABLES

-------------------- For silver.erp_cust_az12 ---------------------

SELECT * FROM datawarehouse.erp_cust_az12

SELECT * from datawarehouse.crm_cust_info

-- As these both tables are connected 
-- We need to combine both tables by erp_cust_az12.cid = crm_cust_info.cst_key

SELECT 
        SUBSTR(cid,9,LENGTH(cid)) as cid,
        bdate,
        gen 
        FROM datawarehouse.erp_cust_az12

-- Now we need to check it for the invalid cid_key

SELECT 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4,LENGTH(cid))  
            ELSE  cid
        END as cid,
        bdate,
        gen 
        FROM datawarehouse.erp_cust_az12
        

-- Now we will check it with invalid cid_key

SELECT 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4,LENGTH(cid))  
            ELSE  cid
        END as cid,
        bdate,
        gen 
        FROM datawarehouse.erp_cust_az12
    WHERE 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4,LENGTH(cid))  
            ELSE  cid
        END  NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)


----- Now we will check for dates ----------

SELECT DISTINCT
        bdate
        FROM datawarehouse.erp_cust_az12
    WHERE bdate < '1924-01-01' OR bdate > CURRENT_DATE()

-- As some dates are exceeding the current date eg (9999-09-13) we need to make them into null

SELECT 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4,LENGTH(cid))  
            ELSE  cid
        END as cid,
        CASE 
            WHEN bdate > CURRENT_DATE() THEN NULL 
            ELSE  bdate
        END as bdate,
        gen 
        FROM datawarehouse.erp_cust_az12


-- Now we will check for the gender column
-- Data standardization & consistency

SELECT DISTINCT gen from datawarehouse.erp_cust_az12

SELECT DISTINCT
    gen AS raw_gen,
    CASE 
        WHEN gen IS NULL THEN 'n/a'
        WHEN LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(gen, '\r',''), '\n',''), '\t',''), CHAR(160), ''))) IN ('f','female') THEN 'Female'
        WHEN LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(gen, '\r',''), '\n',''), '\t',''), CHAR(160), ''))) IN ('m','male')   THEN 'Male'
        ELSE 'n/a'
    END AS normalized_gen
FROM datawarehouse.erp_cust_az12;


-- Here we used to remove Extra spaces , Hidden characters, Mixed case, Non-breaking spaces , Nulls
-- \r -> To return
-- \t -> To remove tabs
-- \n -> To remove newlines
-- char(160) -> Removes non-breaking spaces (common in imported data)

-- If we use without them then it returns always n/a as we mentioned above

-- Inserting into silver.erp_cust_az12

TRUNCATE TABLE silver.erp_cust_az12

INSERT INTO silver.erp_cust_az12
    (cid,
    bdate,
    gen)
    (SELECT 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4,LENGTH(cid))  
            ELSE  cid
        END as cid,
        CASE 
            WHEN bdate > CURRENT_DATE() THEN NULL 
            ELSE  bdate
        END as bdate,
        CASE 
            WHEN gen IS NULL THEN 'n/a'
            WHEN LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(gen, '\r',''), '\n',''), '\t',''), CHAR(160), ''))) IN ('f','female') THEN 'Female'
            WHEN LOWER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(gen, '\r',''), '\n',''), '\t',''), CHAR(160), ''))) IN ('m','male')   THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM datawarehouse.erp_cust_az12)

-- We will recheck it for invalid errors by follwing the rules where we used in

-- Now we need to check it for the invalid cid_key

SELECT 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4,LENGTH(cid))  
            ELSE  cid
        END as cid,
        bdate,
        gen 
        FROM silver.erp_cust_az12
        

-- Now we will check it with invalid cid_key

SELECT 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4,LENGTH(cid))  
            ELSE  cid
        END as cid,
        bdate,
        gen 
        FROM silver.erp_cust_az12
    WHERE 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4,LENGTH(cid))  
            ELSE  cid
        END  NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)


----- Now we will check for dates ----------

SELECT DISTINCT
        bdate
        FROM silver.erp_cust_az12
    WHERE bdate < '1924-01-01' OR bdate > CURRENT_DATE()

SELECT DISTINCT gen
    FROM silver.erp_cust_az12

SELECT * from silver.erp_cust_az12


-------------------------------------------------------------------------

-- Now we will check for erp_loc_a101 table

SELECT * FROM datawarehouse.erp_loc_a101

SELECT * from silver.crm_cust_info

--- In datawarehouse.erp_loc_a101 we need to remove '-' for to equal with cst_key in silver.crm_cust_info

SELECT
        REPLACE(cid,'-','') as cid,
        cntry
    FROM datawarehouse.erp_loc_a101


-- We will check for inalid cid from silver.crm_cust_info

SELECT
        REPLACE(cid,'-','') as cid,
        cntry
    FROM datawarehouse.erp_loc_a101
    WHERE REPLACE(cid,'-','') NOT IN (SELECT cst_key FROM silver.crm_cust_info)

--- Important point here if we use cid on where while comparison it gives every thing
--- But if we give REPLACE(cid,'-','') this it will give correct answer

-- Data standardization & Consistency

SELECT DISTINCT cntry from datawarehouse.erp_loc_a101

-- As there are null, empty string, abbrevations

SELECT
  REPLACE(cid, '-', '') AS cid,
  CASE
    WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(cntry, '\r',''), '\n',''), '\t',''), CHAR(160), ''))) = 'DE'
      THEN 'Germany'
    WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(cntry, '\r',''), '\n',''), '\t',''), CHAR(160), ''))) IN ('US', 'USA')
      THEN 'United States'
    WHEN cntry IS NULL
      OR UPPER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(cntry, '\r',''), '\n',''), '\t',''), CHAR(160), ''))) = ''
      THEN 'n/a'
    ELSE TRIM(cntry)  -- keep original (trimmed) when not mapped
  END AS cntry
FROM datawarehouse.erp_loc_a101;

/*
If your ERP or CRM data was exported from Excel or a legacy system, these characters often appear because:
    Users press Enter in a cell → adds \n.
    Copy-paste from Word → adds \r.
    Tab-delimited files → adds \t.
*/


-- Inserting the data into silver.erp_loc_a101


INSERT into silver.erp_loc_a101
    (cid,
    cntry)
    SELECT
  REPLACE(cid, '-', '') AS cid,
  CASE
    WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(cntry, '\r',''), '\n',''), '\t',''), CHAR(160), ''))) = 'DE'
      THEN 'Germany'
    WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(cntry, '\r',''), '\n',''), '\t',''), CHAR(160), ''))) IN ('US', 'USA')
      THEN 'United States'
    WHEN cntry IS NULL
      OR UPPER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(cntry, '\r',''), '\n',''), '\t',''), CHAR(160), ''))) = ''
      THEN 'n/a'
    ELSE TRIM(cntry)  -- keep original (trimmed) when not mapped
  END AS cntry
FROM datawarehouse.erp_loc_a101;

SELECT * from silver.erp_loc_a101

--- Now we will double check for invalid errors

SELECT
        REPLACE(cid,'-','') as cid,
        cntry
    FROM silver.erp_loc_a101
    WHERE REPLACE(cid,'-','') NOT IN (SELECT cst_key FROM silver.crm_cust_info)

SELECT DISTINCT cntry from silver.erp_loc_a101


---------------------------------------------------------

-- Now for datawarehouse.px_cat_g1v2

SELECT * from datawarehouse.px_cat_g1v2

RENAME TABLE datawarehouse.px_cat_g1v2 to datawarehouse.erp_px_cat_g1v2

SELECT * from datawarehouse.erp_px_cat_g1v2

SELECT * from silver.prd_info

-- Check for unwanted spaces

SELECT * from datawarehouse.erp_px_cat_g1v2
    WHERE id != TRIM(id) OR subcat != TRIM(subcat) or maintenance != TRIM(maintenance)

SELECT DISTINCT cat FROM datawarehouse.erp_px_cat_g1v2

SELECT DISTINCT subcat FROM datawarehouse.erp_px_cat_g1v2

SELECT DISTINCT maintenance FROM datawarehouse.erp_px_cat_g1v2
-- As our output showing two 'Yes' which is not a good sign

TRUNCATE TABLE silver.erp_px_cat_g1v2

INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat , maintenance)
    SELECT id, cat ,subcat ,maintenance FROM datawarehouse.erp_px_cat_g1v2

SELECT * FROM silver.erp_px_cat_g1v2


