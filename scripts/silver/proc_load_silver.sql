/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

DROP PROCEDURE IF EXISTS silver.load_silver;

DELIMITER $$

CREATE PROCEDURE silver.load_silver()
BEGIN
  -- Declare variables for start and end times
  DECLARE v_start_time DATETIME;
  DECLARE v_end_time DATETIME;
  DECLARE exit handler for SQLEXCEPTION
    BEGIN
      -- If an error occurs, capture the end time and print error message
      SET v_end_time = NOW();
      SELECT CONCAT('[ERROR] Error occurred at: ', v_end_time) AS message;
      SELECT CONCAT('[ERROR] Batch failed after: ', TIMEDIFF(v_end_time, v_start_time)) AS message;
      ROLLBACK; -- Optionally rollback if the transaction was started
    END;

  -- Declare handler for warning (e.g., non-fatal warnings)
  DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
      -- Log warning message
      SET v_end_time = NOW();
      SELECT CONCAT('[WARNING] Warning occurred at: ', v_end_time) AS message;
    END;

  -- Declare handler for NO_DATA_FOUND (if query returns no results)
  DECLARE CONTINUE HANDLER FOR NOT FOUND
    BEGIN
      -- Log if no data was found (e.g., empty table result)
      SET v_end_time = NOW();
      SELECT CONCAT('[INFO] No data found at: ', v_end_time) AS message;
    END;

  -- Start transaction (optional, depending on your requirements)
  START TRANSACTION;
  
  -- Set start time
  SET v_start_time = NOW();
  SELECT CONCAT('[INFO] Batch started at: ', v_start_time) AS message;

  -- Load into silver.crm_cust_info
  SELECT '[INFO] Loading into silver.crm_cust_info...' AS message;
  TRUNCATE TABLE silver.crm_cust_info;

  INSERT INTO silver.crm_cust_info (
      cst_id,
      cst_key,
      cst_firstname,
      cst_lastname,
      cst_material_status,
      cst_gndr,
      cst_create_date
  )
  SELECT
      cst_id,
      cst_key,
      TRIM(cst_firstname) AS cst_firstname,
      TRIM(cst_lastname) AS cst_lastname,
      CASE 
          WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
          WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
          ELSE 'n/a'
      END AS cst_maritial_status,
      CASE 
          WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
          WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
          ELSE 'n/a'
      END AS cst_gndr,
      cst_create_date
  FROM (
      SELECT 
          *,
          ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
      FROM bronze.crm_cust_info
  ) t 
  WHERE flag_last = 1;

  -- Load into silver.crm_prd_info
  SELECT '[INFO] Loading into silver.crm_prd_info...' AS message;
  TRUNCATE TABLE silver.crm_prd_info;

  INSERT INTO silver.crm_prd_info (
      prd_id,
      cat_id,
      prd_key,
      prd_nm,
      prd_cost,
      prd_line,
      prd_start_dt,
      prd_end_dt
  )
  SELECT 
      prd_id,
      REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
      SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
      prd_nm,
      IFNULL(prd_cost, 0) AS prd_cost,
      CASE UPPER(TRIM(prd_line))
          WHEN 'M' THEN 'Mountain'
          WHEN 'R' THEN 'Road'
          WHEN 'S' THEN 'Other Sales'
          WHEN 'T' THEN 'Touring'
          ELSE 'n/a'
      END AS prd_line,
      CAST(prd_start_dt AS DATE) AS prd_start_dt,
      CAST(DATE_SUB(
          LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt),
          INTERVAL 1 DAY
      ) AS DATE) AS prd_end_dt
  FROM bronze.crm_prd_info;

  -- Load into silver.crm_sales_details
  SELECT '[INFO] Loading into silver.crm_sales_details...' AS message;
  TRUNCATE TABLE silver.crm_sales_details;

  INSERT INTO silver.crm_sales_details (
      sls_ord_num,
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
      sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      CASE 
          WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
          ELSE CAST(sls_order_dt AS DATE)
      END AS sls_order_dt,
      CASE 
          WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
          ELSE CAST(sls_ship_dt AS DATE)
      END AS sls_ship_dt,
      CASE 
          WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
          ELSE CAST(sls_due_dt AS DATE)
      END AS sls_due_dt,
      CASE 
          WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
              THEN sls_quantity * ABS(sls_price)
          ELSE sls_sales
      END AS sls_sales,
      sls_quantity,
      CASE 
          WHEN sls_price IS NULL OR sls_price <= 0
              THEN sls_sales / NULLIF(sls_quantity, 0)
          ELSE sls_price
      END AS sls_price
  FROM bronze.crm_sales_details;

  -- Load into silver.erp_cust_az12
  SELECT '[INFO] Loading into silver.erp_cust_az12...' AS message;
  TRUNCATE TABLE silver.erp_cust_az12;

  INSERT INTO silver.erp_cust_az12 (
      cid,
      bdate,
      gen
  )
  SELECT
      CASE 
          WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
          ELSE cid
      END AS cid,
      CASE 
          WHEN bdate > CURRENT_DATE() THEN NULL
          ELSE bdate
      END AS bdate,
      CASE
          WHEN gen IS NULL OR REGEXP_REPLACE(gen, '\\s+', '') = '' THEN 'n/a'
          WHEN UPPER(REGEXP_REPLACE(gen, '\\s+', '')) IN ('F', 'FEMALE') THEN 'Female'
          WHEN UPPER(REGEXP_REPLACE(gen, '\\s+', '')) IN ('M', 'MALE') THEN 'Male'
          ELSE 'n/a'
      END AS gen
  FROM bronze.erp_cust_az12;

  -- Load into silver.erp_loc_a101
  SELECT '[INFO] Loading into silver.erp_loc_a101...' AS message;
  TRUNCATE TABLE silver.erp_loc_a101;

  INSERT INTO silver.erp_loc_a101 (
      cid,
      cntry
  )
  SELECT
      REPLACE(cid, '-', '') AS cid,
      CASE
          WHEN cntry = '' THEN 'n/a'
          ELSE cntry
      END AS cntry
  FROM bronze.erp_loc_a101;

  -- Commit transaction (optional, if using transaction control)
  COMMIT;
  
  -- Set end time and display completion message
  SET v_end_time = NOW();
  SELECT CONCAT('[INFO] Batch completed at: ', v_end_time) AS message;
  SELECT CONCAT('[INFO] Total time taken: ', TIMEDIFF(v_end_time, v_start_time)) AS message;

END $$

DELIMITER ;

CALL silver.load_silver();
