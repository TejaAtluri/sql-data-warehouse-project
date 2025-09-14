
Select top 100 * from [bronze].crm_cust_info where cst_id=28389
Select top 100 * from [bronze].crm_prd_info where Prd_key like '%BK-M82S-44%'
Select top 100 * from [bronze].crm_sales_details where  sls_cust_id=28389 and sls_Prd_key like '%BK-M82S-44%'
Select top 100 * from [bronze].erp_CUST_AZ12;
Select top 100 * from [bronze].erp_LOC_A101;
Select top 100 * from [bronze].erp_PX_CAT_G1V2;

/*============================================================
Data Analysis for [crm_cust_info]
============================================================*/

/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [cst_id]
      ,[cst_key]
      ,[cst_firstname]
      ,[cst_lastname]
      ,[cst_marital_status]
      ,[cst_gndr]
      ,[cst_create_date]
  FROM [DataWarehouse].[bronze].[crm_cust_info] 
   


  --Data Analysis on cst_id: Duplicate check
  SELECT 
  cst_id
  ,count(cst_id) as Duplicates
  FROM [bronze].crm_cust_info 
  GROUP BY cst_id
  HAVING count(cst_id) >1

  --Data Analysis on cst_id: NULL and negative value check
  SELECT 
  *
  FROM [bronze].crm_cust_info 
  WHERE cst_id<=0 or cst_id='' or cst_id is NULL 

  --Data Analysis on cst_key: Duplicate check
    SELECT 
  cst_key
  ,count(cst_key) as Duplicates
  FROM [bronze].crm_cust_info 
  GROUP BY cst_key
  HAVING count(cst_key) >1
  
  --Data Analysis on cst_id & cst_key: Duplicate check on combination
  SELECT 
  cst_id
  ,cst_key
  ,count(cst_key) as Duplicates
  FROM [bronze].crm_cust_info 
  GROUP BY cst_id,cst_key
  HAVING count(cst_key) >1

  --Data Analysis on cst_key: NULL and invalid data validation check
  SELECT 
  cst_key
  FROM [bronze].crm_cust_info 
  WHERE cst_key='0' or cst_key='' or trim(cst_key) like' %' or cst_key is NULL 

  --Data Analysis on cst_firstname: Data quality check
  SELECT 
  [cst_firstname]
  FROM [bronze].crm_cust_info 
  WHERE [cst_firstname] is NULL or [cst_firstname]='' or [cst_firstname] like ' %' or [cst_firstname] like '% '

  
  SELECT 
  [cst_firstname]
  FROM [bronze].crm_cust_info 
  WHERE [cst_firstname] <> TRIM([cst_firstname])

  
  --Data Analysis on cst_lastname: Data quality check
  SELECT 
  [cst_lastname]
  ,LEN([cst_lastname])
  ,DATALENGTH([cst_lastname])
  ,TRIM(REPLACE(cst_lastname, CHAR(160), ''))
  ,LEN(TRIM(REPLACE(cst_lastname, CHAR(160), '')))
  ,DATALENGTH(TRIM(REPLACE(cst_lastname, CHAR(160), '')))
  FROM [bronze].crm_cust_info 
  WHERE [cst_lastname] like '% ' or [cst_lastname] like ' %'

  
  SELECT 
  [cst_lastname]
  FROM [bronze].crm_cust_info 
  WHERE [cst_lastname] <> TRIM([cst_lastname]) --and cst_lastname='Henderson ' 


  SELECT @@VERSION;

  SELECT 
    cst_lastname,
    ASCII(RIGHT(cst_lastname, 1)) AS LastCharCode
FROM bronze.crm_cust_info
WHERE cst_lastname LIKE '%Henderson%';

-- Data analysis on cst_marital_status: Distinct
Select 
distinct(cst_marital_status)
from [bronze].crm_cust_info

Select 
*,
CASE TRIM(UPPER(cst_marital_status))
		WHEN 'M' THEN 'Married'
		WHEN 'S' THEN 'Single'
		ELSE 'N/A'
	END AS cst_marital_status
FROM bronze.crm_cust_info


-- Data analysis on cst_gndr: Distinct
Select 
distinct(cst_gndr)
from [bronze].crm_cust_info

-- Data analysis on cst_create_date: Distinct
Select 
MIN(cst_create_date)
,MAX(cst_create_date)
from [bronze].crm_cust_info


/*=================Validation checks on [silver].[crm_cust_info] after insertion======================*/
 --Data Analysis on cst_id: Duplicate check
  SELECT 
  cst_id
  ,count(cst_id) as Duplicates
  FROM [silver].crm_cust_info 
  GROUP BY cst_id
  HAVING count(cst_id) >1

  --Data Analysis on cst_id: NULL and negative value check
  SELECT 
  *
  FROM [silver].crm_cust_info 
  WHERE cst_id<=0 or cst_id='' or cst_id is NULL 

  --Data Analysis on cst_key: Duplicate check
    SELECT 
  cst_key
  ,count(cst_key) as Duplicates
  FROM [silver].crm_cust_info 
  GROUP BY cst_key
  HAVING count(cst_key) >1
  
  --Data Analysis on cst_id & cst_key: Duplicate check on combination
  SELECT 
  cst_id
  ,cst_key
  ,count(cst_key) as Duplicates
  FROM [silver].crm_cust_info 
  GROUP BY cst_id,cst_key
  HAVING count(cst_key) >1

  --Data Analysis on cst_key: NULL and invalid data validation check
  SELECT 
  cst_key
  FROM [silver].crm_cust_info 
  WHERE cst_key='0' or cst_key='' or trim(cst_key) like' %' or cst_key is NULL 

  --Data Analysis on cst_firstname: Data quality check
  SELECT 
  [cst_firstname]
  FROM [silver].crm_cust_info 
  WHERE [cst_firstname] is NULL or [cst_firstname]='' or [cst_firstname] like ' %' or [cst_firstname] like '% '

  
  SELECT 
  [cst_firstname]
  FROM [silver].crm_cust_info 
  WHERE [cst_firstname] <> TRIM([cst_firstname])

  
  --Data Analysis on cst_lastname: Data quality check
  SELECT 
  [cst_lastname]
  ,LEN([cst_lastname])
  ,DATALENGTH([cst_lastname])
  ,TRIM(REPLACE(cst_lastname, CHAR(160), ''))
  ,LEN(TRIM(REPLACE(cst_lastname, CHAR(160), '')))
  ,DATALENGTH(TRIM(REPLACE(cst_lastname, CHAR(160), '')))
  FROM [silver].crm_cust_info 
  WHERE [cst_lastname]='' or [cst_lastname] like '% ' or [cst_lastname] like ' %'

  
  SELECT 
  [cst_lastname]
  FROM [silver].crm_cust_info 
  WHERE [cst_lastname] <> TRIM([cst_lastname]) --and cst_lastname='Henderson ' 


  SELECT @@VERSION;

  SELECT 
    cst_lastname,
    ASCII(RIGHT(cst_lastname, 1)) AS LastCharCode
FROM silver.crm_cust_info
WHERE cst_lastname LIKE '%Henderson%';

-- Data analysis on cst_marital_status: Distinct
Select 
distinct(cst_marital_status)
from [silver].crm_cust_info

-- Data analysis on cst_gndr: Distinct
Select 
distinct(cst_gndr)
from [silver].crm_cust_info

-- Data analysis on cst_create_date: Distinct
Select 
MIN(cst_create_date)
,MAX(cst_create_date)
from [silver].crm_cust_info


/*=================Validation checks on [Bronze].[crm_prd_info] after insertion======================*/
--Data Analysis on prd_id: Duplicate check
SELECT 
	prd_id 
	,count(prd_id) as Duplicates
FROM [bronze].crm_prd_info 
GROUP BY prd_id
HAVING count(prd_id) >1

  --Data Analysis on prd_id: NULL and negative value check
  SELECT 
  *
  FROM [bronze].crm_prd_info 
  WHERE prd_id<=0 or prd_id='' or prd_id is NULL 

  --Data Analysis on prd_key: Duplicate check
   SELECT 
  prd_key
  ,count(prd_key) as Duplicates
  FROM [bronze].crm_prd_info 
  GROUP BY prd_key
  HAVING count(prd_key) >1
  
  --Data Analysis on prd_id & prd_key: Duplicate check on combination
  SELECT 
  prd_id
  ,prd_key
  ,count(prd_key) as Duplicates
  FROM [bronze].crm_prd_info 
  GROUP BY prd_id,prd_key
  HAVING count(prd_key) >1

  --Data Analysis on prd_key: NULL and invalid data validation check
  SELECT 
  prd_key
  FROM [bronze].crm_prd_info 
  WHERE prd_key='0' or prd_key='' or trim(prd_key) like' %' or prd_key is NULL 

  --Data Analysis on prd_key: Format check for matching with respective mapping column in crm_sales_details & erp_PX_CAT_G1V2
    Select ID from bronze.[erp_PX_CAT_G1V2] WHERE TRIM(REPLACE(ID,CHAR(160),'')) like 'CO_PE%''__\___' ESCAPE '\';

  SELECT 
  distinct(prd_key)
  FROM [bronze].crm_prd_info 
  WHERE prd_key like 'CO_PE%' ESCAPE '\';

  --Data Analysis on prd_nm: Data quality check
  SELECT 
	prd_nm
  FROM [bronze].crm_prd_info 
  WHERE prd_nm is NULL or prd_nm='' or prd_nm like ' %' or prd_nm like '% '

  
  SELECT 
  prd_nm
  FROM [bronze].crm_prd_info 
  WHERE prd_nm <> TRIM(prd_nm)
  
--Data Analysis on prd_cst: Data quality check
SELECT 
	*
FROM [bronze].crm_prd_info 
WHERE prd_cost is NULL or prd_cost='' or prd_cost like ' %' or prd_cost like '% ' or prd_cost<=0

--Data Analysis on prd_cst: Range - Min & Max cost values
SELECT 
	Min(prd_cost)
	,MAX(prd_cost)
FROM [bronze].crm_prd_info 

--Data Analysis on prd_line: Distinct values
SELECT 
	DISTINCT(prd_line)
FROM [bronze].crm_prd_info 

SELECT 
	Count(*)
FROM [bronze].crm_prd_info 
WHERE prd_line is NULL

--Data Analysis on prd_start_dt: Distinct values
SELECT 
	Count(*)
FROM [bronze].crm_prd_info 
Where prd_start_dt is not null

--Data Analysis on prd_start_dt: Min & Max Range
SELECT 
	MIN(prd_start_dt) AS min_date
	,MAX(prd_start_dt) AS max_date
FROM [bronze].crm_prd_info 


--Data Analysis on prd_end_dt: Distinct values
SELECT 
	Count(*)
FROM [bronze].crm_prd_info 
Where prd_end_dt is not null

--Data Analysis on prd_end_dt: Min & Max Range
SELECT 
	MIN(prd_end_dt) AS min_date
	,MAX(prd_end_dt) AS max_date
FROM [bronze].crm_prd_info 
Where prd_end_dt is not null

--Data Analysis on prd_end_dt: Compare with startdate
SELECT 
	*
FROM [bronze].crm_prd_info 
Where prd_end_dt < prd_start_dt

--Data Analysis on prd_end_dt: Identify all prd_key records if any startdate > end date
WITH cte_prd_date_range AS
(
	SELECT 
		distinct(prd_key) AS prd_key
	FROM [bronze].crm_prd_info 
	WHERE prd_end_dt < prd_start_dt
)

SELECT
*
FROM [bronze].crm_prd_info CP join 
	(
	SELECT 
	CPI.prd_key,
	Count(CPI.prd_key) as NumberOfRecords
	FROM [bronze].crm_prd_info CPI join cte_prd_date_range CPDR
	ON CPI.prd_key=CPDR.prd_key
	GROUP BY CPI.prd_key
	) t
	ON cp.prd_key=t.prd_key
ORDER BY cp.prd_key, prd_start_dt asc
--Having Count(CPI.prd_key)>1


/*=================Validation checks on [silver].[crm_prd_info] after insertion======================*/
--Data Analysis on prd_id: Duplicate check
SELECT 
	prd_id 
	,count(prd_id) as Duplicates
FROM [silver].crm_prd_info 
GROUP BY prd_id
HAVING count(prd_id) >1

  --Data Analysis on prd_id: NULL and negative value check
  SELECT 
  *
  FROM [silver].crm_prd_info 
  WHERE prd_id<=0 or prd_id='' or prd_id is NULL 

  --Data Analysis on prd_key: Duplicate check
   SELECT 
  prd_key
  ,count(prd_key) as Duplicates
  FROM [silver].crm_prd_info 
  GROUP BY prd_key
  HAVING count(prd_key) >1
  
  --Data Analysis on prd_id & prd_key: Duplicate check on combination
  SELECT 
  prd_id
  ,prd_key
  ,count(prd_key) as Duplicates
  FROM [silver].crm_prd_info 
  GROUP BY prd_id,prd_key
  HAVING count(prd_key) >1

  --Data Analysis on prd_key: NULL and invalid data validation check
  SELECT 
  prd_key
  FROM [silver].crm_prd_info 
  WHERE prd_key='0' or prd_key='' or trim(prd_key) like' %' or prd_key is NULL 

  --Data Analysis on prd_nm: Data quality check
  SELECT 
	prd_nm
  FROM [silver].crm_prd_info 
  WHERE prd_nm is NULL or prd_nm='' or prd_nm like ' %' or prd_nm like '% '

  
  SELECT 
  prd_nm
  FROM [silver].crm_prd_info 
  WHERE prd_nm <> TRIM(prd_nm)
  
--Data Analysis on prd_cst: Data quality check
SELECT 
	*
FROM [silver].crm_prd_info 
WHERE prd_cost is NULL or prd_cost='' or prd_cost like ' %' or prd_cost like '% ' or prd_cost<=0

--Data Analysis on prd_cst: Range - Min & Max cost values
SELECT 
	Min(prd_cost)
	,MAX(prd_cost)
FROM [silver].crm_prd_info 

--Data Analysis on prd_line: Distinct values
SELECT 
	DISTINCT(prd_line)
FROM [silver].crm_prd_info 

SELECT 
	Count(*)
FROM [silver].crm_prd_info 
WHERE prd_line is NULL

--Data Analysis on prd_start_dt: Distinct values
SELECT 
	Count(*)
FROM [silver].crm_prd_info 
Where prd_start_dt is not null

--Data Analysis on prd_start_dt: Min & Max Range
SELECT 
	MIN(prd_start_dt) AS min_date
	,MAX(prd_start_dt) AS max_date
FROM [silver].crm_prd_info 


--Data Analysis on prd_end_dt: Distinct values
SELECT 
	Count(*)
FROM [silver].crm_prd_info 
Where prd_end_dt is not null

--Data Analysis on prd_end_dt: Min & Max Range
SELECT 
	MIN(prd_end_dt) AS min_date
	,MAX(prd_end_dt) AS max_date
FROM [silver].crm_prd_info 
Where prd_end_dt is not null

--Data Analysis on prd_end_dt: Compare with startdate
SELECT 
	*
FROM [silver].crm_prd_info 
Where prd_end_dt < prd_start_dt

--Data Analysis on prd_end_dt: Identify all prd_key records if any startdate > end date
WITH cte_prd_date_range AS
(
	SELECT 
		distinct(prd_key) AS prd_key
	FROM [silver].crm_prd_info 
	WHERE prd_end_dt < prd_start_dt
)

SELECT
*
FROM [silver].crm_prd_info CP join 
	(
	SELECT 
	CPI.prd_key,
	Count(CPI.prd_key) as NumberOfRecords
	FROM [silver].crm_prd_info CPI join cte_prd_date_range CPDR
	ON CPI.prd_key=CPDR.prd_key
	GROUP BY CPI.prd_key
	) t
	ON cp.prd_key=t.prd_key
ORDER BY cp.prd_key, prd_start_dt asc


--Data Analysis on prd_end_dt: cat_id is valid
SELECT 
cat_id as cat_id
FROM [silver].crm_prd_info
WHERE cat_id not IN (SELECT ID FROM [bronze].[erp_PX_CAT_G1V2])

SELECT ID FROM [bronze].[erp_PX_CAT_G1V2] where ID='CO_PE'


/*============================================================
Data Analysis for [crm_sales_details]
============================================================*/
-- Data Analysis on sls_ord_num: Data Quality check
SELECT 
*
FROM bronze.crm_sales_details
WHERE sls_ord_num IS NULL or sls_ord_num='' or sls_ord_num like ' %' or sls_ord_num like '% '

-- Data Analysis on sls_ord_num: Data format check
SELECT 
*
FROM bronze.crm_sales_details
WHERE sls_ord_num not like 'SO%'

-- Data Analysis on sls_ord_num: Duplicate check
SELECT 
sls_ord_num
,COUNT(sls_ord_num)
FROM bronze.crm_sales_details
GROUP BY sls_ord_num
having COUNT(sls_ord_num)>1

-- Data Analysis on sls_ord_num: Duplicate check on combination
SELECT 
sls_ord_num
,sls_prd_key
,COUNT(sls_ord_num)
FROM bronze.crm_sales_details
GROUP BY sls_ord_num,sls_prd_key
having COUNT(sls_ord_num)>1

-- Data Analysis on sls_prd_key: Duplicate check 
SELECT 
sls_prd_key
,COUNT(sls_prd_key)
FROM bronze.crm_sales_details
GROUP BY sls_prd_key
having COUNT(sls_prd_key)>1

-- Data Analysis on sls_prd_key: Data Quality check
SELECT 
*
FROM bronze.crm_sales_details
WHERE sls_prd_key IS NULL or sls_prd_key='' or sls_prd_key like ' %' or sls_prd_key like '% '

-- Data Analysis on sls_prd_key: Data format check
SELECT 
*
FROM bronze.crm_sales_details
WHERE sls_prd_key not like '__-%'

-- Data Analysis on sls_prd_key: Check existance in silver.crm_prd_info
SELECT 
*
FROM bronze.crm_sales_details
WHERE sls_prd_key not in (SELECT prd_key FROM silver.crm_prd_info)

-- Data Analysis on sls_cust_id: Data format check
SELECT 
*
FROM bronze.crm_sales_details 
WHERE sls_cust_id not in (SELECT cst_id FROM bronze.crm_cust_info)

-- Data Analysis on sls_cust_id: Data Quality check
SELECT 
*
FROM bronze.crm_sales_details
WHERE sls_cust_id IS NULL or sls_cust_id='' or sls_cust_id like ' %' or sls_cust_id like '% '

-- Data Analysis on datecolumns: Min & MAX of date
SELECT 
MIN(sls_order_dt)
,MAX(sls_order_dt)
,MIN(sls_ship_dt)
,MAX(sls_ship_dt)
,MIN(sls_due_dt)
,MAX(sls_due_dt)
FROM bronze.crm_sales_details
WHERE sls_order_dt IS NOT NULL  --sls_due_dt IS  NOT NULL and 

-- Data Analysis on datecolumns: Data Quality check
SELECT 
*
FROM bronze.crm_sales_details
WHERE sls_order_dt>sls_ship_dt or sls_order_dt>sls_due_dt

-- Data Analysis on datecolumns: Convert to date format
SELECT 
TOP 10 *
,CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) AS sls_order_dt
FROM bronze.crm_sales_details

-- Data Analysis on datecolumns: Sales, Quantity & Price 
SELECT 
*
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL or sls_sales<=0 or sls_quantity IS NULL or sls_quantity<=0 or sls_price IS NULL or sls_price<=0  --or sls_sales like ' %' or sls_sales like '% '

SELECT 
sls_ord_num
,sls_sales
,sls_quantity
,sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num in
(
	SELECT 
	sls_ord_num
	--,sls_sales
	--,sls_quantity
	--,sls_price
	FROM bronze.crm_sales_details
	WHERE (sls_sales !=sls_quantity*sls_price 
	or sls_sales IS NULL or sls_sales<=0 or sls_quantity IS NULL or sls_quantity<=0 or sls_price IS NULL)-- or sls_price<=0 )
	--sls_ord_num ='SO58335' AND 

	EXCEPT

	SELECT 
		sls_ord_num
		--,CASE 
		--	WHEN sls_sales is NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price)-- AND (sls_price<>0 AND sls_price IS  not NULL) 
		--	THEN sls_quantity*ABS(sls_price)
		--	ELSE sls_sales
		--END AS sls_sales
		--,sls_quantity
		--,CASE
		--	WHEN sls_price is NULL OR sls_price<=0 -- AND sls_sales>0 
		--	THEN sls_sales/NULLIF(sls_quantity,0)
		--	ELSE ABS(sls_price)
		--END AS sls_price
	FROM bronze.crm_sales_details
	WHERE sls_sales!=sls_quantity* ABS(sls_price)
)
AND sls_sales !=sls_quantity*sls_price 
	or sls_sales IS NULL or sls_sales<=0 or sls_quantity IS NULL or sls_quantity<=0 or sls_price IS NULL

/*============================================================
Data Analysis for [erp_CUST_AZ12]
============================================================*/
-- Data Analysis on ID: Data Quality check

SELECT
	--CASE
	--WHEN CID	
	--,BDATE 
	--,GEN 
	LEN(MIN(CID))
	,LEN(MAX(CID))
FROM bronze.[erp_CUST_AZ12]

SELECT
	DISTINCT(CID)
FROM bronze.[erp_CUST_AZ12]
WHERE BDATE LIKE 'NAS%' OR CID LIKE 'AW%'

SELECT
	*
FROM bronze.[erp_CUST_AZ12]
WHERE CID IS NULL or CID='' or CID like ' %' or CID like '% '

SELECT 
*
FROM SILVER.crm_cust_info
WHERE cst_key NOT IN 
(
SELECT CASE
		WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
		ELSE CID
	END AS CID
FROM BRONZE.[erp_CUST_AZ12]
)

-- Data Analysis on BDATE: Data Quality check
SELECT
	--CASE
	--WHEN CID	
	--,BDATE 
	--,GEN 
	MIN(BDATE)
	,MAX(BDATE)
FROM bronze.[erp_CUST_AZ12]

SELECT
	*
FROM bronze.[erp_CUST_AZ12]
WHERE BDATE IS NULL or BDATE='' or BDATE like ' %' or BDATE like '% '

-- Data Analysis on GEN: Data Quality check
SELECT
DISTINCT (GEN)
FROM bronze.[erp_CUST_AZ12]

SELECT
	*
FROM bronze.[erp_CUST_AZ12]
WHERE GEN IS NULL or GEN='' or GEN like ' %' or GEN like '% '

SELECT
	CASE
		WHEN UPPER(TRIM(GEN))IN ('M','MALE') THEN 'Male'
		WHEN UPPER(TRIM(GEN))IN ('F','FEMALE') THEN 'Female'
		ELSE NULL
	END AS GEN
FROM bronze.[erp_CUST_AZ12]

/*=====================Validate inserted data in [silver].[erp_CUST_AZ12]======================*/
-- Data Analysis on ID: Data Quality check

SELECT
	--CASE
	--WHEN CID	
	--,BDATE 
	--,GEN 
	LEN(MIN(CID))
	,LEN(MAX(CID))
FROM silver.[erp_CUST_AZ12]

SELECT
	DISTINCT(CID)
FROM silver.[erp_CUST_AZ12]
WHERE BDATE LIKE 'NAS%' --OR CID LIKE 'AW%'

SELECT
	*
FROM silver.[erp_CUST_AZ12]
WHERE CID IS NULL or CID='' or CID like ' %' or CID like '% '


-- Data Analysis on BDATE: Data Quality check
SELECT
	--CASE
	--WHEN CID	
	--,BDATE 
	--,GEN 
	MIN(BDATE)
	,MAX(BDATE)
FROM silver.[erp_CUST_AZ12]

SELECT
	*
FROM silver.[erp_CUST_AZ12]
WHERE BDATE IS NULL or BDATE='' or BDATE like ' %' or BDATE like '% '

-- Data Analysis on GEN: Data Quality check
SELECT
DISTINCT (GEN)
FROM silver.[erp_CUST_AZ12]

SELECT
	*
FROM silver.[erp_CUST_AZ12]
WHERE GEN IS NULL or GEN='' or GEN like ' %' or GEN like '% '



/*============================================================
Data Analysis for [erp_LOC_A101]
============================================================*/
-- Data Analysis on ID: Data Quality check

SELECT
	--CASE
	--WHEN CID	
	--,BDATE 
	--,GEN 
	LEN(MIN(CID))
	,LEN(MAX(CID))
FROM bronze.erp_LOC_A101

SELECT
	DISTINCT(CID)
FROM bronze.erp_LOC_A101
WHERE CID NOT LIKE 'AW-%'

SELECT
	*
FROM bronze.erp_LOC_A101
WHERE CID IS NULL or CID='' or CID like ' %' or CID like '% '

SELECT 
*
FROM SILVER.crm_cust_info
WHERE cst_key IN 
(
SELECT REPLACE(CID,'-','') AS CID
FROM BRONZE.erp_LOC_A101
)

-- Data Analysis on CNTRY: Data Quality check
SELECT
	DISTINCT(CNTRY)
FROM bronze.erp_LOC_A101 WHERE CNTRY IN (

SELECT
CASE
	WHEN UPPER(TRIM(CNTRY))IN ('US','USA','UNITED STATES') THEN 'United States'
	WHEN UPPER(TRIM(CNTRY))IN ('DE','GERMANY') THEN 'Germany'
	WHEN CNTRY IS NULL OR CNTRY='' THEN 'n/a'
	ELSE CNTRY
END AS CNTRY
FROM bronze.erp_LOC_A101

)


/*======================Validate [silver].[erp_LOC_A101]======================*/
-- Data Analysis on ID: Data Quality check

SELECT
	--CASE
	--WHEN CID	
	--,BDATE 
	--,GEN 
	LEN(MIN(CID))
	,LEN(MAX(CID))
FROM silver.erp_LOC_A101

SELECT
	DISTINCT(CID)
FROM silver.erp_LOC_A101
WHERE CID LIKE 'AW-%'

SELECT
	*
FROM silver.erp_LOC_A101
WHERE CID IS NULL or CID='' or CID like ' %' or CID like '% '

SELECT 
*
FROM SILVER.crm_cust_info
WHERE cst_key not IN 
(
SELECT REPLACE(CID,'-','') AS CID
FROM silver.erp_LOC_A101
)

-- Data Analysis on CNTRY: Data Quality check
SELECT
	DISTINCT(CNTRY)
FROM silver.erp_LOC_A101






/*============================================================
Data Analysis for [erp_PX_CAT_G1V2]
============================================================*/
-- Data Analysis on ID: Data Quality check

SELECT
	DISTINCT(ID)
FROM bronze.erp_PX_CAT_G1V2
WHERE ID IN (SELECT CAT_ID FROM silver.crm_prd_info)


SELECT
	*
FROM bronze.erp_PX_CAT_G1V2
WHERE ID IS NULL or ID='' or ID like ' %' or ID like '% '


-- Data Analysis on CAT: Data Quality check
SELECT
	DISTINCT(CAT)
FROM bronze.erp_PX_CAT_G1V2 
WHERE CAT!=TRIM(CAT)

SELECT
	*
FROM bronze.erp_PX_CAT_G1V2
WHERE CAT IS NULL or CAT='' or CAT like ' %' or CAT like '% '



-- Data Analysis on SUBCAT: Data Quality check
SELECT
	DISTINCT(SUBCAT)
FROM bronze.erp_PX_CAT_G1V2 
WHERE SUBCAT!=TRIM(SUBCAT)

SELECT
	*
FROM bronze.erp_PX_CAT_G1V2
WHERE SUBCAT IS NULL or SUBCAT='' or SUBCAT like ' %' or SUBCAT like '% '

-- Data Analysis on MAINTENANCE: Data Quality check
SELECT
	DISTINCT(MAINTENANCE)
FROM bronze.erp_PX_CAT_G1V2 
WHERE MAINTENANCE!=TRIM(MAINTENANCE)


--Select top 100 * from silver.crm_cust_info 
--Select top 100 * from silver.crm_prd_info 
--Select top 100 * from silver.crm_sales_details 
--Select top 100 * from silver.erp_CUST_AZ12;
--Select top 100 * from silver.erp_LOC_A101;
--Select top 100 * from silver.erp_PX_CAT_G1V2;
