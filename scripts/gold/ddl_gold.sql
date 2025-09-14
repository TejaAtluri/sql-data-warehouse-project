/*View for customers information*/
IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS  
SELECT
	ROW_NUMBER() OVER (ORDER BY CCI.[cst_id] ASC) AS customer_key
	,CCI.[cst_id] AS customer_id
	,CCI.[cst_key] AS customer_number
	,CCI.[cst_firstname] AS first_name
	,CCI.[cst_lastname] AS last_name
	,EL.[CNTRY] AS country
	,CCI.[cst_marital_status] AS marital_status
	,CASE
		WHEN CCI.[cst_gndr] !='n/a' THEN CCI.[cst_gndr]
		ELSE COALESCE(EC.[GEN],'n/a')
		END AS gender
	,EC.[BDATE] AS birthdate
	,CCI.[cst_create_date] AS create_date
FROM [silver].[crm_cust_info] CCI
LEFT JOIN [silver].[erp_CUST_AZ12] EC ON CCI.cst_KEY= EC.cid
LEFT JOIN [silver].[erp_LOC_A101] EL ON CCI.cst_KEY= EL.CID

/*View for products information*/
IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY CPI.[prd_start_dt],CPI.[prd_key]) AS product_key
	,CPI.[prd_id] AS product_id
	,CPI.[prd_key] AS product_number
	,CPI.[prd_nm] AS product_name
	,CPI.[cat_id] AS category_id
	,EPC.[CAT] AS category
    ,EPC.[SUBCAT] AS subcategory
    ,EPC.[MAINTENANCE] AS maintaince
	,CPI.[prd_cost] AS cost
	,CPI.[prd_line] AS product_line
	,CPI.[prd_start_dt] AS start_name
FROM [silver].[crm_prd_info] CPI
LEFT JOIN [silver].[erp_PX_CAT_G1V2] EPC ON CPI.cat_id=EPC.ID
WHERE CPI.[prd_end_dt] IS NULL


/*View for sales information*/
IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT [sls_ord_num] AS sales_id
      ,pr.product_key 
      ,cu.customer_key 
      ,[sls_order_dt] AS order_date
      ,[sls_ship_dt] AS shipping_date
      ,[sls_due_dt] AS due_date
      ,[sls_sales] AS sales_amount
      ,[sls_quantity] AS quantity
      ,[sls_price] AS price
  FROM [silver].[crm_sales_details] csd
  LEFT JOIN [gold].[dim_products] pr ON csd.[sls_prd_key]=pr.product_number
  LEFT JOIN [gold].[dim_customers] cu ON csd.[sls_cust_id]=cu.customer_id

