# Databricks notebook source
%python
%pip install oauth2client
%pip install gspread
%pip install gspread_dataframe
%pip install databricks
%pip install google-api-python-client
%pip install pygsheets
import pandas as pd
import json
from pyspark.sql import SparkSession
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe

# Read from Google Sheet
def read_sheet(sheet_id, sheet_name, num_columns):
    service_account_json_str = dbutils.secrets.get(
        scope="analytics_team", 
        key="shivanshu-saurabh@woven-sequence-462514-g0.iam.gserviceaccount.com"
    )
    service_account_info = json.loads(service_account_json_str)
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
    all_data = sheet.get_all_values()
    headers = all_data[0][:num_columns]
    rows = [row[:num_columns] for row in all_data[1:]]

    pdf = pd.DataFrame(rows, columns=headers)
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(pdf)

    # Clean column names
    for col_name in df.columns:
        clean_name = ''.join(c if c.isalnum() else '_' for c in col_name)
        if clean_name and clean_name[0].isdigit():
            clean_name = 'col_' + clean_name
        df = df.withColumnRenamed(col_name, clean_name)
    
    return df, client, creds

# Write to Google Sheet
def write_sheet(df, sheet_id, sheet_name):
    pandas_df = df.toPandas()
    worksheet = gspread.authorize(creds).open_by_key(sheet_id).worksheet(sheet_name)
    worksheet.clear()
    set_with_dataframe(worksheet, pandas_df)

# Step 1: Read and load data from Google Sheet
data_df, client, creds = read_sheet(
    sheet_id='1ntyCyB7lBQA1uHueoi_8bwamFhfbtS3bKMbK4r1vvqo',
    sheet_name='Updated-Campaign Setup',
    num_columns=25
)

# Step 2: Write it to Delta table
data_df.write.mode('overwrite').saveAsTable("gold.scratch.drr_test")

# Step 3: Run the logic
query_df = spark.sql("""
WITH base_pvids AS (
  SELECT pvid.pvid FROM gold.scratch.drr_test AS pvid
),
sales_data AS (
  SELECT 
    mu.product_variant_id AS pvid,
    si.product_name AS campaign_product_name,
    SUM(mu.quantity) AS total_sales,
    SUM(CASE 
          WHEN mu.day >= CURRENT_DATE - INTERVAL '3' DAY 
          THEN mu.quantity 
          ELSE 0 
        END) AS sales_3day
  FROM gold.zepto.master_marketing_user_gppo mu
  JOIN base_pvids bp ON mu.product_variant_id = bp.pvid
  LEFT JOIN gold.zepto.sku_info si
    ON mu.product_variant_id = si.product_variant_id
  WHERE mu.gsv = 0
  GROUP BY mu.product_variant_id, si.product_name
),
grn_data AS (
  SELECT 
    LOWER(podet.sku) AS pvid,
    SUM(grn_qty) AS mh_grn
  FROM gold.ops.pl_po_details podet
  JOIN base_pvids bp ON LOWER(podet.sku) = LOWER(bp.pvid)
  LEFT JOIN gold.zepto.sku_info s 
    ON LOWER(podet.sku) = LOWER(s.product_variant_id)
  LEFT JOIN (
    SELECT externpocode, MAX(grn_date) AS po_grn_date 
    FROM gold.ops.pl_po_details 
    GROUP BY 1
  ) grn_po ON podet.externpocode = grn_po.externpocode
  WHERE podet.location_type = 'WAREHOUSE'
    AND grn_qty > 0
  GROUP BY LOWER(podet.sku)
),
inventory_data AS (
  SELECT 
    hs.product_variant_id AS pvid,
    SUM(COALESCE(hs.good_available + hs.cross_dock_available, 0)) AS retail_inventory
  FROM gold.ops.pl_inventory_all_legs hs
  JOIN base_pvids bp ON hs.product_variant_id = bp.pvid
  LEFT JOIN gold.zepto.stores s
    ON hs.store_id = s.store_id
  LEFT JOIN silver.oms.store_product sp
    ON hs.store_id = sp.store_id AND hs.product_variant_id = sp.product_variant_id
  WHERE hs.datestr = CURRENT_DATE
    AND hs.store_type = 'RETAIL_STORE'
    AND s.city_name <> 'Test City'
    AND store_online_status = TRUE
    AND store_active_status = TRUE
    AND store_live_status = TRUE
    AND sp.is_active = TRUE
    AND hour = (SELECT MAX(hour) FROM gold.ops.pl_inventory_all_legs WHERE datestr = CURRENT_DATE)
  GROUP BY hs.product_variant_id
)
,non_zero_gsv AS (
  SELECT DISTINCT mu.product_variant_id AS pvid
  FROM gold.zepto.master_marketing_user_gppo mu
  JOIN base_pvids bp ON mu.product_variant_id = bp.pvid
  WHERE mu.gsv != 0
)

-- Final Output
SELECT 
  s.pvid AS Product_variant_id,
  s.campaign_product_name AS CAMPAIGN_PRODUCT_NAME,
  s.total_sales AS SALES,
  COALESCE(g.mh_grn, 0) AS MH_GRN,
  ROUND(COALESCE(s.total_sales, 0) * 1.0 / NULLIF(g.mh_grn, 0), 4) AS Sales_Percent,
  s.sales_3day AS SALES_3DAYS,
  ROUND(s.sales_3day * 1.0 / 3, 2) AS DRR,
  CASE 
    WHEN s.sales_3day = 0 THEN 'infinity'
    ELSE ROUND(COALESCE(inv.retail_inventory, 0) / (s.sales_3day * 1.0 / 3), 2)
  END AS DOH
FROM sales_data s
LEFT JOIN grn_data g ON LOWER(s.pvid) = g.pvid
LEFT JOIN inventory_data inv ON s.pvid = inv.pvid
WHERE COALESCE(g.mh_grn, 0) - COALESCE(s.total_sales, 0) > 100
AND coalesce(g.mh_grn) != 0
AND NOT (
    COALESCE(s.total_sales, 0) = 0
    AND s.pvid IN (SELECT pvid FROM non_zero_gsv)
  )
ORDER BY Sales_Percent ASC
""")

# Step 4: Export to another Google Sheet
write_sheet(query_df, '16CP55NRyOMe4bSLXosVSKjCRaoek8vdMq0-Fo5ik70k', 'raw')
