import duckdb
import os

token = os.getenv("MOTHERDUCK_TOKEN")
if not token:
	raise RuntimeError("MOTHERDUCK_TOKEN environment variable not set.")

# Connect to MotherDuck using the token
con = duckdb.connect(
    database='md:dto_base',
    config = {'motherduck_token': token} )
con.sql("""CREATE OR REPLACE TABLE tbl_raw_retail_sales_value AS(
  with json_rows as(
    SELECT unnest(from_json(json("dataSet"),'["JSON"]')) entries FROM
    read_json('https://www.censtatd.gov.hk/api/get.php?id=620-67001&lang=en&param=N4KABGBEDGBukC4zAL4BpxQM7yaCEkAagIIAyA+gEoDKiYA2pgVFQIYDuFAshQBYBrACYUhkZmAC6GFsQDylAJIARABrU6SJi0LsuARiEAHCgEshADwoA7SDJ2QAmgHtHFQyYCkFLOJbSJYnIKFXVaem0HPXdjM0sbOwlCFzcPCm9fCUlMdExIIwBTACdTZzE8QKwAFzYiqvpIACYABn0ANla-MFzCcwa2loBaNoB2Zs77SAAbNmsAcwaC2xAUIA',COLUMNS={dataSet:VARCHAR})
  )
  SELECT
    json_extract(entries,'$.freq')[2:-2]::VARCHAR freq,
    json_extract(entries,'$.period')[2:-2]::VARCHAR period,
    json_extract(entries,'$.sv')[2:-2]::VARCHAR sv,
    json_extract(entries,'$.svDesc')[2:-2]::VARCHAR svDesc,
    json_extract(entries,'$.figure')::DOUBLE  figure,
    json_extract(entries,'$.sd_value')[2:-2]::VARCHAR sd_value,
  FROM json_rows
)""")
con.close()