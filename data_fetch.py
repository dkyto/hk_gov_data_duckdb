import duckdb

con = duckdb.connect(database='./hk_gov_data.ddb')
con.sql(f"""USE hk_gov_data;""")
con.sql(f"""
CREATE OR REPLACE TABLE tbl_raw_retail_sales_value AS(
  with json_rows as(
    SELECT unnest(from_json(json("dataSet"),'["JSON"]')) entries FROM
    read_json('https://www.censtatd.gov.hk/api/get.php?id=620-67001&lang=en&param=N4KABGBEDGBukC4zAL4BpxQM7yaCEkAagIIAyA+gEoDKiYA2pgVFQIYDuFAshQBYBrACYUhkZmAC6GFsQDylAJIARABrU6SJi0LsuARiEAHCgEshADwoA7SDJ2QAmgHtHFQyYCkFLOJbSJYnIKFXVaem0HPXdjM0sbOwlCFzcPCm9fCUlMdExIIwBTACdTZzE8QKwAFzYiqvpIACYABn0ANla-MFzCcwa2loBaNoB2Zs77SAAbNmsAcwaC2xAUIA',COLUMNS={{dataSet:VARCHAR}})
  )
  SELECT
    json_extract(entries,'$.freq')[2:-2]::VARCHAR freq,
    json_extract(entries,'$.period')[2:-2]::VARCHAR period,
    json_extract(entries,'$.sv')[2:-2]::VARCHAR sv,
    json_extract(entries,'$.svDesc')[2:-2]::VARCHAR svDesc,
    json_extract(entries,'$.figure')::DOUBLE  figure,
    json_extract(entries,'$.sd_value')[2:-2]::VARCHAR sd_value,
  FROM json_rows
)
""")
con.sql(f"""
CREATE OR REPLACE TABLE tbl_raw_population_value AS(
  with json_rows as(
    SELECT unnest(from_json(json("dataSet"),'["JSON"]')) entries FROM
    read_json('https://www.censtatd.gov.hk/api/get.php?id=110-01001&lang=tc&param=N4KABGBEDGBukC4yghSBlAogDUWA2uKmgLKQA0RxkAYpFWALqXUCCA4pnocWgAwBaACwUGaAKwCAnKN5QAjIPkiWvSPMnyZq6gCZBulWKi7Ju7ccgBmQVaNzrkqxYdDBQ+2qGShLteMFxT2pxSXE-agA2QUjg1EhIyUiI+IB2QVS4tFTJVJS0AA5BAqyoAvEAanpiRiIAX1VIAGd4JBR4gAUAeQ7uSwAlAEMAdwB9AGlR+QATAAdR2YBTACdRgDtZNQ7lgHt5mfmAUnXq1FqIBqJIJeWASx3pvHa0JoAXQeXXvHUpSMV5U6XNC3R5IdSKAR8RRQzaQAA2gzWAHNvq9oPQ6kA',COLUMNS={{dataSet:VARCHAR}})
  )
  SELECT
    json_extract(entries,'$.SEX')[2:-2]::VARCHAR sex,
    json_extract(entries,'$.SEXDesc')[2:-2]::VARCHAR sex_desc,
    json_extract(entries,'$.AGE')[2:-2]::VARCHAR "age",
    json_extract(entries,'$.AGEDesc')[2:-2]::VARCHAR age_desc,
    json_extract(entries,'$.freq')::VARCHAR freq,
    json_extract(entries,'$.period')[2:-2]::VARCHAR period,
    json_extract(entries,'$.sv')[2:-2]::VARCHAR sv,
    json_extract(entries,'$.svDesc')[2:-2]::VARCHAR sv_desc,
    TRY_CAST(json_extract(entries,'$.figure')[0:]::VARCHAR AS DOUBLE) figure,
    json_extract(entries,'$.sd_value')[2:-2]::VARCHAR sd_value,
    json_keys(entries) ->>0
  FROM json_rows
)
""")


con.close()
 