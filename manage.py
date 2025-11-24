import duckdb
import os

token = os.getenv("MOTHERDUCK_TOKEN")
if not token:
	raise RuntimeError("MOTHERDUCK_TOKEN environment variable not set.")

# Connect to MotherDuck using the token
con = duckdb.connect(
    database='md:github_action',
    config = {'motherduck_token': token} )
con.sql("CREATE OR REPLACE TABLE github_action.test_table AS SELECT 1 AS tester")
con.close()