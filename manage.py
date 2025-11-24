import duckdb
import os

token = os.environ.get("MOTHERDUCK_TOKEN")
if not token:
	raise RuntimeError("MOTHERDUCK_TOKEN environment variable not set.")

# Connect to MotherDuck using the token
con = duckdb.connect(database='github_actions', config={'motherduck_token': token})
con.execute("CREATE OR REPLACE TABLE test_table AS SELECT 1 AS tester")
con.close()