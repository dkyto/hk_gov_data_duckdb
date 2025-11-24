import duckdb
con = duckdb.connect(database=':memory:')
con.execute("SELECT 1 as tester")
con.close()