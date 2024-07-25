import duckdb
import pandas as pd
from datetime import datetime

t1 = datetime.now()
cursor = duckdb.connect()

df = cursor.execute("""
                        INSTALL httpfs;
                        LOAD httpfs;
                        SET s3_region='us-east-1';
                        SET s3_access_key_id='';
                        SET s3_secret_access_key='';
                        
                        CREATE TABLE data AS SELECT CAST(started_at as DATE) as started_at_date, count(ride_id) as ride_id_count
                        FROM read_csv_auto('s3://confessions-of-a-data-guy/*.csv')
                        GROUP BY started_at_date;
                    
                        COPY data TO 's3://confessions-of-a-data-guy/ducky-results.parquet';
                        """).df()
t2 = datetime.now()
print(f"Time taken: {t2 - t1}")
