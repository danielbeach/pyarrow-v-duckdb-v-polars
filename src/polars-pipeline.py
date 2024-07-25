import polars as pl
import pyarrow.dataset as ds
from datetime import datetime
import s3fs


def main():
    t1 = datetime.now()
    bucket = "confessions-of-a-data-guy"
    key = ""
    secret = ""

    fs = s3fs.S3FileSystem(key=key,
                           secret=secret,
                           config_kwargs={'region_name':'us-east-1'}
                                          )

    s3_endpoint = f"s3://{bucket}/"

    myds = ds.dataset([y for y in fs.ls(s3_endpoint) if ".csv" in y], 
                      filesystem=fs, 
                      format="csv")
    lazy_df = pl.scan_pyarrow_dataset(myds)

    lazy_df = lazy_df.groupby("started_at").agg(pl.count("ride_id").alias("ride_id_count"))

    with fs.open("s3://confessions-of-a-data-guy/harddrives/metrics-polars", "wb") as f:
        lazy_df.collect().write_parquet(f)
    
    t2 = datetime.now()
    print(f"Time taken: {t2 - t1}")


if __name__ == "__main__":
    main()