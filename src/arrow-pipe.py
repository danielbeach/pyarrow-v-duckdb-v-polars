import pyarrow.dataset as ds
from pyarrow import fs
import pyarrow.compute as pc
import pyarrow.parquet as pq
from datetime import datetime


def read_remote_csvs(s3, path: str, key: str, secret: str):
    expr = pc.field("rideable_type") == 'electric_bike'
    dataset = ds.dataset(filesystem=s3, source=path, format="csv")
    return dataset.scanner(columns=["ride_id", "started_at"], filter=expr).to_table()


def main():
    t1 = datetime.now()
    path = "confessions-of-a-data-guy"
    key = ""
    secret = ""
    s3 = fs.S3FileSystem(access_key=key, 
                         secret_key=secret, 
                         request_timeout=1000, 
                         connect_timeout=1000,
                         region='us-east-1')
    table = read_remote_csvs(s3, path, key, secret)
    table = table.add_column(0, 'started_at_date', table.column('started_at').cast('date32[day]'))
    metrics = table.group_by('started_at_date').aggregate([('ride_id', 'count')])
    pq.write_to_dataset(metrics, root_path='confessions-of-a-data-guy/metrics', filesystem=s3)
    t2 = datetime.now()
    print(f"Time taken: {t2 - t1}")


if __name__ == "__main__":
    main()