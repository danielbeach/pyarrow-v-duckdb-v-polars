## Polars vs Pyarrow vs DuckDB

This little repo shows a pipeline that ...

- reads multiple CSV files from s3
- does some aggregation
- writes results as parquet back to s3

There is a pipeline for  ...
- pyarrow
- polars
- duckdb

The idea is just to compare them, what the code looks like and what
the performance is like.

- pyarrow has strange syntax
- polars cannot read multipule CSV files from s3 without pyarrow, but is faster than pyarrow
- DuckDB has the best syntax, easiest to write, but is the slowest.

Checkout full blog https://www.confessionsofadataguy.com/pyarrow-vs-polars-vs-duckdb-for-data-pipelines/