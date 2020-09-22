import pytest

import pandas as pd
import numpy as np
from pathlib import Path
import pyarrow.parquet as pq


def test_brotli_compression():
    df = pd.DataFrame(
        np.random.randint(0, 10000000, size=(1000000, 4)), columns=["a", "b", "c", "d"]
    )

    df.astype("Int32").to_parquet("tmp/i32.brotli.parquet", compression="brotli")
    i32_size = int(Path("tmp/i32.brotli.parquet").stat().st_size)
    print(i32_size)
    print(pq.read_table(Path("tmp/i32.brotli.parquet")).schema)

    df.astype("Int64").to_parquet("tmp/i64.brotli.parquet", compression="brotli")
    i64_size = int(Path("tmp/i64.brotli.parquet").stat().st_size)
    print(i64_size)
    print(pq.read_table(Path("tmp/i64.brotli.parquet")).schema)

    df.astype("Int32").to_parquet("tmp/i32.snappy.parquet", compression="snappy")
    i32_size = int(Path("tmp/i32.snappy.parquet").stat().st_size)
    print(i32_size)
    print(pq.read_table(Path("tmp/i32.snappy.parquet")).schema)

    df.astype("Int64").to_parquet("tmp/i64.snappy.parquet", compression="snappy")
    i64_size = int(Path("tmp/i64.brotli.parquet").stat().st_size)
    print(i64_size)
    print(pq.read_table(Path("tmp/i64.snappy.parquet")).schema)


    # df.astype("Int64").to_parquet("/tmp/i64.parquet", compression="brotli")

    # i64_size = int(Path("/tmp/i64.parquet").stat().st_size)
