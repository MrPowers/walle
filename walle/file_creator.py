import pandas as pd


def write_gzipped_csv():
    df = pd.read_csv('data/writing_data1.csv')
    df.to_csv('tmp/writing_data1.csv.gz', compression='gzip')


def write_parquet():
    df = pd.read_csv('data/writing_data1.csv')
    df.to_parquet('tmp/writing_data1.parquet', compression = None)


def write_snappy_parquet():
    df = pd.read_csv('data/writing_data1.csv')
    df.to_parquet('tmp/writing_data1.snappy.parquet', compression = 'snappy')


def write_gzip_parquet():
    df = pd.read_csv('data/writing_data1.csv')
    df.to_parquet('tmp/writing_data1.gzip.parquet', compression = 'gzip')


def write_brotli_parquet():
    df = pd.read_csv('data/writing_data1.csv')
    df.to_parquet('tmp/writing_data1.brotli.parquet', compression = 'brotli')
