import pandas as pd
import os


os.makedirs("tmp/csv", exist_ok=True)
os.makedirs("tmp/parquet", exist_ok=True)


def write_gzipped_csv():
    df = pd.read_csv('data/writing_data1.csv')
    df.to_csv('tmp/csv/writing_data1.csv.gz', compression='gzip')


def write_bz2_csv():
    df = pd.read_csv('data/writing_data1.csv')
    df.to_csv('tmp/csv/writing_data1.csv.bz2', compression='bz2')


def write_zip_csv():
    df = pd.read_csv('data/writing_data1.csv')
    df.to_csv('tmp/csv/writing_data1.csv.zip', compression='zip')


# def write_xz_csv():
    # df = pd.read_csv('data/writing_data1.csv')
    # df.to_csv('tmp/csv/writing_data1.csv.xz', compression='xz')


def write_parquet():
    df = pd.read_csv('data/writing_data1.csv')
    df.to_parquet('tmp/parquet/writing_data1.parquet', compression = None)


def write_snappy_parquet():
    df = pd.read_csv('data/writing_data1.csv')
    df.to_parquet('tmp/parquet/writing_data1.snappy.parquet', compression = 'snappy')


def write_gzip_parquet():
    df = pd.read_csv('data/writing_data1.csv')
    df.to_parquet('tmp/parquet/writing_data1.gzip.parquet', compression = 'gzip')


def write_brotli_parquet():
    df = pd.read_csv('data/writing_data1.csv')
    df.to_parquet('tmp/parquet/writing_data1.brotli.parquet', compression = 'brotli')
