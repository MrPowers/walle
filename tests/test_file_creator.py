import pytest

from walle.file_creator import *

def test_write_gzipped_csv():
    write_gzipped_csv()


def test_write_bz2_csv():
    write_bz2_csv()


def test_write_zip_csv():
    write_zip_csv()


# def test_write_xz_csv():
    # write_xz_csv()


def test_write_parquet():
    write_parquet()


def test_write_snappy_parquet():
    write_snappy_parquet()


def test_write_gzip_parquet():
    write_gzip_parquet()


def test_write_brotli_parquet():
    write_brotli_parquet()


