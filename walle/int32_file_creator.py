import pandas as pd
import os


os.makedirs("tmp/int32_parquet", exist_ok=True)


def write_int32_snappy_parquet():
    df = pd.read_csv('data/writing_data1.csv')
    df['AccX_int32'] = df['AccX'] * 10_000
    df['AccY_int32'] = df['AccY'] * 10_000
    df['AccZ_int32'] = df['AccZ'] * 10_000
    df['GyroX_int32'] = df['GyroX'] * 10_000
    df['GyroY_int32'] = df['GyroY'] * 10_000
    df['GyroZ_int32'] = df['GyroZ'] * 10_000
    df2 = df.astype({'AccX_int32': 'int32', 'AccY_int32': 'int32', 'AccZ_int32': 'int32', 'GyroX_int32': 'int32', 'GyroY_int32': 'int32', 'GyroZ_int32': 'int32'})

    cols = ['ID', 'Gender', 'Age', 'AccX_int32', 'AccY_int32', 'AccZ_int32', 'GyroX_int32', 'GyroY_int32', 'GyroZ_int32']

    df2[cols].to_parquet('tmp/int32_parquet/prime_writing_data1.snappy.parquet', compression = 'snappy')

