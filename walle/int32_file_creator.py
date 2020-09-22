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

    df2[cols].to_parquet('tmp/int32_parquet/int32_writing_data1.snappy.parquet', compression = 'snappy')


def write_int32_prime_snappy_parquet():
    df = pd.read_csv('data/writing_data1.csv')
    df['AccX_int32_prime'] = (df['AccX'] - df['AccX'].shift(1)) * 10_000
    df['AccY_int32_prime'] = (df['AccY'] - df['AccY'].shift(1)) * 10_000
    df['AccZ_int32_prime'] = (df['AccZ'] - df['AccZ'].shift(1)) * 10_000
    df['GyroX_int32_prime'] = (df['GyroX'] - df['GyroX'].shift(1)) * 10_000
    df['GyroY_int32_prime'] = (df['GyroY'] - df['GyroY'].shift(1)) * 10_000
    df['GyroZ_int32_prime'] = (df['GyroZ'] - df['GyroZ'].shift(1)) * 10_000

    df = df.iloc[1:]

    df2 = df.astype({'AccX_int32_prime': 'int32', 'AccY_int32_prime': 'int32', 'AccZ_int32_prime': 'int32', 'GyroX_int32_prime': 'int32', 'GyroY_int32_prime': 'int32', 'GyroZ_int32_prime': 'int32'})

    cols = ['ID', 'Gender', 'Age', 'AccX_int32_prime', 'AccY_int32_prime', 'AccZ_int32_prime', 'GyroX_int32_prime', 'GyroY_int32_prime', 'GyroZ_int32_prime']

    df2[cols].to_parquet('tmp/int32_parquet/int32_prime_writing_data1.snappy.parquet', compression = 'snappy')


def write_int32_double_prime_snappy_parquet():
    df = pd.read_csv('data/writing_data1.csv')
    df['AccX_prime'] = df['AccX'] - df['AccX'].shift(1)
    df['AccY_prime'] = df['AccY'] - df['AccY'].shift(1)
    df['AccZ_prime'] = df['AccZ'] - df['AccZ'].shift(1)
    df['GyroX_prime'] = df['GyroX'] - df['GyroX'].shift(1)
    df['GyroY_prime'] = df['GyroY'] - df['GyroY'].shift(1)
    df['GyroZ_prime'] = df['GyroZ'] - df['GyroZ'].shift(1)

    df['AccX_dprime'] = df['AccX_prime'] - df['AccX_prime'].shift(1)
    df['AccY_dprime'] = df['AccY_prime'] - df['AccY_prime'].shift(1)
    df['AccZ_dprime'] = df['AccZ_prime'] - df['AccZ_prime'].shift(1)
    df['GyroX_dprime'] = df['GyroX_prime'] - df['GyroX_prime'].shift(1)
    df['GyroY_dprime'] = df['GyroY_prime'] - df['GyroY_prime'].shift(1)
    df['GyroZ_dprime'] = df['GyroZ_prime'] - df['GyroZ_prime'].shift(1)

    df = df.iloc[2:]

    df2 = df.astype({'AccX_dprime': 'int32', 'AccY_dprime': 'int32', 'AccZ_dprime': 'int32', 'GyroX_dprime': 'int32', 'GyroY_dprime': 'int32', 'GyroZ_dprime': 'int32'})

    cols = ['ID', 'Gender', 'Age', 'AccX_dprime', 'AccY_dprime', 'AccZ_dprime', 'GyroX_dprime', 'GyroY_dprime', 'GyroZ_dprime']

    df2[cols].to_parquet('tmp/int32_parquet/int32_dprime_writing_data1.snappy.parquet', compression = 'snappy')

