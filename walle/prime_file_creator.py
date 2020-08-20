import pandas as pd
import os


os.makedirs("tmp/prime_parquet", exist_ok=True)


def write_prime_snappy_parquet():
    df = pd.read_csv('data/writing_data1.csv')
    df['AccX_prime'] = df['AccX'] - df['AccX'].shift(1)
    df['AccY_prime'] = df['AccY'] - df['AccY'].shift(1)
    df['AccZ_prime'] = df['AccZ'] - df['AccZ'].shift(1)
    df['GyroX_prime'] = df['GyroX'] - df['GyroX'].shift(1)
    df['GyroY_prime'] = df['GyroY'] - df['GyroY'].shift(1)
    df['GyroZ_prime'] = df['GyroZ'] - df['GyroZ'].shift(1)

    cols = ['ID', 'Gender', 'Age', 'AccX_prime', 'AccY_prime', 'AccZ_prime', 'GyroX_prime', 'GyroY_prime', 'GyroZ_prime']

    df[cols].to_parquet('tmp/prime_parquet/prime_writing_data1.snappy.parquet', compression = 'snappy')


def write_double_prime_snappy_parquet():
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

    cols = ['ID', 'Gender', 'Age', 'AccX_dprime', 'AccY_dprime', 'AccZ_dprime', 'GyroX_dprime', 'GyroY_dprime', 'GyroZ_dprime']

    df[cols].to_parquet('tmp/prime_parquet/double_prime_writing_data1.snappy.parquet', compression = 'snappy')
