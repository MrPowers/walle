# walle

Experimentation with big data file formats and compression algorithms.

## Setup

Run `poetry install` to create a virtual environment with all the project dependencies.

Run `poetry run pytest tests/` to run the test suite.  This'll create a bunch of files in the `tmp/` directory.

## Example results

The `writing_data1.csv` file contains accelerometer and gyroscope data for a student that was writing for a minute.  The data was [sourced here](https://data.mendeley.com/datasets/w3wsc359pc/1#folder-13c9d6e3-de3f-4911-aa6e-743729542888).

The file contains 92,491 rows of data that looks like this:

```
ID,Gender,Age,AccX,AccY,AccZ,GyroX,GyroY,GyroZ
1,1,22,-0.03879346865085720,-0.05386269936296160,-0.02593945533348080,0.05371093750000000,-0.58053588867187500,-0.88148498535156194
1,1,22,-0.03441603076709401,0.06479018194777469,-0.06279627323963240,0.03700256347656250,-0.54809570312500000,-0.79600524902343794
1,1,22,0.02176867875030770,-0.10951051560460300,-0.00587493336471480,0.06597900390625000,-0.56910705566406194,-0.87266540527343794
```

The uncompressed `writing_data1.csv` file is 12.1 MB.  This graph how the data file can be compressed with different file formats and compression algorithms.

![Initial benchmarks](https://github.com/MrPowers/walle/blob/master/images/benchmarks1.png)

