# PySpark - Window Functions

## Introduction
#### Window functions in PySpark are a powerful feature for data manipulation and analysis. They allow us to perform complex calculations on subsets of data within a DataFrame, without the need for expensive joins or subqueries.we will use the window functions row_number(), rank() and dense_rank() of PySpark with a certain window specification.

## Window Functions
#### At its core, a window function performs a calculation on a set of rows. This "window" of rows is defined by a partition and an ordering within that partition. PySpark provides a rich set of window functions that can be applied to DataFrame columns.

## Define Window
#### Before applying window functions, it's essential to define a window specification. The window specification specifies how rows are partitioned and ordered within each partition.

#### In our example, we want to partition the data by the column "language" and order each partition in descending order based on the column "users".

## Applying Window Functions

### Window Function row_number()
#### The row_number() function assigns a unique sequential integer to each row within its partition.

### Window Function rank()
#### The rank() function computes the rank of each row within its partition based on the specified ordering.

### Window Function dense_rank()
#### The dense_rank() function computes the rank of each row within its partition based on the specified ordering, without skipping ranks for ties.

## Needs to ran following commands

#### Start the containers

```sh
docker-compose up -d
```

#### Ran the python file

```sh
docker-compose run pyspark python dataframe_spark.py
```
#### Stop the containers

```sh
docker-compose down
```
