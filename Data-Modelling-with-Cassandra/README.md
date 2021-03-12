# Project: Data Modeling with Cassandra


## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

## Objective
Create an Apache Cassandra database which can create queries on song play data, and build an ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

## Dataset
For this project, we'll be working with one dataset: event_data. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:</br></br>
```event_data/2018-11-08-events.csv```</br>
```event_data/2018-11-09-events.csv```
