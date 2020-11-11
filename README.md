# Data Lakes with Spark
This is the forth project of Udacity's **Data Engineering Nanodegree**:mortar_board:.  
The purpose is to build an **ETL pipeline** that extracts data from `S3`, processes them using Spark, and load the data back into `S3(or hdfs)` as a set of dimensional tables.

## Background
A startup called :musical_note:*Sparkify* wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.  
The analytics team is particularly interested in understanding what songs users are listening to.  
Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.  

## File Description
- `etl.py` loads data from S3, process that data usign Spark, and writes them back to S3(or hdfs).
- `sample_parquet_data.ipynb` is a notebook to show sample data of each table in parquet file.

## Database Schema
![ERD](https://github.com/kjh7176/data_warehouse/blob/master/images/db_schema.PNG "ERD from https://dbdiagram.io/")
<table>
    <tr>
        <th>tablename</th>
        <th>tbl_rows</th>
        <th>sortkey1</th>
        <th>diststyle</th>
    </tr>
    <tr>
        <td>artists</td>
        <td>9553</td>
        <td>artist_id</td>
        <td>ALL</td>
    </tr>
    <tr>
        <td>songplays</td>
        <td>309</td>
        <td>start_time</td>
        <td>KEY(song_id)</td>
    </tr>
    <tr>
        <td>songs</td>
        <td>14896</td>
        <td>None</td>
        <td>KEY(song_id)</td>
    </tr>
    <tr>
        <td>time</td>
        <td>6813</td>
        <td>start_time</td>
        <td>ALL</td>
    </tr>
    <tr>
        <td>users</td>
        <td>96</td>
        <td>user_id</td>
        <td>ALL</td>
    </tr>
</table>

## Usage
> To use this, you need to set `output_data` path in `etl.py` to your own S3 or hdfs
 1. Copy
```
$ git clone https://github.com/kjh7176/data_lakes_with_spark

# change current working directory
$ cd data_lakes_with_spark
```

 2. Execute ETL process
```
$ python etl.py
```

 3. Test  
   Open `sample_parquet_data.ipynb` in order to check if data is inserted correctly.
