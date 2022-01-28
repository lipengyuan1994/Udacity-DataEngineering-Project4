
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, we will build an ETL pipeline for a data lake hosted on S3. We will load data from S3, process the data into analytics tables using Spark, and load them back into S3. We will deploy this Spark process on a cluster using AWS.


## Deployement

run the ipython notebook on EMR cluster.


## ETL Pipeline

1. Read data from S3

  * Song data: `s3://patrickudacityde/song_data/`
  * Log data: `s3://patrickudacityde/log_data/`

  The script reads song\_data and load\_data from S3.
2. Process data using spark

  Transforms them to create five different tables listed below :

#### Fact Table

  **songplays** - records in log data associated with song plays i.e. records with page `NextSong`

  * *songplay\_id, start\_time, user\_id, level, song\_id, artist\_id, session\_id, location, user\_agent*

#### Dimension Tables

  **users** - users in the app Fields - *user\_id, first\_name, last\_name, gender, level*

  **songs** - songs in music database Fields - *song\_id, title, artist\_id, year, duration*

  **artists** - artists in music database Fields - *artist\_id, name, location, lattitude, longitude*

  **time** - timestamps of records in **songplays** broken down into specific units Fields - *start\_time, hour, day, week, month, year, weekday*

3. Load it back to S3

  Writes them to partitioned parquet files in table directories on S3.


Example screenshots:

![](https://cdn.jsdelivr.net/gh/lipengyuan1994/image-host@master/uPic/2022012723425216433449721643344972402eA3dRRscreenshot_songplays.png)
