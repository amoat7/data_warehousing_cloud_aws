<h1><center>Data Modeling with Postgres</center></h1>

<h2>Introduction</h2>
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

<h2> Datatsets </h2>
I am working with two datasets that reside in s3. 


<strong>Song Dataset</strong>

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 

<strong>Log Dataset </strong>

The second dataset consists of log files in JSON format generated by an event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.


<h2>Schema for Song Play Analysis</h2>

<strong>Facts Table</strong>

1. <strong>songplays</strong> - records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

<strong>Dimension Tables</strong>

2. <strong>users</strong> - users in the app
    - user_id, first_name, last_name, gender, level
3. <strong>songs</strong> - songs in music database
    - song_id, title, artist_id, year, duration
4. <strong>artists</strong> - artists in music database
    - artist_id, name, location, latitude, longitude
5. <strong>time</strong> - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday

<h2> File Structure </h2>

1. ```create_table.py```- creates the fact and dimension tables for the star schema in Redshift.

2. ```etl.py``` -  loads data from S3 into staging tables on Redshift and then process that data into the analytics tables on Redshift.
    
3. ```sql_queries.py``` -  defines  SQL statements, which is imported into the two other files above.
