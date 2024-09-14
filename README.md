# Project: Data Warehouse

## Quickstart
    * Create an AWS S3 bucket.
    * Copy log_data, song_data, log_json_path.json from s3://udacity-dend/song_data and s3://udacity-dend/log_data to your bucket
    * Edit dwh.cfg: add S3 bucket, add access key and secret key, add your ARN role, add your HOST (ENDPOINT).
    * Install Python + library, run python create_tables.py, python etl.py
    
## Dataset

    Dataset is a set of files in JSON format stored in AWS S3 buckets and contains two part:
    * Log_data: s3://udacity-dend/log_data : event data of service usage.
    * Song_data: s3://udacity-dend/song_data : Artists and songs data.
## Fact and Dimension & Star Schema
    * Fact table: 
        **songplays: (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    * Dimensions table:
        ** users: (user_id, first_name, last_name, gender, level)
        ** songs: (song_id, title, artist_id, year, duration)
        ** artists: (artist_id, name, location, latitude, longitude)
        ** time: (start_time, hour, day, week, month, year, weekday)
    ![Schema](https://github.com/nxavu2002/udacity-project-data-lakes/blob/main/Schema.png?raw=true)

## ETL pipelines

    1. Run create_tables.py to create staging, fact, dimension table.
    
    2. Run etl.py to load data to staging and insert data to fact, dimension tables.

## Summary
    Project is designed to build ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into fact table and dimension tables for their analytics team to continue finding insights into what songs their users are listening to.
