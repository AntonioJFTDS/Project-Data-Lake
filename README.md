# Project Description

The goal of this project is to process music streaming data from AWS S3 using Apache Spark on an EMR cluster. The process involves extracting JSON data, transforming it into structured Spark DataFrames, and storing the results in AWS S3 in **Parquet format**.

Each time a user plays a song on the music app, the event is logged in JSON files stored in the **`log_data`** folder at `s3://udacity-dend/log_data`. These logs capture user interactions and session details.

Additionally, metadata about each song available in the music app is stored in JSON files within the **`song_data`** folder at `s3://udacity-dend/song_data/A/A/A`. This dataset includes details about songs and artists.

The ETL script processes these datasets by:
- **Extracting** raw JSON data from S3.
- **Transforming** the data into structured tables for analysis.
- **Loading** the processed data back into S3 as **optimized Parquet files**, partitioned for efficiency.

The output consists of five structured tables: **songs, artists, users, time, and songplays**, ensuring efficient storage and query performance.

# Database design: 

- **The dimension table `songs_table`:** Each row holds the info relative to a song available on the music app.
- **The dimension table `artists_table`:** Each row holds the info relative to an artist who created a song available on the music app.
- **The dimension table `time_table`:** Each row holds the info relative to the instance when a user of the app played a song.
- **The dimension table `users_table`:** Each row holds the info relative to a user of the app that has played at least one or several songs.
- **The fact table `songplays_table`:** Each row holds the info relative to when a song is played.

# ETL Process: 
The information related to each song available on the music app is stored in the JSON files within the folder `song_data`.
Each file in `song_data` refers to one specific song.
So the program will extract the information from the files in `song_data` to the Spark DataFrames `songs_table` and `artists_table`. The code does basically a copy/paste.

Every time a user of the music app plays a song, it is recorded in the JSON files within the folder `log_data`.
Each file in `log_data` refers to one specific day from November 2018.
So the program will extract the information from the files in `log_data` to the Spark DataFrames `time_table`, `users_table`, and `songplays_table`.
The code does basically a copy/paste.

We note that in order to get `songplays_table`, we need to join the information from the files in `log_data` with the Spark DataFrame `songs_table` (just created, see above).

# Project Repository files: 

**etl.py:** 
- Connects to the AWS EMR cluster.
- Loads the data from AWS `s3://udacity-dend/song_data/A/A/A` to the AWS cluster and creates the Spark DataFrames `songs_table` and `artists_table`.
- Renames and changes the data type of columns of the Spark DataFrames `songs_table` and `artists_table`.
- Writes `songs_table` and `artists_table` to AWS S3.
- Loads the data from AWS `s3://udacity-dend/log_data` to the AWS cluster and creates the Spark DataFrames `time_table`, `users_table`, and `songplays_table`.
- Renames and changes the data type of columns of the Spark DataFrames `time_table`, `users_table`, and `songplays_table`.
- Note that to create the Spark DataFrame `songplays_table`, we will have to join the information from AWS `s3://udacity-dend/log_data` with the Spark DataFrame `songs_table` just recently created.
- Writes `time_table`, `users_table`, and `songplays_table` to AWS S3.

**dl.cfg:**
- Contains the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to access AWS.

# How To Run the Project:
This describes the steps to run the project:
- Run `etl.py`
