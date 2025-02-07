# Project description: 

The goal of the project is to extract the data from AWS S3 to an EMR cluster, then create a set of fact/dimensional Spark DataFrames, and finally write those DataFrames in AWS S3.

We know that every time a user of the music app plays a song, it is recorded in the JSON files within the folder `log_data` located in AWS S3 at `s3://udacity-dend/log_data`.
We also know that the information related to each song available on the music app is stored in the JSON files within the folder `song_data` located in AWS S3 at `s3://udacity-dend/song_data/A/A/A`.

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
