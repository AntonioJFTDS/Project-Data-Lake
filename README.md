# Project description: 

The goal of the project is to extract the data from AWS S3 to an EMR cluster then create a set of fact/dimensional Spark dataFrame and finally to write those dataframe in AWS S3.

We know that every time a user of the music app plays a song, it is recorded on the json files within the folder `log_data` located in AWS S3 at `s3://udacity-dend/log_data`.
We also know that the information related to each song available on the music app is stored in the json files within the folder `song_data` located in AWS S3 at `s3://udacity-dend/song_data/A/A/A`.

# Database design: 

- **The dimension table `songs_table`:** each row holds the info relative to a song available at the music app.

- **The dimension table `artists_table`:** each row holds the info relative to an artist who create a song available at the music app.

- **The dimension table `time_table`:** each row holds the info relative to the instance when an user of the app played a song.

- **The dimension table `users_table`:** each row holds the info relative to an user of the app that has played at leat one or several songs.

- **The fact table `songplays_table`:** each row holds the info relative to when a song is played.



# ETL Process: 
The information related to each song available on the music app is stored in the json files within the folder `song_data`.
Each file of `song_data` refres to one specific song.
So the program will extract the information from the files in `song_data` to the Spark dataFrames `songs_table` and `artists_table`. The code does basically a copy/paste.

Every time an user of the music app plays a song, it is recorded on the json files within the folder `log_data`.
Each file of `log_data` refers to one specific day from November 2018.
So the program will extract the information from the files in `log_data` to the Spark dataFrames `time_table`, `users_table` and `songplays_table`
The code does basically a copy/paste. 

We note that in order to get `songplays_table` we need join the information from the files in `log_data` with the Spark dataframe `songs_table` (just created, see above).

# Project Repository files: 

**etl.py:** 
- Connect to the AWS EMR cluster 
- We load the data from AWS `s3://udacity-dend/song_data/A/A/A` to the AWS cluster and we create the Spark dataFrames `songs_table` and `artists_table`
- We rename and change the data type of columns of the Spark dataFrames `songs_table` and `artists_table`
- We write `songs_table` and `artists_table` in AWS S3
- We load the data from AWS `s3://udacity-dend/log_data` to the AWS cluster and we create the Spark dataFrames `time_table`, `users_table` and `songplays_table`
- We rename and change the data type of columns of the Spark dataFrames `time_table`, `users_table` and `songplays_table`
- Note that to create the Spark dataFrame `songplays_table`, we will have to join the information from AWS `s3://udacity-dend/log_data` with the Spark dataFrames  `songs_table` just recently created.
- We write `time_table`, `users_table` and `songplays_table` in AWS S3.

**dl.cfg:**
- contains the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to acces to AWS

# How To Run the Project: This describes the steps to run the project
- run `etl.py`
