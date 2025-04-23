# explore-flink

This repository includes the following:

1) A simple producer that streams data from a Kaggle dataset capturing UK user behavior on Netflix browsing activity to Confluent Cloud. The data is serialized in AVRO format before streaming. There are three Python scripts in the `data-streamer` folder:
    * `data-streamer-json.py`: Writes simple JSON event objects.
    * `data-streamer-avro.py`: Serialized the data in the AVRO format before streaming it.
    * `data-streamer-avro-restart.py`: This version can continue from where it left off after restarting. This version writes a local file with the timestamp from the last event written to the stream. This can be useful when demoing a 'live' stream on top of data already processed.

2) Example Apache Flink queries for performing data analysis. These live in the `sql` folder:

* Calculate the average watch duration for each movie title across all users.
* Analyze daily engagement patterns for each movie title.
* Calculate daily view counts and total watch time for each title to track how user interest fluctuates day by day.
 
