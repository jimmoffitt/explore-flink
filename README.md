# explore-flink

This repository includes the following:

1) A simple producer that streams data from a Kaggle dataset capturing UK user behavior on Netflix browsing activity to Confluent Cloud. The data is serialized in AVRO format before streaming.

2) Example Apache Flink queries for preforming data analysis:

* Calculate the average watch duration for each movie title across all users.
* Analyze daily engagement patterns for each movie title.
* Calculate daily view counts and total watch time for each title to track how user interest fluctuates day by day.
 
