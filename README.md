*SPH-YT-DATA*

The code base in Master Branch is a Data Engineering project to fetch youtube data.

Initial load will fetch data for provided specific list of channels.

Initial load is configurable to pass number of days , to fetch the historical data.

Incremental load will continue to fetch the latest data from last initial load.

It can be fully deployed on AWS stack,using event bridge , lambda , s3 and quicksight.

Just update the key , schedule it and its ready to ingest data from youtube in defined s3 buckets.
