# doodleTest

To start streaming just run main.py

## What's done

* Read frames from kafka topic (doodle by default)
* Calculate number of unique uids per minute. 
We assume that frames come sorted by ts. If not, need to store sets of uids per minute in a distionary where key is timestamp rounded to minute. Then every frame's minute differs from previous it's checked if this minute already exists in the distionary.
If we need to aggregate by different time frames we would accumulate values in a few different lists or in a dictionary of lists. 
* Write result to kafka topic (results by default)
* Calculate how many frames the app process per second

