# TODO

## make precache.py more readable

Separate into 3 parts:

1. Periodic calling DATA.NIF.NO to get raw data

2. CHECK FOR CHANGES

3. PROCESS CHANGES

Currently, the code mixes these three concerns together, making it hard to follow.

## current cache size and endpoints should be same

## METRICS:

### Add metrics for pre-cache-calling with metrics for

-   total calls made
-   total failures
-   total time taken
-   number of changes detected - with regard to saved data.

### add metrics for redis-server

-   current memory usage
-   current number of keys
