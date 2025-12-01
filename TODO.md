# TODO

## make precache.py more readable

Separate into 3 parts:

1. Periodic calling DATA.NIF.NO to get raw data

2. CHECK FOR CHANGES

3. PROCESS CHANGES


## make a different timers

1. gets data from DATA.NIF.NO every 3 minutes = PRIMARY RUN ( 3 minutes )
2. gets additional data from DATA.NIF.NO every 3 runs = SECONDARY RUN  ( 9 minutes )
3. gets additional data from DATA.NIF.NO every 300 runs = TERTIARY RUN ( 15 hours )

### PRIMARY RUN

- get basic data from DATA.NIF.NO
- store data in redis
- check for changes
- if changes, process changes ( PRIMARY RUN CHANGES )

### SECONDARY RUN

- get additional data from DATA.NIF.NO
- store data in redis
- check for changes
- if changes, process changes ( SECONDARY RUN CHANGES )

clubs data from DATA.NIF.NO every 3 runs = SECONDARY RUN ( 9 minutes )
team data from DATA.NIF.NO every 3 runs = SECONDARY RUN ( 9 minutes )

### TERTIARY RUN
- get additional data from DATA.NIF.NO
- store data in redis
- check for changes
- if changes, process changes ( TERTIARY RUN CHANGES )

get venues data from DATA.NIF.NO every 300 runs = TERTIARY RUN ( 15 hours )