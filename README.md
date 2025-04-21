# PostgresPyLoad
Phase1 takes a CSV file describing the structure of a relational database, and creates a postgres database based on it and loads data into it. Then Phase2 does financial analysis on the inserted data.

# Python dependencies (Tested on Python 3.11.1)
- psycopg2
- glob
- pandas

# Instructions
This assumes there is already a postgresSQL server running on loacalhost on the default port. The server just needs to be running, completely new out of the box. The scripts take care of the rest. There are database credential variables at the top of both Phase1.py and Phase2.py, and they are defaulted to username=postgres and password=admin. 

Phase1 must be successfully ran at least once before Phase2 will work.

# Input
- input/INFORMATION_SCHEMA.csv describes the database tables, their schema, their fields, and the data types of those fields.
- input/<TABLE_NAME>.csv describes the data of the respective table to load. The name of the file must match the TABLE_NAME in INFORMATION_SCHEMA.csv, and the headers of these files must match the fields described in INFORMATION_SCHEMA.csv
