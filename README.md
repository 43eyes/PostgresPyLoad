# PostgresPyLoad
Takes a CSV file describing the structure of a relational database, and creates a postgres database based on it and loads data into it.

# Python dependencies
- psycopg2
- glob
- pandas

# Instructions
This assumes there is already a postgresSQL server running on loacalhost on the default port. The server just needs to be running, completely new out of the box. The scripts take care of the rest. There are database credential variables at the top of both Phase1.py and Phase2.py, and they are defaulted to username=postgres and password=admin. 

Phase1 must be successfully ran at least once before Phase2 will work.
