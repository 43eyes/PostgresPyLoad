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

Note: If two fields of the same table are described under two different schemas in INFORMATION_SCHEMA.csv, for example:

```
TABLE_SCHEMA, TABLE_NAME,    ORDINAL_POSITION, COLUMN_NAME,    DATA_TYPE
dbo,          CUSTOM_FIELDS, 2,                CUSTOM_TEXT,    VARCHAR
dbo,          CUSTOM_FIELDS, 0,                CUSTOM_GUID,    VARCHAR
dif,          CUSTOM_FIELDS, 1,                CUSTOM_DATE,    DATE
dif,          CUSTOM_FIELDS, 3,                CUSTOM_NUMERIC, "NUMERIC(38,4)"
```

the program should handle this, just make sure that the single CSV file for the table contains all of the fields described in INFORMATION_SCHEMA.csv. However, trying to load a different number of records to the two tables of different schemas will not work. I do not have control over the format of the input files for this project. If I were to do it, I would have separate files for each schema/table pair, instead of just table.
