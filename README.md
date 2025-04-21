# PostgresPyLoad
Phase1 takes a CSV file describing the structure of a relational database, and creates a postgres database based on it and loads data into it. Then Phase2 does financial analysis on the inserted data.

# Python dependencies (Tested on Python 3.11.1)
- psycopg2
- glob
- pandas

# Instructions
This assumes there is already a PostgresSQL server running on localhost on the default port. The server just needs to be running, completely new out of the box. The scripts take care of the rest. There are database credential variables at the top of both Phase1.py and Phase2.py, and they are defaulted to username=postgres and password=admin. 

Phase1 must be successfully run at least once before Phase2 will work.

# Input
- ```input/INFORMATION_SCHEMA.csv```‎ —‎ Describes the database tables, their schema, their fields, and the data types of those fields.
- ```input/<TABLE_NAME>.csv describes```‎ —‎ The data of the respective table to load. The name of the file must match the TABLE_NAME in INFORMATION_SCHEMA.csv, and the headers of this file must match the fields described in INFORMATION_SCHEMA.csv.

Note: If two or more fields of the same table are described under two or more different schemas in INFORMATION_SCHEMA.csv, for example:

```
TABLE_SCHEMA, TABLE_NAME,    ORDINAL_POSITION, COLUMN_NAME,    DATA_TYPE
dbo,          CUSTOM_FIELDS, 2,                CUSTOM_TEXT,    VARCHAR
dbo,          CUSTOM_FIELDS, 0,                CUSTOM_GUID,    VARCHAR
dif,          CUSTOM_FIELDS, 1,                CUSTOM_DATE,    DATE
dif,          CUSTOM_FIELDS, 3,                CUSTOM_NUMERIC, "NUMERIC(38,4)"
```

The program handles this example of tables defined across multiple schemas, but uses the same CSV file for all schema versions of a table. It correctly filters columns for each schema version and loads the same records into each schema version from the fields that table schema supports. To properly support different numbers of records per schema, separate CSV files with schema-specific naming would be required. As it stands, the program does not support schema-specific naming of CSV files. I cannot control the format of the input files for this project, as it is a programming assessment for a job interview with strict input file naming conventions.
