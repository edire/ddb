# Description

A Python library by Dire Analytics for custom data connections.

## Installation

pip install git+https://github.com/edire/ddb.git

## SQL

```python
from ddb.sql import SQL

odbc = SQL(db='Test', server='localhost')

df = odbc.read("select * from dbo.tbl")

odbc.run("exec dbo.stp")

df.to_sql("tbl", con=odbc.con, schema="dbo", index=False)
odbc.to_sql(df, "tbl", schema="dbo", index=False)
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

MIT License

## Updates

10/04/2023 - Updated Bigquery data type mapping.<br>
09/19/2023 - Added port option for MySQL.<br>
08/12/2023 - Update for applymap deprecation and upper env vars.<br>
07/07/2023 - Reverted MSSQL and MySQL Run logic in order to pick up proc errors.<br>
06/29/2023 - Reverted MySQL engine to con.<br>
06/16/2023 - Updated SQL and MySQL modules for SQLAlchemy 2.0.<br>
04/23/2023 - Updated bigquery to remove string length restrictions.  Added pyarrow to required libraries for bigquery to_dataframe function.  Added db-dtypes to required libraries for bigquery.<br>
03/27/2023 - Updated clean column function to convert to string before cleaning.<br>
03/14/2023 - Updated data type amounts for float columns in sql and mysql.<br>
03/13/2023 - Added **kwargs to create_table function across all to eliminate error of passing missing variables.<br>
02/23/2023 - Updated class names for consistency.<br>
02/22/2023 - Fixed run logic in SQL and MySQL to use autocommit appropriately.  Removed parameters in SQL class.<br>
02/20/2023 - Updated bigquery module to allow connections from cloud resources.<br>
02/17/2023 - Updated MySQL for reading multiple statement queries into DataFrame.<br>
02/10/2023 - Added full functionality to BigQuery module.<br>
02/06/2023 - Updated MySQL connector to automatically password parse if necessary.<br>
01/31/2023 - Added option to not drop columns in clean function.<br>
01/09/2023 - Added openpyxl to dependencies on install.<br>
01/08/2023 - Fixed duplicate RowLoadDateTime issue in create_table function for sql and mysql.<br>
01/06/2023 - Added BigQuery module with read function.  Updated MySQL RowLoadDateTime for new and old MySQL server versions.<br>
01/05/2023 - Adjusted env variable default names for multiple connection types.<br>
01/04/2023 - Added password parse option to MySQL.<br>
12/31/2022 - Added SSAS module.<br>
12/22/2022 - Added clean_string function to tools.<br>
12/14/2022 - Added mysql module.