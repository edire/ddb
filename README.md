# Description

A Python library by Dire Analytics for custom data connections.

## Installation

pip install git+https://github.com/edire/ddb.git

## SQL

```python
import ddb

odbc = ddb.SQL(db='Test', server='localhost')

df = odbc.read("select * from dbo.tbl")

odbc.run("exec dbo.stp")

df.to_sql("tbl2", con=odbc.con, schema="dbo", index=False)
odbc.to_sql(df, "tbl2", schema="dbo", index=False)
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

MIT License

## Updates

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