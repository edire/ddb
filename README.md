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

01/04/2023 - Added password parse option to MySQL.
12/31/2022 - Added SSAS module.
12/22/2022 - Added clean_string function to tools.
12/14/2022 - Added mysql module.