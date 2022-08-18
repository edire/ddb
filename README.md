# my_odbc

A Python library for custom data connections.

## Installation

pip install git+https://github.com/edire/my_odbc.git

## Usage

```python
import my_odbc

odbc = my_odbc.SQL(db='Test', server='localhost')

df = odbc.read("select * from dbo.tbl")

odbc.run("exec dbo.stp")

df.to_sql("tbl2", con=odbc.con, schema="dbo", index=False)
odbc.to_sql(df, "tbl2", schema="dbo", index=False)
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

MIT License

## Release Updates

Reverted back to original mssql "run" logic to pick up errors at any point during code execution.