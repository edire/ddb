# setup.py placed at root directory
from setuptools import setup
setup(
    name='my-odbc-edire',
    version='0.0.6',
    author='Eric Di Re',
    description='Custom ODBC Data Connections.',
    url='https://github.com/edire/my_odbc.git',
    python_requires='>=3.6',
    packages=['my_odbc'],
    install_requires=['pyodbc', 'sqlalchemy', 'numpy', 'pandas']
)