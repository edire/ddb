# setup.py placed at root directory
from setuptools import setup
setup(
    name='ddb',
    version='0.2.4',
    author='Eric Di Re',
    description='Custom DB Data Connections.',
    url='https://github.com/edire/ddb.git',
    python_requires='>=3.9',
    packages=['ddb'],
    package_data={'ddb': ['MiscFiles/Microsoft.AnalysisServices.Tabular.DLL']},
    install_requires=['pyodbc', 'sqlalchemy', 'numpy', 'pandas', 'pymysql', 'pythonnet', 'google-cloud-bigquery']
)