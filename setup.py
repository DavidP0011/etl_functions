from setuptools import setup, find_packages
import pathlib

# Directorio actual del archivo setup.py
here = pathlib.Path(__file__).parent.resolve()
long_description = (here / "README.md").read_text(encoding="utf-8") if (here / "README.md").exists() else ""

setup(
    name='dpm_functions',
    version='0.1',
    description='Conjunto de funciones para procesos ETL',
    url='https://github.com/DavidP0011/etl_functions',
    author='David Plaza', 
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    python_requires='>=3.7, <4',
    install_requires=[
        "google-cloud-bigquery>=2.34.0",         # Conexión y consultas en BigQuery
        "google-cloud-secret-manager>=2.7.0",       # Acceso a Secret Manager para autenticación en GCP
        "google-cloud-translate>=3.0.0",           # Traducciones en funciones SQL y mapeo de países
        "google-cloud-storage>=2.8.0",             # Operaciones con Google Cloud Storage (dpm_GCP.py y dpm_GCS.py)
        "pandas>=1.3.0",                         # Manipulación de DataFrames
        "pandas-gbq>=0.14.0",                      # Carga de DataFrames a BigQuery
        "pycountry>=20.7.3",                       # Información de países para mapeos
        "rapidfuzz>=2.13.6",                       # Fuzzy matching en normalización y mapeo
        "phonenumbers>=8.12.39",                   # Procesamiento de números telefónicos (SQL_generate_country_from_phone)
        "boto3>=1.20.0",                           # Acceso a S3 (dpm_S3.py)
        "requests>=2.25.0",                        # Descarga de archivos y scraping (dpm_GCS.py, dpm_scrap.py)
        "beautifulsoup4>=4.9.0",                   # Parsing HTML para scraping (dpm_scrap.py)
        "dateparser>=1.0.0",                       # Análisis de fechas en dpm_SQL.py
        "rarfile>=4.0.0"                           # Soporte opcional para descompresión de archivos .rar (en dpm_GCS.py)
    ],
    entry_points={
        # Scripts ejecutables desde la línea de comando
        # 'console_scripts': [
        #     'nombre_comando=dpm_functions.modulo:funcion_principal',
        # ],
    },
)
