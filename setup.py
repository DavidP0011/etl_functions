from setuptools import setup, find_packages
import pathlib

# Directorio actual del archivo setup.py
here = pathlib.Path(__file__).parent.resolve()

# Si tienes un README (por ejemplo, README.md) en la raíz, lo usamos como long_description
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
        "google-cloud-bigquery>=2.34.0",
        "google-cloud-secretmanager>=2.7.0",
        "google-cloud-translate>=3.0.0",
        "pandas>=1.3.0",
        "pandas-gbq>=0.14.0",
        # "googletrans==4.0.0-rc1",
        "pycountry>=20.7.3",
        "rapidfuzz>=2.13.6",
        "phonenumbers>=8.12.39"
    ]

    entry_points={
        # Scripts ejecutables desde la línea de comando
        # 'console_scripts': [
        #     'nombre_comando=dpm_functions.modulo:funcion_principal',
        # ],
    },
)
