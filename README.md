A continuación te propongo un ejemplo de README.md basado en el contenido y funcionalidad del repositorio:

---

# etl_functions

**etl_functions** es un paquete Python que ofrece un conjunto integral de funciones para construir pipelines ETL (Extracción, Transformación y Carga) de datos. Este repositorio facilita la integración y automatización de procesos de ETL en entornos cloud y locales, permitiendo conectarse a servicios como Google BigQuery, Google Cloud Storage, Amazon S3 y ejecutar consultas SQL, además de incorporar funciones de web scraping.

## Características

- **Conexión y carga en BigQuery:**  
  Funciones para ejecutar scripts SQL, extraer esquemas de tablas y cargar DataFrames en BigQuery.

- **Interacción con Google Cloud Storage:**  
  Funciones para descargar archivos desde la web, detectar y descomprimir archivos comprimidos, y subirlos a GCS.

- **Soporte para Amazon S3:**  
  Listado de archivos y carpetas en buckets de S3 para facilitar la gestión de datos en AWS.

- **Ejecución y generación de scripts SQL:**  
  Módulo con funciones que generan sentencias SQL dinámicas para diversas operaciones (por ejemplo, creación de vistas BI, limpieza de datos, generación de campos ordinales y normalización de cadenas).

- **Web scraping:**  
  Funciones especializadas para extraer datos de sitios web (por ejemplo, datos de Box Office Mojo), realizar transformaciones y cargar los resultados en BigQuery.

- **Autenticación flexible:**  
  Métodos de autenticación que se adaptan al entorno de ejecución, ya sea en entornos locales, Colab o directamente en GCP, mediante el uso de claves locales o la obtención de credenciales desde Secret Manager.

## Instalación

Puedes instalar el paquete directamente desde GitHub usando pip:

```bash
!pip install -q git+https://github.com/DavidP0011/etl_functions.git
```

O, si prefieres clonar el repositorio:

```bash
git clone https://github.com/DavidP0011/etl_functions.git
cd etl_functions
pip install .
```

## Requisitos

El paquete depende de varias librerías de terceros. Se recomienda que, al instalarlo, se verifiquen (o instalen) las siguientes dependencias:

```python
install_requires=[
    "google-cloud-bigquery>=2.34.0",
    "google-cloud-secretmanager>=2.7.0",
    "google-cloud-translate>=3.0.0",
    "google-cloud-storage>=2.8.0",
    "pandas>=1.3.0",
    "pandas-gbq>=0.14.0",
    "pycountry>=20.7.3",
    "rapidfuzz>=2.13.6",
    "phonenumbers>=8.12.39",
    "boto3>=1.20.0",
    "requests>=2.25.0",
    "beautifulsoup4>=4.9.0",
    "dateparser>=1.0.0",
    "rarfile>=4.0.0"
]
```

Estas dependencias se instalan automáticamente al instalar el paquete, siempre que el archivo `setup.py` esté correctamente configurado.

## Uso

El paquete se organiza en varios módulos, cada uno enfocado en un aspecto del proceso ETL. Por ejemplo:

- **dpm_GBQ.py:**  
  Contiene funciones para la interacción con BigQuery, como la ejecución de scripts SQL y la generación de sentencias dinámicas.

- **dpm_GCP.py:**  
  Funciones para extraer información del esquema de BigQuery y obtener metadatos de Google Cloud Storage.

- **dpm_GCS.py:**  
  Funciones para descargar archivos desde enlaces web, detectar si están comprimidos y subirlos a un bucket de GCS, así como para cargar archivos desde GCS a BigQuery.

- **dpm_S3.py:**  
  Función para listar la estructura de carpetas y archivos en un bucket de Amazon S3.

- **dpm_SQL.py:**  
  Funciones avanzadas para generar y ejecutar sentencias SQL en BigQuery, con soporte para transformaciones y mapeos.

- **dpm_scrap.py:**  
  Función para realizar web scraping (por ejemplo, para extraer datos de Box Office Mojo) y cargar los resultados en BigQuery.

### Ejemplo básico

Una vez instalado, puedes importar y utilizar las funciones según el módulo que necesites. Por ejemplo, para ejecutar un script SQL en BigQuery:

```python
from dpm_functions import dpm_GBQ

config = {
    "GCP_project_id": "mi-proyecto",
    "SQL_script": "SELECT * FROM `mi_dataset.mi_tabla` LIMIT 10",
    "json_keyfile_colab": "/ruta/a/mi/credencial.json",
    "ini_environment_identificated": "COLAB"
}

dpm_GBQ.GBQ_execute_SQL(config)
```

## Módulos y Funciones

Aquí se detalla brevemente cada módulo y algunas de sus funciones:

- **dpm_GBQ.py**  
  - `_ini_authenticate_API`: Autentica la conexión con GCP según el entorno.  
  - `GBQ_execute_SQL`: Ejecuta un script SQL en BigQuery y muestra detalles de la ejecución.  
  - `SQL_generate_academic_date_str`: Genera sentencias SQL para calcular fechas académicas o fiscales.  
  - ... (y otras funciones para generar vistas, limpieza y normalización de datos).

- **dpm_GCP.py**  
  - `_ini_authenticate_API`: Similar al de dpm_GBQ, para autenticación en GCP.  
  - `GBQ_tables_schema_df`: Extrae el esquema de tablas de BigQuery y lo devuelve como DataFrame.  
  - `GCS_tables_schema_df`: Extrae metadatos de buckets y objetos en Google Cloud Storage.

- **dpm_GCS.py**  
  - `GCS_web_download_links_to_bucket`: Descarga archivos desde URLs, descomprime si es necesario y los carga en un bucket de GCS.  
  - `GCS_files_to_GBQ`: Carga archivos desde GCS a BigQuery, aplicando filtros y procesamiento por chunks.

- **dpm_S3.py**  
  - `S3_folder_and_files_list`: Lista de forma recursiva la estructura de carpetas y archivos de un bucket S3.

- **dpm_SQL.py**  
  - Contiene funciones para generar y ejecutar scripts SQL que permiten transformar y cargar datos en BigQuery.

- **dpm_scrap.py**  
  - `box_office_mojo_to_GBQ`: Realiza web scraping en Box Office Mojo, añade identificadores IMDb y carga los datos en BigQuery.

## Contribuir

¡Las contribuciones son bienvenidas! Si deseas mejorar este paquete, por favor sigue estos pasos:

1. Haz un fork del repositorio.
2. Crea una rama con tus cambios: `git checkout -b feature/nueva-funcionalidad`
3. Realiza tus cambios y haz commit: `git commit -am 'Agrega nueva funcionalidad'`
4. Sube tus cambios a tu fork: `git push origin feature/nueva-funcionalidad`
5. Abre un Pull Request en este repositorio.

## Licencia

Este proyecto se distribuye bajo la [Licencia MIT](LICENSE).

## Contacto

Para cualquier consulta, sugerencia o reporte de errores, por favor abre un issue en GitHub o contacta al autor.
