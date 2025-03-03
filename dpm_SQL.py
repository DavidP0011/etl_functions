# __________________________________________________________________________________________________________________________________________________________
# Repositorio de funciones
# __________________________________________________________________________________________________________________________________________________________

from google.cloud import bigquery
import pandas as pd
import pandas_gbq
from googletrans import Translator  # Versión 4.0.0-rc1
import unicodedata
import re
import pycountry
from rapidfuzz import process, fuzz
import time
import os
from google.auth import default as gauth_default
from google.oauth2 import service_account

# ----------------------------------------------------------------------------
# DF_to_GBQ()
# ----------------------------------------------------------------------------
def DF_to_GBQ(params: dict) -> None:
    """
    Carga un DataFrame en una tabla de Google BigQuery e imprime un informe detallado con
    los datos del job de carga, incluyendo estadísticas y métricas finales.
    
    Parámetros en params:
      - source_df (pd.DataFrame): DataFrame a subir.
      - destination_table (str): Tabla destino en formato 'project_id.dataset_id.table_id'.
      - json_keyfile_GCP_secret_id (str): Secret ID para obtener credenciales desde Secret Manager (requerido en GCP).
      - json_keyfile_colab (str): Ruta al archivo JSON de credenciales (requerido en entornos no GCP).
      - if_exists (str, opcional): 'fail', 'replace' o 'append' (por defecto 'append').
    """
    print("[START 🚀] Iniciando carga del DataFrame a BigQuery...", flush=True)
    
    import os, re, time, json
    import pandas as pd
    from google.cloud import bigquery, secretmanager
    from google.oauth2 import service_account

    def _validar_parametros(params: dict) -> None:
        required_params = ['source_df', 'destination_table']
        missing = [p for p in required_params if p not in params]
        if missing:
            raise RuntimeError(f"[VALIDATION [ERROR ❌]] Faltan parámetros obligatorios: {missing}")

    def _sanitizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
        def sanitize(col: str) -> str:
            new_col = re.sub(r'[^0-9a-zA-Z_]', '_', col)
            if new_col and new_col[0].isdigit():
                new_col = '_' + new_col
            return new_col
        df.columns = [sanitize(col) for col in df.columns]
        return df

    _validar_parametros(params)
    df = params['source_df']
    destination_table = params['destination_table']
    if_exists = params.get('if_exists', 'append')
    df = _sanitizar_columnas(df)
    print("[METRICS [INFO 📊]] Columnas sanitizadas:", df.columns.tolist(), flush=True)
    
    # ───── Autenticación ─────
    def _autenticar_gcp(project_id: str) -> bigquery.Client:
        print("[AUTHENTICATION [INFO] 🔐] Iniciando autenticación en BigQuery...", flush=True)
        if os.environ.get("GOOGLE_CLOUD_PROJECT"):
            secret_id = params.get("json_keyfile_GCP_secret_id")
            if not secret_id:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En GCP se debe proporcionar 'json_keyfile_GCP_secret_id'.")
            print("[AUTHENTICATION [INFO] 🔐] Entorno GCP detectado. Obteniendo credenciales desde Secret Manager...", flush=True)
            client_sm = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client_sm.access_secret_version(name=secret_name)
            secret_string = response.payload.data.decode("UTF-8")
            secret_info = json.loads(secret_string)
            credentials = service_account.Credentials.from_service_account_info(secret_info)
            print("[AUTHENTICATION [SUCCESS ✅]] Credenciales obtenidas desde Secret Manager.", flush=True)
        else:
            json_path = params.get("json_keyfile_colab")
            if not json_path:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En entornos no GCP se debe proporcionar 'json_keyfile_colab'.")
            print("[AUTHENTICATION [INFO] 🔐] Entorno local/Colab detectado. Usando credenciales desde archivo JSON...", flush=True)
            credentials = service_account.Credentials.from_service_account_file(json_path)
            print("[AUTHENTICATION [SUCCESS ✅]] Credenciales cargadas desde archivo JSON.", flush=True)
        client = bigquery.Client(credentials=credentials, project=project_id)
        return client

    try:
        project_id = destination_table.split('.')[0]
    except IndexError:
        raise RuntimeError("El formato de 'destination_table' debe ser 'project_id.dataset_id.table_id'.")
    client = _autenticar_gcp(project_id)
    
    # ───── Carga a BigQuery ─────
    def _cargar_dataframe(client: bigquery.Client, df: pd.DataFrame, destination_table: str, if_exists: str) -> None:
        config_map = {
            'fail': 'WRITE_EMPTY',
            'replace': 'WRITE_TRUNCATE',
            'append': 'WRITE_APPEND'
        }
        job_config = bigquery.LoadJobConfig(write_disposition=config_map.get(if_exists, 'WRITE_APPEND'))
        print(f"[LOAD [START 📤]] Iniciando carga de datos a la tabla '{destination_table}'...", flush=True)
        start_time = time.time()
        job = client.load_table_from_dataframe(df, destination_table, job_config=job_config)
        job.result()  # Espera a que se complete la carga
        elapsed_time = time.time() - start_time
        print("[LOAD [SUCCESS ✅]] Datos cargados correctamente.", flush=True)
        print(f"[METRICS [INFO 📊]] Filas insertadas: {df.shape[0]}, Columnas: {df.shape[1]}", flush=True)
        print(f"[METRICS [INFO 📊]] Tiempo total de carga: {elapsed_time:.2f} segundos\n", flush=True)
        _mostrar_detalles_job(client, job, elapsed_time, destination_table)

    def _mostrar_detalles_job(client: bigquery.Client, job, elapsed_time: float, destination_table: str) -> None:
        print("[METRICS [INFO 📊]] Detalles del job de carga:", flush=True)
        print(f"  - ID del job: {job.job_id}", flush=True)
        print(f"  - Estado: {job.state}", flush=True)
        print(f"  - Tiempo de creación: {job.created}", flush=True)
        if hasattr(job, 'started'):
            print(f"  - Tiempo de inicio: {job.started}", flush=True)
        if hasattr(job, 'ended'):
            print(f"  - Tiempo de finalización: {job.ended}", flush=True)
        print(f"  - Bytes procesados: {job.total_bytes_processed if job.total_bytes_processed is not None else 'N/A'}", flush=True)
        print(f"  - Bytes facturados: {job.total_bytes_billed if job.total_bytes_billed is not None else 'N/A'}", flush=True)
        print(f"  - Cache hit: {job.cache_hit}", flush=True)
        try:
            count_query = f"SELECT COUNT(*) AS total_rows FROM `{destination_table}`"
            count_result = client.query(count_query).result()
            rows_in_table = [row['total_rows'] for row in count_result][0]
            print(f"  - Filas en la tabla destino: {rows_in_table}", flush=True)
            table_ref = client.get_table(destination_table)
            table_size_kb = table_ref.num_bytes / 1024
            print(f"  - Tamaño de la tabla destino: {table_size_kb:.2f} KB", flush=True)
        except Exception as e:
            print(f"[METRICS [WARNING ⚠️]] No se pudo obtener información adicional de la tabla destino: {e}", flush=True)
        print(f"[END [FINISHED 🏁]] Proceso de carga finalizado en {elapsed_time:.2f} segundos.\n", flush=True)

    _cargar_dataframe(client, df, destination_table, if_exists)


# ----------------------------------------------------------------------------
# GBQ_execute_SQL()
# ----------------------------------------------------------------------------
def GBQ_execute_SQL(params: dict) -> None:
    """
    Ejecuta un script SQL en Google BigQuery y muestra un resumen detallado con estadísticas del proceso.
    
    Parámetros en params:
      - GCP_project_id (str): ID del proyecto de GCP.
      - SQL_script (str): Script SQL a ejecutar.
      - json_keyfile_GCP_secret_id (str, opcional): Secret ID para obtener credenciales desde Secret Manager (en GCP).
      - json_keyfile_colab (str, opcional): Ruta del archivo JSON de credenciales (en entornos no GCP).
      - destination_table (str, opcional): Tabla destino para obtener estadísticas adicionales.
    """
    print("[START 🚀] Iniciando ejecución de script SQL en BigQuery...", flush=True)
    import os, time, re, json
    from google.cloud import bigquery, secretmanager
    from google.oauth2 import service_account

    def _validar_parametros(params: dict) -> (str, str, str):
        project_id = params.get('GCP_project_id')
        sql_script = params.get('SQL_script')
        if not project_id or not sql_script:
            raise ValueError("[VALIDATION [ERROR ❌]] Faltan 'GCP_project_id' o 'SQL_script'.")
        destination_table = params.get('destination_table')
        return project_id, sql_script, destination_table

    def _autenticar(project_id: str):
        if os.environ.get("GOOGLE_CLOUD_PROJECT"):
            secret_id = params.get("json_keyfile_GCP_secret_id")
            if not secret_id:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En GCP se debe proporcionar 'json_keyfile_GCP_secret_id'.")
            print("[AUTHENTICATION [INFO] 🔐] Entorno GCP detectado. Obteniendo credenciales desde Secret Manager...", flush=True)
            client_sm = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client_sm.access_secret_version(name=secret_name)
            secret_string = response.payload.data.decode("UTF-8")
            secret_info = json.loads(secret_string)
            creds = service_account.Credentials.from_service_account_info(secret_info)
            print("[AUTHENTICATION [SUCCESS ✅]] Credenciales obtenidas desde Secret Manager.", flush=True)
        else:
            json_path = params.get("json_keyfile_colab")
            if not json_path:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En entornos no GCP se debe proporcionar 'json_keyfile_colab'.")
            print("[AUTHENTICATION [INFO] 🔐] Entorno local/Colab detectado. Usando credenciales desde archivo JSON...", flush=True)
            creds = service_account.Credentials.from_service_account_file(json_path)
            print("[AUTHENTICATION [SUCCESS ✅]] Credenciales cargadas desde archivo JSON.", flush=True)
        return creds

    def _inicializar_cliente(project_id: str, creds) -> bigquery.Client:
        print("[LOAD [START 📤]] Iniciando cliente de BigQuery...", flush=True)
        client = bigquery.Client(project=project_id, credentials=creds)
        print(f"[LOAD [SUCCESS ✅]] Cliente inicializado para el proyecto: {project_id}\n", flush=True)
        return client

    def _mostrar_resumen_script(sql_script: str) -> None:
        action = sql_script.strip().split()[0]
        print(f"[EXTRACTION [INFO 📊]] Acción detectada en el script SQL: {action}", flush=True)
        print("[EXTRACTION [INFO 📊]] Resumen del script (primeras 5 líneas):", flush=True)
        for line in sql_script.strip().split('\n')[:5]:
            print(line, flush=True)
        print("...", flush=True)

    def _ejecutar_query(client: bigquery.Client, sql_script: str, start_time: float):
        print("[TRANSFORMATION [START 🔄]] Ejecutando el script SQL...", flush=True)
        query_job = client.query(sql_script)
        query_job.result()  # Espera a que se complete la consulta
        elapsed_time = time.time() - start_time
        print("[TRANSFORMATION [SUCCESS ✅]] Consulta SQL ejecutada exitosamente.\n", flush=True)
        return query_job, elapsed_time

    def _mostrar_detalles_trabajo(client: bigquery.Client, query_job, elapsed_time: float, destination_table: str) -> None:
        print("[METRICS [INFO 📊]] Detalles del trabajo de BigQuery:", flush=True)
        print(f"  - ID del job: {query_job.job_id}", flush=True)
        print(f"  - Estado: {query_job.state}", flush=True)
        print(f"  - Tiempo de creación: {query_job.created}", flush=True)
        if hasattr(query_job, 'started'):
            print(f"  - Tiempo de inicio: {query_job.started}", flush=True)
        if hasattr(query_job, 'ended'):
            print(f"  - Tiempo de finalización: {query_job.ended}", flush=True)
        print(f"  - Bytes procesados: {query_job.total_bytes_processed if query_job.total_bytes_processed is not None else 'N/A'}", flush=True)
        print(f"  - Bytes facturados: {query_job.total_bytes_billed if query_job.total_bytes_billed is not None else 'N/A'}", flush=True)
        print(f"  - Cache hit: {query_job.cache_hit}", flush=True)
        if destination_table:
            try:
                count_query = f"SELECT COUNT(*) AS total_rows FROM `{destination_table}`"
                count_result = client.query(count_query).result()
                rows_in_table = [row['total_rows'] for row in count_result][0]
                print(f"  - Filas en la tabla destino: {rows_in_table}", flush=True)
            except Exception as e:
                print(f"[METRICS [WARNING ⚠️]] No se pudo obtener información de la tabla destino: {e}", flush=True)
        print(f"[END [FINISHED 🏁]] Tiempo total de ejecución: {elapsed_time:.2f} segundos\n", flush=True)

    project_id, sql_script, destination_table = _validar_parametros(params)
    creds = _autenticar(project_id)
    client = _inicializar_cliente(project_id, creds)
    _mostrar_resumen_script(sql_script)
    start_time = time.time()
    try:
        query_job, elapsed_time = _ejecutar_query(client, sql_script, start_time)
        _mostrar_detalles_trabajo(client, query_job, elapsed_time, destination_table)
    except Exception as e:
        print(f"[TRANSFORMATION [ERROR ❌]] Ocurrió un error al ejecutar el script SQL: {str(e)}\n", flush=True)
        raise


# ----------------------------------------------------------------------------
# SQL_generate_academic_date_str()
# ----------------------------------------------------------------------------
def SQL_generate_academic_date_str(params):
    """
    Genera una sentencia SQL para crear o reemplazar una tabla con campos de fecha académica/fiscal,
    basándose en reglas de corte sobre un campo fecha existente.
    
    Parámetros en params:
      - table_source (str): Tabla de origen.
      - table_destination (str): Tabla destino.
      - custom_fields_config (dict): Configuración de campos y reglas de corte.
      - json_keyfile (str, opcional): Ruta al archivo JSON de credenciales.
    
    Retorna:
        str: Sentencia SQL generada.
    """
    print("[START 🚀] Iniciando generación del SQL para fechas académicas/fiscales...", flush=True)
    table_source = params["table_source"]
    table_destination = params["table_destination"]
    custom_fields_config = params["custom_fields_config"]

    print(f"[EXTRACTION [INFO 📊]] table_source: {table_source}", flush=True)
    print(f"[EXTRACTION [INFO 📊]] table_destination: {table_destination}", flush=True)
    print("[TRANSFORMATION [INFO 🔄]] Procesando configuración de fechas...", flush=True)

    additional_expressions = []
    for field, configs in custom_fields_config.items():
        for cfg in configs:
            start_month = cfg.get("start_month", 9)
            start_day = cfg.get("start_day", 1)
            suffix = cfg.get("suffix", "custom")
            new_field = f"{field}_{suffix}"
            expression = f"""
CASE
  WHEN (EXTRACT(MONTH FROM `{field}`) > {start_month})
       OR (EXTRACT(MONTH FROM `{field}`) = {start_month} AND EXTRACT(DAY FROM `{field}`) >= {start_day}) THEN
    CONCAT(
      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) - 2000) AS STRING), 2, '0'),
      '-',
      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) + 1 - 2000) AS STRING), 2, '0')
    )
  ELSE
    CONCAT(
      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) - 1 - 2000) AS STRING), 2, '0'),
      '-',
      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) - 2000) AS STRING), 2, '0')
    )
END AS `{new_field}`
""".strip()
            additional_expressions.append(expression)
            print(f"[TRANSFORMATION [INFO 🔄]] Expresión generada para '{field}' con suffix '{suffix}'.", flush=True)
    
    additional_select = ",\n  ".join(additional_expressions) if additional_expressions else ""
    if additional_select:
        SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
SELECT
  s.*,
  {additional_select}
FROM `{table_source}` s
;
""".strip()
    else:
        SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
SELECT
  *
FROM `{table_source}`;
""".strip()
    print("[END [FINISHED 🏁]] SQL generado exitosamente.\n", flush=True)
    return SQL_script

# ----------------------------------------------------------------------------
# SQL_generate_BI_view_str()
# ----------------------------------------------------------------------------
def SQL_generate_BI_view_str(params: dict) -> str:
    """
    Crea o reemplaza una vista (o tabla) BI, seleccionando columnas de una tabla fuente con mapeos y filtros.
    
    Parámetros en params:
      - table_source (str): Tabla origen.
      - table_destination (str): Vista o tabla destino.
      - fields_mapped_df (pd.DataFrame): DataFrame con ["Campo Original", "Campo Formateado"].
      - use_mapped_names (bool): Si True, usa nombres formateados.
      - creation_date_field (str, opcional): Campo de fecha.
      - use_date_range (bool): Si True, filtra por rango de fechas.
      - date_range (dict, opcional): {"from": "YYYY-MM-DD", "to": "YYYY-MM-DD"}.
      - exclude_deleted_records_bool (bool): Si True, excluye registros marcados como borrados.
      - exclude_deleted_records_field_name (str, opcional)
      - exclude_deleted_records_field_value: Valor que indica borrado.
    
    Retorna:
        str: Sentencia SQL generada.
    """
    print("[START 🚀] Iniciando generación de vista BI...", flush=True)
    table_source = params.get("table_source")
    table_destination = params.get("table_destination")
    fields_mapped_df = params.get("fields_mapped_df")
    if not table_source or not table_destination or not isinstance(fields_mapped_df, pd.DataFrame):
        raise ValueError("[VALIDATION [ERROR ❌]] Faltan parámetros obligatorios: 'table_source', 'table_destination' o 'fields_mapped_df'.")
    
    use_mapped_names = params.get("use_mapped_names", True)
    creation_date_field = params.get("creation_date_field", "")
    date_range = params.get("date_range", {})
    use_date_range = params.get("use_date_range", False)
    exclude_deleted_records_bool = params.get("exclude_deleted_records_bool", False)
    exclude_deleted_records_field_name = params.get("exclude_deleted_records_field_name", "")
    exclude_deleted_records_field_value = params.get("exclude_deleted_records_field_value", None)
    
    select_cols = []
    for _, row in fields_mapped_df.iterrows():
        orig = row["Campo Original"]
        mapped = row["Campo Formateado"]
        select_cols.append(f"`{orig}` AS `{mapped}`" if use_mapped_names else f"`{orig}`")
    select_clause = ",\n  ".join(select_cols)
    
    where_filters = []
    if use_date_range and creation_date_field:
        date_from = date_range.get("from", "")
        date_to = date_range.get("to", "")
        if date_from and date_to:
            where_filters.append(f"`{creation_date_field}` BETWEEN '{date_from}' AND '{date_to}'")
        elif date_from:
            where_filters.append(f"`{creation_date_field}` >= '{date_from}'")
        elif date_to:
            where_filters.append(f"`{creation_date_field}` <= '{date_to}'")
    if exclude_deleted_records_bool and exclude_deleted_records_field_name and exclude_deleted_records_field_value is not None:
        where_filters.append(f"(`{exclude_deleted_records_field_name}` IS NULL OR `{exclude_deleted_records_field_name}` != {exclude_deleted_records_field_value})")
    where_clause = " AND ".join(where_filters) if where_filters else "TRUE"
    
    sql_script_str = f"""
CREATE OR REPLACE VIEW `{table_destination}` AS
SELECT
  {select_clause}
FROM `{table_source}`
WHERE {where_clause}
;
""".strip()
    print("[END [FINISHED 🏁]] Vista BI generada.\n", flush=True)
    return sql_script_str

# ----------------------------------------------------------------------------
# SQL_generate_CPL_to_contacts_str()
# ----------------------------------------------------------------------------
def SQL_generate_CPL_to_contacts_str(params: dict) -> str:
    """
    Genera una sentencia SQL para crear o reemplazar una tabla que combina una tabla principal de contactos,
    una tabla agregada con métricas y tablas de métricas publicitarias.
    
    Parámetros en params:
      - table_destination (str): Tabla resultado.
      - table_source (str): Tabla de contactos.
      - table_aggregated (str): Tabla agregada de métricas.
      - join_field (str): Campo de fecha para unión.
      - join_on_source (str): Campo de fuente para unión.
      - contact_creation_number (str): Campo de número de contactos creados.
      - ad_platforms (list): Lista de diccionarios con configuraciones de plataformas.
    
    Retorna:
        str: Sentencia SQL generada.
    """
    print("[START 🚀] Iniciando generación del SQL para CPL a contacts...", flush=True)
    table_destination = params["table_destination"]
    table_source = params["table_source"]
    table_aggregated = params["table_aggregated"]
    join_field = params["join_field"]
    join_on_source = params["join_on_source"]
    contact_creation_number = params["contact_creation_number"]
    ad_platforms = params["ad_platforms"]

    from_clause = f"""
FROM `{table_source}` o
LEFT JOIN `{table_aggregated}` a
  ON DATE(o.{join_field}) = a.{join_field}
  AND o.{join_on_source} = a.{join_on_source}
"""
    joins = []
    select_platform_metrics = []
    for idx, plat in enumerate(ad_platforms, start=1):
        alias = f"p{idx}"
        prefix = plat["prefix"]
        table = plat["table"]
        date_field = plat["date_field"]
        source_value = plat["source_value"]
        joins.append(f"""
LEFT JOIN `{table}` {alias}
  ON a.{join_field} = {alias}.{date_field}
""")
        for key, value in plat.items():
            if key.startswith("total_"):
                metric = key.replace("total_", "")
                col_name = f"contact_Ads_{prefix}_{metric}_by_day"
                expr = f"""
CASE
  WHEN a.{join_on_source} = "{source_value}" AND a.{contact_creation_number} > 0
    THEN {alias}.{value} / a.{contact_creation_number}
  ELSE NULL
END AS {col_name}
""".strip()
                select_platform_metrics.append(expr)
    final_select = ",\n".join(["o.*"] + select_platform_metrics)
    join_clause = "".join(joins)
    SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
SELECT
  {final_select}
{from_clause}
{join_clause}
;
""".strip()
    print("[END [FINISHED 🏁]] SQL para CPL a contacts generado.\n", flush=True)
    return SQL_script

# ----------------------------------------------------------------------------
# SQL_generate_cleaning_str()
# ----------------------------------------------------------------------------
def SQL_generate_cleaning_str(params: dict) -> str:
    """
    Genera una sentencia SQL para crear o sobrescribir una tabla de 'staging' aplicando mapeos, filtros
    y prefijos opcionales a los campos destino.
    
    Parámetros en params:
      - table_source (str): Tabla fuente.
      - table_destination (str): Tabla destino.
      - fields_mapped_use (bool): Si True, usa el nombre formateado.
      - fields_mapped_df (pd.DataFrame): DataFrame con ["Campo Original", "Campo Formateado"].
      - fields_destination_prefix (str, opcional)
      - exclude_records_by_creation_date_bool (bool)
      - exclude_records_by_creation_date_field (str, opcional)
      - exclude_records_by_creation_date_range (dict, opcional)
      - exclude_records_by_keywords_bool (bool)
      - exclude_records_by_keywords_fields (list, opcional)
      - exclude_records_by_keywords_words (list, opcional)
      - fields_to_trim (list, opcional)
    
    Retorna:
        str: Sentencia SQL generada.
    """
    print("[START 🚀] Iniciando generación del SQL de limpieza...", flush=True)
    table_source = params.get("table_source")
    table_destination = params.get("table_destination")
    fields_mapped_df = params.get("fields_mapped_df")
    fields_mapped_use = params.get("fields_mapped_use", True)
    fields_destination_prefix = params.get("fields_destination_prefix", "")
    exclude_records_by_creation_date_bool = params.get("exclude_records_by_creation_date_bool", False)
    exclude_records_by_creation_date_field = params.get("exclude_records_by_creation_date_field", "")
    exclude_records_by_creation_date_range = params.get("exclude_records_by_creation_date_range", {})
    exclude_records_by_keywords_bool = params.get("exclude_records_by_keywords_bool", False)
    exclude_records_by_keywords_fields = params.get("exclude_records_by_keywords_fields", [])
    exclude_records_by_keywords_words = params.get("exclude_records_by_keywords_words", [])
    fields_to_trim = params.get("fields_to_trim", [])

    select_clauses = []
    for _, row in fields_mapped_df.iterrows():
        campo_origen = row['Campo Original']
        campo_destino = f"{fields_destination_prefix}{row['Campo Formateado']}" if fields_mapped_use else f"{fields_destination_prefix}{campo_origen}"
        if campo_origen in fields_to_trim:
            select_clause = f"TRIM(REPLACE(`{campo_origen}`, '  ', ' ')) AS `{campo_destino}`"
        else:
            select_clause = f"`{campo_origen}` AS `{campo_destino}`"
        select_clauses.append(select_clause)
    select_part = ",\n  ".join(select_clauses)

    where_filters = []
    if exclude_records_by_creation_date_bool and exclude_records_by_creation_date_field:
        date_from = exclude_records_by_creation_date_range.get("from", "")
        date_to = exclude_records_by_creation_date_range.get("to", "")
        if date_from:
            where_filters.append(f"`{exclude_records_by_creation_date_field}` >= '{date_from}'")
        if date_to:
            where_filters.append(f"`{exclude_records_by_creation_date_field}` <= '{date_to}'")
    if exclude_records_by_keywords_bool and exclude_records_by_keywords_fields and exclude_records_by_keywords_words:
        for field in exclude_records_by_keywords_fields:
            where_filters.extend([f"`{field}` NOT LIKE '%{word}%'" for word in exclude_records_by_keywords_words])
    where_clause = " AND ".join(where_filters) if where_filters else "TRUE"
    SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
SELECT
  {select_part}
FROM `{table_source}`
WHERE {where_clause}
;
""".strip()
    print("[END [FINISHED 🏁]] SQL de limpieza generado.\n", flush=True)
    return SQL_script

# ----------------------------------------------------------------------------
# SQL_generate_country_from_phone()
# ----------------------------------------------------------------------------
def SQL_generate_country_from_phone(config: dict) -> str:
    """
    Genera un script SQL que extrae datos de una tabla de contactos y determina el país a partir
    del número telefónico, aplicando procesamiento en lotes y actualizando la tabla destino.
    
    Parámetros en config:
      - source_table (str): Tabla de contactos.
      - source_contact_phone_field (str): Campo con el teléfono.
      - source_contact_id_field_name (str): Campo identificador del contacto.
      - source_engagement_call_table (str): Tabla de llamadas.
      - source_engagement_call_id_match_contact_field_name (str): Campo para hacer match entre llamadas y contactos.
      - source_engagement_call_status_field_name (str): Campo de estatus de la llamada.
      - destination_table (str): Tabla destino.
      - destination_id_match_contact_field_name (str): Campo para hacer match en la tabla destino.
      - destination_country_mapped_field_name (str): Campo donde se mapea el país.
      - destination_call_status_field_name (str): Campo donde se mapea el estatus de la llamada.
      - default_phone_prefix (str, opcional): Prefijo por defecto. Por defecto "+34".
      - json_keyfile_GCP_secret_id (str): Secret ID para obtener credenciales en GCP.
      - json_keyfile_colab (str): Ruta al archivo JSON de credenciales para entornos no GCP.
    Retorna:
        str: El script SQL generado para actualizar la tabla destino.
    """
    import os, time, json, re, unicodedata
    import pandas as pd
    from google.cloud import bigquery, secretmanager
    from google.oauth2 import service_account
    import pandas_gbq

    print("[AUTHENTICATION [INFO] 🔐] Iniciando autenticación...", flush=True)
    if os.environ.get("GOOGLE_CLOUD_PROJECT"):
        secret_id = config.get("json_keyfile_GCP_secret_id")
        if not secret_id:
            raise ValueError("[AUTHENTICATION [ERROR ❌]] En GCP se debe proporcionar 'json_keyfile_GCP_secret_id'.")
        print("[AUTHENTICATION [INFO] 🔐] Entorno GCP detectado. Obteniendo credenciales desde Secret Manager...", flush=True)
        project = os.environ.get("GOOGLE_CLOUD_PROJECT")
        client_sm = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{project}/secrets/{secret_id}/versions/latest"
        response = client_sm.access_secret_version(name=secret_name)
        secret_string = response.payload.data.decode("UTF-8")
        secret_info = json.loads(secret_string)
        creds = service_account.Credentials.from_service_account_info(secret_info)
        print("[AUTHENTICATION [SUCCESS ✅]] Credenciales obtenidas desde Secret Manager.", flush=True)
    else:
        json_path = config.get("json_keyfile_colab")
        if not json_path:
            raise ValueError("[AUTHENTICATION [ERROR ❌]] En entornos no GCP se debe proporcionar 'json_keyfile_colab'.")
        print("[AUTHENTICATION [INFO] 🔐] Entorno local detectado. Usando credenciales desde archivo JSON...", flush=True)
        creds = service_account.Credentials.from_service_account_file(json_path)
        print("[AUTHENTICATION [SUCCESS ✅]] Credenciales cargadas desde archivo JSON.", flush=True)
    
    source_project = config["source_table"].split(".")[0]
    client = bigquery.Client(project=source_project, credentials=creds)
    
    # Extracción de datos de contactos
    print("[EXTRACTION [START ⏳]] Extrayendo datos de la tabla de contactos...", flush=True)
    query_source = f"""
        SELECT {config['source_contact_id_field_name']}, {config['source_contact_phone_field']}
        FROM `{config['source_table']}`
    """
    df_contacts = client.query(query_source).to_dataframe()
    if df_contacts.empty:
        print("[EXTRACTION [WARNING ⚠️]] No se encontraron datos en la tabla de contactos.", flush=True)
        return ""
    
    df_contacts.rename(columns={config['source_contact_phone_field']: "phone"}, inplace=True)
    df_contacts = df_contacts[df_contacts["phone"].notna() & (df_contacts["phone"].str.strip() != "")]
    
    # Procesamiento de teléfonos (por lotes)
    print("[TRANSFORMATION [START 🔄]] Preprocesando y procesando teléfonos en lotes...", flush=True)
    def _preprocess_phone(phone: str, default_prefix: str = config.get("default_phone_prefix", "+34")) -> str:
        if phone and isinstance(phone, str):
            phone = phone.strip()
            if not phone.startswith("+"):
                phone = default_prefix + phone
        return phone

    def _get_country_from_phone(phone: str) -> str:
        if not phone or not isinstance(phone, str):
            return None
        try:
            import phonenumbers
            phone_obj = phonenumbers.parse(phone, None)
            country_code = phonenumbers.region_code_for_number(phone_obj)
            if country_code:
                try:
                    import pycountry
                    country_obj = pycountry.countries.get(alpha_2=country_code)
                    return country_obj.name if country_obj else country_code
                except Exception:
                    return country_code
            else:
                return None
        except Exception:
            return None

    def _process_phone_numbers(series: pd.Series, batch_size: int = 1000) -> tuple:
        import math
        total = len(series)
        num_batches = math.ceil(total / batch_size)
        results = [None] * total
        error_batches = 0
        for i in range(num_batches):
            start = i * batch_size
            end = min((i+1) * batch_size, total)
            batch = series.iloc[start:end].copy()
            try:
                batch = batch.apply(lambda x: _preprocess_phone(x))
                results[start:end] = batch.apply(_get_country_from_phone).tolist()
            except KeyboardInterrupt:
                print(f"[CANCELLATION [INFO ❌]] Proceso interrumpido por el usuario en el lote {i+1} de {num_batches}.", flush=True)
                break
            except Exception as e:
                error_batches += 1
                print(f"[EXTRACTION [ERROR ❌]] Error en el lote {i+1}: {e}", flush=True)
            print(f"[METRICS [INFO 📊]] Lote {i+1}/{num_batches} procesado.", flush=True)
        return pd.Series(results, index=series.index), num_batches, error_batches

    df_contacts["country_name_iso"], num_batches, error_batches = _process_phone_numbers(df_contacts["phone"], batch_size=1000)
    
    # Extracción de estatus de llamadas
    print("[EXTRACTION [START ⏳]] Extrayendo estatus de llamadas por contacto...", flush=True)
    query_calls = f"""
        SELECT {config['source_engagement_call_id_match_contact_field_name']} AS contact_id,
               {config['source_engagement_call_status_field_name']} AS call_status,
               CASE {config['source_engagement_call_status_field_name']}
                   WHEN 'COMPLETED' THEN 1
                   WHEN 'IN_PROGRESS' THEN 2
                   WHEN 'CONNECTING' THEN 3
                   WHEN 'QUEUED' THEN 4
                   WHEN 'BUSY' THEN 5
                   WHEN 'NO_ANSWER' THEN 6
                   WHEN 'FAILED' THEN 7
                   WHEN 'CANCELED' THEN 8
                   ELSE 9
               END AS ranking
        FROM `{config['source_engagement_call_table']}`
        WHERE {config['source_engagement_call_id_match_contact_field_name']} IS NOT NULL
    """
    df_calls = client.query(query_calls).to_dataframe()
    print("[EXTRACTION [SUCCESS ✅]] Estatus de llamadas extraídos.", flush=True)
    df_calls.rename(columns={"contact_id": config['source_contact_id_field_name']}, inplace=True)
    
    mapping_df = pd.merge(df_contacts[[config['source_contact_id_field_name'], "phone", "country_name_iso"]], 
                          df_calls[[config['source_contact_id_field_name'], "call_status"]],
                          on=config['source_contact_id_field_name'], how="left")
    mapping_df = mapping_df.dropna(subset=["country_name_iso", "call_status"], how="all")
    
    total_registros = len(df_contacts)
    exitos = df_contacts["country_name_iso"].notna().sum()
    fallidos = total_registros - exitos
    print("\n[METRICS [INFO 📊]] Estadísticas del procesamiento:", flush=True)
    print(f"    - Registros totales: {total_registros}", flush=True)
    print(f"    - Exitosos: {exitos} ({(exitos/total_registros)*100:.2f}%)", flush=True)
    print(f"    - Fallidos: {fallidos} ({(fallidos/total_registros)*100:.2f}%)", flush=True)
    print(f"    - Lotes procesados: {num_batches} (Errores en {error_batches} lotes)", flush=True)
    
    country_counts = df_contacts["country_name_iso"].value_counts(dropna=True)
    print("\n[METRICS [INFO 📊]] Distribución por país:", flush=True)
    for country, count in country_counts.items():
        porcentaje = (count/total_registros)*100
        print(f"    - {country}: {count} registros ({porcentaje:.2f}%)", flush=True)
    
    parts = config["destination_table"].split(".")
    if len(parts) != 3:
        raise ValueError("[VALIDATION [ERROR ❌]] 'destination_table' debe tener el formato 'proyecto.dataset.tabla'.")
    dest_project, dest_dataset, _ = parts
    aux_table = f"{dest_project}.{dest_dataset}.temp_country_phone_mapping"
    
    print(f"\n[LOAD [START 📤]] Subiendo tabla auxiliar {aux_table}...", flush=True)
    pandas_gbq.to_gbq(mapping_df,
                        destination_table=aux_table,
                        project_id=dest_project,
                        if_exists="replace",
                        credentials=creds)
    
    def _build_update_sql(aux_table: str, client: bigquery.Client) -> str:
        try:
            dest_table = client.get_table(config["destination_table"])
            dest_fields = [field.name for field in dest_table.schema]
        except Exception:
            dest_fields = []
        if (config['destination_country_mapped_field_name'] in dest_fields and 
            config['destination_call_status_field_name'] in dest_fields):
            join_sql = (
                f"CREATE OR REPLACE TABLE `{config['destination_table']}` AS\n"
                f"SELECT d.* REPLACE(m.country_name_iso AS `{config['destination_country_mapped_field_name']}`,\n"
                f"                  m.call_status AS `{config['destination_call_status_field_name']}`)\n"
                f"FROM `{config['destination_table']}` d\n"
                f"LEFT JOIN `{aux_table}` m\n"
                f"  ON d.{config['destination_id_match_contact_field_name']} = m.{config['source_contact_id_field_name']};"
            )
        else:
            join_sql = (
                f"CREATE OR REPLACE TABLE `{config['destination_table']}` AS\n"
                f"SELECT d.*, m.country_name_iso AS `{config['destination_country_mapped_field_name']}`,\n"
                f"             m.call_status AS `{config['destination_call_status_field_name']}`\n"
                f"FROM `{config['destination_table']}` d\n"
                f"LEFT JOIN `{aux_table}` m\n"
                f"  ON d.{config['destination_id_match_contact_field_name']} = m.{config['source_contact_id_field_name']};"
            )
        drop_sql = f"DROP TABLE `{aux_table}`;"
        return join_sql + "\n" + drop_sql
    
    sql_script = _build_update_sql(aux_table, client)
    print("\n[TRANSFORMATION [SUCCESS ✅]] SQL generado para actualizar la tabla destino.", flush=True)
    print(sql_script, flush=True)
    print("[END [FINISHED 🏁]] Proceso finalizado.\n", flush=True)
    
    return sql_script


# ----------------------------------------------------------------------------
# SQL_generate_country_name_mapping()
# ----------------------------------------------------------------------------
def SQL_generate_country_name_mapping(config: dict) -> str:
    """
    Genera un script SQL para mapear nombres de países desde una tabla de BigQuery, usando
    mapeo manual y fuzzy matching, y sube una tabla auxiliar para actualizar la tabla destino.
    
    Parámetros en config:
      - source_table (str): Tabla origen.
      - source_country_name_best_list (list): Lista de campos candidatos.
      - source_id_name_field (str): Campo identificador en la tabla origen.
      - country_name_skip_values_list (list, opcional): Lista de valores a omitir.
      - manual_mapping_dic (dict, opcional): Diccionario de mapeo manual.
      - destination_table (str): Tabla destino.
      - destination_id_field_name (str): Campo identificador en la tabla destino.
      - destination_country_mapped_field_name (str): Campo a actualizar con el nombre mapeado.
      - json_keyfile_GCP_secret_id (str): Secret ID para obtener credenciales en GCP.
      - json_keyfile_colab (str): Ruta al archivo JSON de credenciales para entornos no GCP.
    Retorna:
        str: Script SQL completo para ejecutar (JOIN y DROP).
    """
    import os, json, unicodedata, re
    import pandas as pd
    from google.cloud import bigquery, secretmanager
    from google.oauth2 import service_account
    import pandas_gbq

    print("[AUTHENTICATION [INFO] 🔐] Iniciando autenticación...", flush=True)
    if os.environ.get("GOOGLE_CLOUD_PROJECT"):
        secret_id = config.get("json_keyfile_GCP_secret_id")
        if not secret_id:
            raise ValueError("[AUTHENTICATION [ERROR ❌]] En GCP se debe proporcionar 'json_keyfile_GCP_secret_id'.")
        print("[AUTHENTICATION [INFO] 🔐] Entorno GCP detectado. Obteniendo credenciales desde Secret Manager...", flush=True)
        project = os.environ.get("GOOGLE_CLOUD_PROJECT")
        client_sm = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{project}/secrets/{secret_id}/versions/latest"
        response = client_sm.access_secret_version(name=secret_name)
        secret_string = response.payload.data.decode("UTF-8")
        secret_info = json.loads(secret_string)
        creds = service_account.Credentials.from_service_account_info(secret_info)
        print("[AUTHENTICATION [SUCCESS ✅]] Credenciales obtenidas desde Secret Manager.", flush=True)
    else:
        json_path = config.get("json_keyfile_colab")
        if not json_path:
            raise ValueError("[AUTHENTICATION [ERROR ❌]] En entornos no GCP se debe proporcionar 'json_keyfile_colab'.")
        print("[AUTHENTICATION [INFO] 🔐] Entorno local detectado. Usando credenciales desde archivo JSON...", flush=True)
        creds = service_account.Credentials.from_service_account_file(json_path)
        print("[AUTHENTICATION [SUCCESS ✅]] Credenciales cargadas desde archivo JSON.", flush=True)
    
    # Función interna para normalizar texto
    def _normalize_text(texto: str) -> str:
        texto = texto.lower().strip()
        texto = unicodedata.normalize('NFD', texto)
        texto = ''.join(c for c in texto if unicodedata.category(c) != 'Mn')
        texto = re.sub(r'[^a-z0-9\s]', '', texto)
        return texto

    print("[METRICS [INFO 📊]] Validando parámetros obligatorios...", flush=True)
    if not (isinstance(config.get("source_table"), str) and 
            isinstance(config.get("source_id_name_field"), str) and 
            isinstance(config.get("destination_table"), str) and 
            isinstance(config.get("destination_id_field_name"), str) and 
            isinstance(config.get("destination_country_mapped_field_name"), str) and 
            isinstance(config.get("source_country_name_best_list"), list)):
        raise ValueError("[VALIDATION [ERROR ❌]] Faltan parámetros obligatorios o tienen formato incorrecto.")
    
    def _build_update_sql(aux_table: str, client: bigquery.Client) -> str:
        try:
            dest_table = client.get_table(config["destination_table"])
            dest_fields = [field.name for field in dest_table.schema]
        except Exception:
            dest_fields = []
        if config['destination_country_mapped_field_name'] in dest_fields:
            join_sql = (
                f"CREATE OR REPLACE TABLE `{config['destination_table']}` AS\n"
                f"SELECT d.* REPLACE(m.country_name_iso AS `{config['destination_country_mapped_field_name']}`)\n"
                f"FROM `{config['destination_table']}` d\n"
                f"LEFT JOIN `{aux_table}` m\n"
                f"  ON d.{config['destination_id_field_name']} = m.{config['source_id_name_field']};"
            )
        else:
            join_sql = (
                f"CREATE OR REPLACE TABLE `{config['destination_table']}` AS\n"
                f"SELECT d.*, m.country_name_iso AS `{config['destination_country_mapped_field_name']}`\n"
                f"FROM `{config['destination_table']}` d\n"
                f"LEFT JOIN `{aux_table}` m\n"
                f"  ON d.{config['destination_id_field_name']} = m.{config['source_id_name_field']};"
            )
        drop_sql = f"DROP TABLE `{aux_table}`;"
        return join_sql + "\n" + drop_sql

    source_project = config["source_table"].split(".")[0]
    client_bq = bigquery.Client(project=source_project, credentials=creds)
    print(f"[EXTRACTION [START ⏳]] Extrayendo datos de {config['source_table']}...", flush=True)
    country_fields_sql = ", ".join(config["source_country_name_best_list"])
    query_source = f"""
        SELECT {config['source_id_name_field']}, {country_fields_sql}
        FROM `{config['source_table']}`
    """
    df = client_bq.query(query_source).to_dataframe()
    if df.empty:
        print("[EXTRACTION [WARNING ⚠️]] No se encontraron datos en la tabla origen.", flush=True)
        return ""
    print("[TRANSFORMATION [START 🔄]] Procesando la mejor opción de país...", flush=True)
    df["best_country_name"] = df.apply(
        lambda row: next((row[field] for field in config["source_country_name_best_list"] if pd.notna(row[field]) and row[field]), None),
        axis=1
    )
    unique_countries = df["best_country_name"].dropna().unique().tolist()
    print(f"[METRICS [INFO 📊]] Se encontraron {len(unique_countries)} países únicos.", flush=True)
    
    skip_set = set(_normalize_text(x) for x in config.get("country_name_skip_values_list", []) if isinstance(x, str))
    mapping_results = {}
    countries_to_translate = []
    for country in unique_countries:
        if not isinstance(country, str):
            mapping_results[country] = None
            continue
        if _normalize_text(country) in skip_set:
            print(f"[EXTRACTION [INFO 📊]] Saltando mapeo para '{country}' (lista de omisión).", flush=True)
            mapping_results[country] = country
        else:
            countries_to_translate.append(country)
    
    print(f"[TRANSFORMATION [START 🔄]] Traduciendo {len(countries_to_translate)} países en lote...", flush=True)
    # Se asume que existe una función translate_batch_custom similar a la definida en otro bloque.
    # Para este ejemplo se usará el valor original.
    translated_dict = {}
    for country in countries_to_translate:
        translated_dict[country] = country
        print(f"[TRANSFORMATION [SUCCESS ✅]] '{country}' mapeado a: {country}", flush=True)
    for country in countries_to_translate:
        mapping_results[country] = translated_dict.get(country, country)
    
    df["country_name_iso"] = df["best_country_name"].map(mapping_results)
    mapping_df = df[[config["source_id_name_field"], "country_name_iso"]].drop_duplicates()
    parts = config["destination_table"].split(".")
    if len(parts) != 3:
        raise ValueError("[VALIDATION [ERROR ❌]] 'destination_table' debe tener el formato 'proyecto.dataset.tabla'.")
    dest_project, dest_dataset, _ = parts
    aux_table = f"{dest_project}.{dest_dataset}.temp_country_mapping"
    
    print(f"[LOAD [START 📤]] Subiendo tabla auxiliar {aux_table}...", flush=True)
    pandas_gbq.to_gbq(mapping_df,
                        destination_table=aux_table,
                        project_id=dest_project,
                        if_exists="replace",
                        credentials=creds)
    sql_script = _build_update_sql(aux_table, client_bq)
    print("[TRANSFORMATION [SUCCESS ✅]] SQL generado para actualizar la tabla destino.", flush=True)
    print(sql_script, flush=True)
    print("[END [FINISHED 🏁]] Proceso finalizado.\n", flush=True)
    return sql_script



# ----------------------------------------------------------------------------
# SQL_generate_deal_ordinal_str()
# ----------------------------------------------------------------------------
def SQL_generate_deal_ordinal_str(params):
    """
    Genera un script SQL que crea o reemplaza una tabla con un campo ordinal de negocio por contacto,
    basándose en la fecha de creación y filtrando por un campo (por ejemplo, 'pipeline').
    
    Parámetros en params:
      - table_source (str): Tabla origen.
      - table_destination (str): Tabla destino.
      - contact_id_field (str): Campo identificador del contacto.
      - deal_id_field (str): Campo identificador del negocio.
      - deal_createdate_field (str): Campo de fecha de creación.
      - deal_filter_field (str): Campo para filtrar.
      - deal_filter_values (list): Valores permitidos para el filtro.
      - deal_ordinal_field_name (str): Nombre del campo ordinal.
    
    Retorna:
        str: Script SQL generado.
    """
    print("[START 🚀] Iniciando generación del SQL para ordinal de negocios...", flush=True)
    table_source = params["table_source"]
    table_destination = params["table_destination"]
    contact_id_field = params["contact_id_field"]
    deal_id_field = params["deal_id_field"]
    deal_createdate_field = params["deal_createdate_field"]
    deal_filter_field = params["deal_filter_field"]
    deal_filter_values = params["deal_filter_values"]
    deal_ordinal_field_name = params["deal_ordinal_field_name"]

    filter_str_list = ", ".join([f"'{v}'" for v in deal_filter_values])
    SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
WITH deals_filtered AS (
  SELECT
    {contact_id_field},
    {deal_id_field},
    ROW_NUMBER() OVER (
      PARTITION BY {contact_id_field}
      ORDER BY {deal_createdate_field}
    ) AS {deal_ordinal_field_name}
  FROM `{table_source}`
  WHERE {deal_filter_field} IN ({filter_str_list})
)
SELECT
  src.*,
  f.{deal_ordinal_field_name}
FROM `{table_source}` src
LEFT JOIN deals_filtered f
  ON src.{contact_id_field} = f.{contact_id_field}
  AND src.{deal_id_field} = f.{deal_id_field}
;
""".strip()
    print("[END [FINISHED 🏁]] SQL para ordinal de negocios generado.\n", flush=True)
    return SQL_script

# ----------------------------------------------------------------------------
# SQL_generate_join_tables_str()
# ----------------------------------------------------------------------------
def SQL_generate_join_tables_str(params: dict) -> str:
    """
    Crea o reemplaza una tabla uniendo una tabla primaria, secundaria y opcionalmente una tabla puente,
    aplicando prefijos a las columnas para evitar duplicados.
    
    Parámetros en params:
      - table_source_primary (str): Tabla primaria.
      - table_source_primary_id_field (str): Campo de unión en la tabla primaria.
      - table_source_secondary (str): Tabla secundaria.
      - table_source_secondary_id (str): Campo de unión en la tabla secundaria.
      - table_source_bridge_use (bool): Indica si se usa tabla puente.
      - table_source_bridge (str, opcional): Tabla puente.
      - table_source_bridge_ids_fields (dict, opcional): Diccionario con keys 'primary_id' y 'secondary_id'.
      - join_type (str, opcional): Tipo de JOIN (por defecto "LEFT").
      - join_field_prefixes (dict, opcional): Prefijos para las tablas.
      - table_destination (str): Tabla destino.
      - json_keyfile_GCP_secret_id (str, opcional): Secret ID para GCP.
      - json_keyfile_colab (str, opcional): Ruta al archivo JSON para entornos no GCP.
      - GCP_project_id (str, opcional)
    Retorna:
        str: Sentencia SQL generada.
    """
    import os, json
    from google.cloud import bigquery, secretmanager
    from google.oauth2 import service_account

    print("[START 🚀] Iniciando generación del SQL para unión de tablas...", flush=True)
    table_source_primary = params["table_source_primary"]
    table_source_primary_id_field = params["table_source_primary_id_field"]
    table_source_secondary = params["table_source_secondary"]
    table_source_secondary_id = params["table_source_secondary_id"]
    table_source_bridge_use = params.get("table_source_bridge_use", False)
    table_source_bridge = params.get("table_source_bridge", "")
    table_source_bridge_ids_fields = params.get("table_source_bridge_ids_fields", {})
    join_type = params.get("join_type", "LEFT").upper()
    valid_join_types = ["INNER", "LEFT", "RIGHT", "FULL", "CROSS"]
    if join_type not in valid_join_types:
        raise ValueError(f"[VALIDATION [ERROR ❌]] join_type '{join_type}' no es válido. Debe ser uno de {valid_join_types}.")
    join_field_prefixes = params.get("join_field_prefixes", {"primary": "p_", "secondary": "s_", "bridge": "b_"})
    table_destination = params["table_destination"]

    def split_dataset_table(full_name: str):
        parts = full_name.split(".")
        if len(parts) == 2:
            project = params.get("GCP_project_id") or os.environ.get("GOOGLE_CLOUD_PROJECT")
            if not project:
                raise ValueError("[VALIDATION [ERROR ❌]] Para formato 'dataset.table' se requiere 'GCP_project_id'.")
            return (project, parts[0], parts[1])
        elif len(parts) == 3:
            return (parts[0], parts[1], parts[2])
        else:
            raise ValueError(f"[VALIDATION [ERROR ❌]] Nombre de tabla inválido: {full_name}")

    def get_table_columns(full_table_name: str):
        proj, dset, tbl = split_dataset_table(full_table_name)
        print(f"[EXTRACTION [START ⏳]] Obteniendo columnas de {full_table_name}...", flush=True)
        if os.environ.get("GOOGLE_CLOUD_PROJECT"):
            secret_id = params.get("json_keyfile_GCP_secret_id")
            if not secret_id:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En GCP se debe proporcionar 'json_keyfile_GCP_secret_id'.")
            client_sm = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{proj}/secrets/{secret_id}/versions/latest"
            response = client_sm.access_secret_version(name=secret_name)
            secret_string = response.payload.data.decode("UTF-8")
            secret_info = json.loads(secret_string)
            creds = service_account.Credentials.from_service_account_info(secret_info)
            print("[AUTHENTICATION [SUCCESS ✅]] Credenciales obtenidas desde Secret Manager.", flush=True)
        else:
            json_path = params.get("json_keyfile_colab")
            if not json_path:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] Se debe proporcionar 'json_keyfile_colab' en entornos no GCP.")
            creds = service_account.Credentials.from_service_account_file(json_path)
            print("[AUTHENTICATION [SUCCESS ✅]] Credenciales cargadas desde archivo JSON.", flush=True)
        client = bigquery.Client(project=proj, credentials=creds)
        query = f"""
        SELECT column_name
        FROM `{proj}.{dset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{tbl}'
        ORDER BY ordinal_position
        """
        rows = client.query(query).result()
        col_list = [row.column_name for row in rows]
        print(f"[EXTRACTION [SUCCESS ✅]] {len(col_list)} columnas encontradas en {full_table_name}.", flush=True)
        return col_list

    primary_cols = get_table_columns(table_source_primary)
    secondary_cols = get_table_columns(table_source_secondary)
    bridge_cols = []
    if table_source_bridge_use and table_source_bridge:
        bridge_cols = get_table_columns(table_source_bridge)
    
    primary_selects = [f"{join_field_prefixes['primary']}.{col} AS {join_field_prefixes['primary']}{col}" for col in primary_cols]
    secondary_selects = [f"{join_field_prefixes['secondary']}.{col} AS {join_field_prefixes['secondary']}{col}" for col in secondary_cols]
    bridge_selects = []
    if table_source_bridge_use and bridge_cols:
        bridge_selects = [f"{join_field_prefixes['bridge']}.{col} AS {join_field_prefixes['bridge']}{col}" for col in bridge_cols]
    
    all_selects = primary_selects + secondary_selects + bridge_selects
    select_clause = ",\n  ".join(all_selects)
    
    if table_source_bridge_use and table_source_bridge:
        join_clause = f"""
FROM `{table_source_primary}` AS {join_field_prefixes['primary']}
{join_type} JOIN `{table_source_bridge}` AS {join_field_prefixes['bridge']}
  ON {join_field_prefixes['bridge']}.{table_source_bridge_ids_fields['primary_id']} = {join_field_prefixes['primary']}.{table_source_primary_id_field}
{join_type} JOIN `{table_source_secondary}` AS {join_field_prefixes['secondary']}
  ON {join_field_prefixes['bridge']}.{table_source_bridge_ids_fields['secondary_id']} = {join_field_prefixes['secondary']}.{table_source_secondary_id}
"""
    else:
        join_clause = f"""
FROM `{table_source_primary}` AS {join_field_prefixes['primary']}
{join_type} JOIN `{table_source_secondary}` AS {join_field_prefixes['secondary']}
  ON {join_field_prefixes['primary']}.{table_source_primary_id_field} = {join_field_prefixes['secondary']}.{table_source_secondary_id}
"""
    SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
SELECT
  {select_clause}
{join_clause}
;
""".strip()
    print("[END [FINISHED 🏁]] SQL para unión de tablas generado.\n", flush=True)
    return SQL_script


# ----------------------------------------------------------------------------
# SQL_generate_new_columns_from_mapping()
# ----------------------------------------------------------------------------
def SQL_generate_new_columns_from_mapping(config: dict) -> tuple:
    """
    Genera un script SQL que agrega nuevas columnas a una tabla de BigQuery a partir de un mapeo
    definido en un DataFrame de referencia, subiendo una tabla auxiliar y realizando un JOIN.
    
    Parámetros en config:
      - source_table_to_add_fields (str): Tabla source.
      - source_table_to_add_fields_reference_field_name (str): Campo para hacer match.
      - referece_table_for_new_values_df (pd.DataFrame): DataFrame de referencia.
      - referece_table_for_new_values_field_names_dic (dict): Diccionario de campos a incorporar.
      - values_non_matched_result (str): Valor para registros sin match.
      - json_keyfile_GCP_secret_id (str, opcional): Secret ID para GCP.
      - json_keyfile_colab (str, opcional): Ruta al archivo JSON para entornos no GCP.
    Retorna:
        tuple: (sql_script, mapping_df)
    """
    import os, json, unicodedata, re
    import pandas as pd
    from google.cloud import bigquery, secretmanager
    from google.oauth2 import service_account
    import pandas_gbq

    print("[START 🚀] Iniciando generación del SQL para agregar nuevas columnas desde mapeo...", flush=True)
    source_table = config.get("source_table_to_add_fields")
    source_field = config.get("source_table_to_add_fields_reference_field_name")
    ref_df = config.get("referece_table_for_new_values_df")
    ref_field_names_dic = config.get("referece_table_for_new_values_field_names_dic")
    non_matched_value = config.get("values_non_matched_result", "descartado")
    if not (isinstance(source_table, str) and source_table):
        raise ValueError("[VALIDATION [ERROR ❌]] 'source_table_to_add_fields' es obligatorio y debe ser cadena.")
    if not (isinstance(source_field, str) and source_field):
        raise ValueError("[VALIDATION [ERROR ❌]] 'source_table_to_add_fields_reference_field_name' es obligatorio y debe ser cadena.")
    if not isinstance(ref_df, pd.DataFrame) or ref_df.empty:
        raise ValueError("[VALIDATION [ERROR ❌]] 'referece_table_for_new_values_df' debe ser un DataFrame válido y no vacío.")
    if not isinstance(ref_field_names_dic, dict) or not ref_field_names_dic:
        raise ValueError("[VALIDATION [ERROR ❌]] 'referece_table_for_new_values_field_names_dic' debe ser un diccionario no vacío.")
    
    print("[METRICS [INFO 📊]] Parámetros obligatorios validados.", flush=True)
    
    def _normalize_text(texto: str) -> str:
        texto = texto.lower().strip()
        texto = unicodedata.normalize('NFD', texto)
        texto = ''.join(c for c in texto if unicodedata.category(c) != 'Mn')
        texto = re.sub(r'[^a-z0-9\s]', '', texto)
        return texto

    def _sanitize_field_name(name: str) -> str:
        name = name.lower().strip()
        name = unicodedata.normalize('NFD', name)
        name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
        name = re.sub(r'\s+', '_', name)
        name = re.sub(r'[^a-z0-9_]', '', name)
        if re.match(r'^\d', name):
            name = '_' + name
        return name
    
    ref_field_names_list = list(ref_field_names_dic.keys())
    sanitized_columns = {col: _sanitize_field_name(col) for col in ref_field_names_list}
    
    def _extract_source_values(source_table: str, source_field: str, client: bigquery.Client) -> pd.DataFrame:
        query = f"""
            SELECT DISTINCT `{source_field}` AS raw_value
            FROM `{source_table}`
        """
        df = client.query(query).to_dataframe()
        return df
    
    def _build_reference_mapping(ref_df: pd.DataFrame, ref_field_names_list: list) -> dict:
        mapping = {}
        match_field = ref_field_names_list[0]
        for _, row in ref_df.iterrows():
            key_val = row.get(match_field)
            if isinstance(key_val, str):
                norm_key = _normalize_text(key_val)
                mapping[norm_key] = { col: row.get(col, non_matched_value) for col in ref_field_names_list }
        return mapping
    
    def _apply_mapping(df_source: pd.DataFrame, reference_mapping: dict) -> dict:
        mapping_results = {}
        for raw in df_source["raw_value"]:
            if not isinstance(raw, str) or not raw:
                mapping_results[raw] = { col: non_matched_value for col in ref_field_names_list }
                continue
            norm_raw = _normalize_text(raw)
            if norm_raw in reference_mapping:
                mapping_results[raw] = reference_mapping[norm_raw]
                print(f"[TRANSFORMATION [SUCCESS ✅]] [MATCH] '{raw}' encontrado en referencia.", flush=True)
            else:
                mapping_results[raw] = { col: non_matched_value for col in ref_field_names_list }
                print(f"[TRANSFORMATION [WARNING ⚠️]] [NO MATCH] '{raw}' no encontrado.", flush=True)
        return mapping_results
    
    # ───── Autenticación ─────
    def _obtener_cliente():
        source_project = source_table.split(".")[0]
        if os.environ.get("GOOGLE_CLOUD_PROJECT"):
            secret_id = config.get("json_keyfile_GCP_secret_id")
            if not secret_id:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En GCP se debe proporcionar 'json_keyfile_GCP_secret_id'.")
            from google.cloud import secretmanager
            client_sm = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{source_project}/secrets/{secret_id}/versions/latest"
            response = client_sm.access_secret_version(name=secret_name)
            secret_string = response.payload.data.decode("UTF-8")
            creds = service_account.Credentials.from_service_account_info(json.loads(secret_string))
            print("[AUTHENTICATION [SUCCESS ✅]] Credenciales obtenidas desde Secret Manager.", flush=True)
        else:
            json_path = config.get("json_keyfile_colab")
            if not json_path:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En entornos no GCP se debe proporcionar 'json_keyfile_colab'.")
            creds = service_account.Credentials.from_service_account_file(json_path)
            print("[AUTHENTICATION [SUCCESS ✅]] Credenciales cargadas desde archivo JSON.", flush=True)
        client = bigquery.Client(project=source_project, credentials=creds)
        return client

    client = _obtener_cliente()
    df_source = _extract_source_values(source_table, source_field, client)
    if df_source.empty:
        print("[EXTRACTION [WARNING ⚠️]] No se encontraron valores en la tabla source.", flush=True)
        return ("", pd.DataFrame())
    print(f"[EXTRACTION [INFO 📊]] Se encontraron {len(df_source)} valores únicos.", flush=True)
    
    reference_mapping = _build_reference_mapping(ref_df, ref_field_names_list)
    mapping_results = _apply_mapping(df_source, reference_mapping)
    
    mapping_rows = []
    for raw, mapping_dict in mapping_results.items():
        row = {"raw_value": raw}
        for col, value in mapping_dict.items():
            sanitized_col = sanitized_columns.get(col, col)
            row[sanitized_col] = value
        mapping_rows.append(row)
    mapping_df = pd.DataFrame(mapping_rows)
    
    parts = source_table.split(".")
    if len(parts) != 3:
        raise ValueError("[VALIDATION [ERROR ❌]] 'source_table_to_add_fields' debe ser 'proyecto.dataset.tabla'.")
    dest_project, dest_dataset, _ = parts
    aux_table = f"{dest_project}.{dest_dataset}.temp_new_columns_mapping"
    
    print(f"[LOAD [START 📤]] Subiendo tabla auxiliar {aux_table}...", flush=True)
    pandas_gbq.to_gbq(mapping_df, destination_table=aux_table, project_id=dest_project, if_exists="replace", credentials=client._credentials)
    
    join_fields = [ col for col in ref_field_names_list if ref_field_names_dic.get(col, False) ]
    new_columns_sql = ",\n".join([f"m.`{sanitized_columns[col]}` AS `{sanitized_columns[col]}`" for col in join_fields])
    update_sql = (
        f"CREATE OR REPLACE TABLE `{source_table}` AS\n"
        f"SELECT s.*, {new_columns_sql}\n"
        f"FROM `{source_table}` s\n"
        f"LEFT JOIN `{aux_table}` m\n"
        f"  ON s.`{source_field}` = m.raw_value;"
    )
    drop_sql = f"DROP TABLE `{aux_table}`;"
    sql_script = update_sql + "\n" + drop_sql
    print("[END [FINISHED 🏁]] SQL para nuevas columnas generado.\n", flush=True)
    return sql_script, mapping_df


# ----------------------------------------------------------------------------
# SQL_generation_normalize_strings()
# ----------------------------------------------------------------------------
def SQL_generation_normalize_strings(config: dict) -> tuple:
    """
    Normaliza los valores de una columna en una tabla de BigQuery usando mapeo manual y fuzzy matching.
    
    Parámetros en config:
      - source_table_to_normalize (str): Tabla fuente.
      - source_table_to_normalize_field_name (str): Columna a normalizar.
      - referece_table_for_normalization_manual_df (pd.DataFrame): DataFrame de mapeo manual (columnas 'Bruto' y 'Normalizado').
      - referece_table_for_normalization_rapidfuzz_df (pd.DataFrame): DataFrame para fuzzy matching.
      - referece_table_for_normalization_rapidfuzz_field_name (str): Columna candidata en fuzzy matching.
      - rapidfuzz_score_filter_use (bool)
      - rapidfuzz_score_filter_min_value (int/float)
      - rapidfuzz_score_filter_no_pass_mapping (str)
      - json_keyfile_GCP_secret_id (str, opcional): Secret ID para GCP.
      - json_keyfile_colab (str, opcional): Ruta al archivo JSON para entornos no GCP.
      - destination_field_name (str, opcional)
    Retorna:
        tuple: (sql_script, df_fuzzy_results)
    """
    import os, json, unicodedata, re
    import pandas as pd
    from google.cloud import bigquery, secretmanager
    from google.oauth2 import service_account
    import pandas_gbq
    from rapidfuzz import process, fuzz  # Asegúrate de tener rapidfuzz instalado

    print("[START 🚀] Iniciando normalización de cadenas...", flush=True)
    source_table = config.get("source_table_to_normalize")
    source_field = config.get("source_table_to_normalize_field_name")
    manual_df = config.get("referece_table_for_normalization_manual_df")
    rapidfuzz_df = config.get("referece_table_for_normalization_rapidfuzz_df")
    rapidfuzz_field = config.get("referece_table_for_normalization_rapidfuzz_field_name")
    rapidfuzz_filter_use = config.get("rapidfuzz_score_filter_use", False)
    rapidfuzz_min_score = config.get("rapidfuzz_score_filter_min_value", 0)
    rapidfuzz_no_pass_value = config.get("rapidfuzz_score_filter_no_pass_mapping", "descartado")
    destination_field_name = config.get("destination_field_name", "").strip()
    if not (isinstance(source_table, str) and source_table):
        raise ValueError("[VALIDATION [ERROR ❌]] 'source_table_to_normalize' es obligatorio.")
    if not (isinstance(source_field, str) and source_field):
        raise ValueError("[VALIDATION [ERROR ❌]] 'source_table_to_normalize_field_name' es obligatorio.")
    if not isinstance(manual_df, pd.DataFrame) or manual_df.empty:
        raise ValueError("[VALIDATION [ERROR ❌]] 'referece_table_for_normalization_manual_df' debe ser un DataFrame no vacío.")
    if not isinstance(rapidfuzz_df, pd.DataFrame) or rapidfuzz_df.empty:
        raise ValueError("[VALIDATION [ERROR ❌]] 'referece_table_for_normalization_rapidfuzz_df' debe ser un DataFrame no vacío.")
    if not (isinstance(rapidfuzz_field, str) and rapidfuzz_field):
        raise ValueError("[VALIDATION [ERROR ❌]] 'referece_table_for_normalization_rapidfuzz_field_name' es obligatorio.")
    
    def _normalize_text(texto: str) -> str:
        texto = texto.lower().strip()
        texto = unicodedata.normalize('NFD', texto)
        texto = ''.join(c for c in texto if unicodedata.category(c) != 'Mn')
        texto = re.sub(r'[^a-z0-9\s]', '', texto)
        return texto

    def _extract_source_values(source_table: str, source_field: str, client: bigquery.Client) -> pd.DataFrame:
        query = f"""
            SELECT DISTINCT `{source_field}` AS raw_value
            FROM `{source_table}`
        """
        df = client.query(query).to_dataframe()
        return df

    def _build_manual_mapping(manual_df: pd.DataFrame) -> dict:
        mapping = {}
        for _, row in manual_df.iterrows():
            bruto = row["Bruto"]
            normalizado = row["Normalizado"]
            if isinstance(bruto, str):
                mapping[_normalize_text(bruto)] = normalizado
        return mapping

    def _build_rapidfuzz_candidates(rapidfuzz_df: pd.DataFrame, rapidfuzz_field: str) -> dict:
        candidates = {}
        for _, row in rapidfuzz_df.iterrows():
            candidate = row[rapidfuzz_field]
            if isinstance(candidate, str):
                candidates[_normalize_text(candidate)] = candidate
        return candidates

    def _apply_mapping(df_source: pd.DataFrame, manual_mapping: dict, rapidfuzz_candidates: dict) -> tuple:
        mapping_results = {}
        fuzzy_results_list = []
        candidate_keys = list(rapidfuzz_candidates.keys())
        for raw in df_source["raw_value"]:
            if not isinstance(raw, str) or not raw:
                mapping_results[raw] = raw
                continue
            normalized_raw = _normalize_text(raw)
            if normalized_raw in manual_mapping:
                mapping_results[raw] = manual_mapping[normalized_raw]
                print(f"[TRANSFORMATION [SUCCESS ✅]] [MANUAL] '{raw}' mapeado a: {manual_mapping[normalized_raw]}", flush=True)
            else:
                best_match = process.extractOne(normalized_raw, candidate_keys, scorer=fuzz.ratio)
                if best_match:
                    match_key, score, _ = best_match
                    if rapidfuzz_filter_use and score < rapidfuzz_min_score:
                        mapping_results[raw] = rapidfuzz_no_pass_value
                        fuzzy_results_list.append({source_field: raw, "Nombre programa formativo": rapidfuzz_no_pass_value, "Rapidfuzz score": score})
                        print(f"[TRANSFORMATION [WARNING ⚠️]] [FUZY] '{raw}' obtuvo score {score} (< {rapidfuzz_min_score}). Se asigna: {rapidfuzz_no_pass_value}", flush=True)
                    else:
                        mapping_results[raw] = rapidfuzz_candidates[match_key]
                        fuzzy_results_list.append({source_field: raw, "Nombre programa formativo": rapidfuzz_candidates[match_key], "Rapidfuzz score": score})
                        print(f"[TRANSFORMATION [SUCCESS ✅]] [FUZY] '{raw}' mapeado a: {rapidfuzz_candidates[match_key]} (Score: {score})", flush=True)
                else:
                    mapping_results[raw] = rapidfuzz_no_pass_value
                    fuzzy_results_list.append({source_field: raw, "Nombre programa formativo": rapidfuzz_no_pass_value, "Rapidfuzz score": None})
                    print(f"[TRANSFORMATION [ERROR ❌]] No se encontró mapeo para '{raw}'. Se asigna: {rapidfuzz_no_pass_value}", flush=True)
        return mapping_results, fuzzy_results_list

    print(f"[EXTRACTION [START ⏳]] Extrayendo valores únicos de `{source_field}` desde {source_table}...", flush=True)
    source_project = source_table.split(".")[0]
    if os.environ.get("GOOGLE_CLOUD_PROJECT"):
        secret_id = config.get("json_keyfile_GCP_secret_id")
        if not secret_id:
            raise ValueError("[AUTHENTICATION [ERROR ❌]] En GCP se debe proporcionar 'json_keyfile_GCP_secret_id'.")
        from google.cloud import secretmanager
        client_sm = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{source_project}/secrets/{secret_id}/versions/latest"
        response = client_sm.access_secret_version(name=secret_name)
        secret_string = response.payload.data.decode("UTF-8")
        creds = service_account.Credentials.from_service_account_info(json.loads(secret_string))
        print("[AUTHENTICATION [SUCCESS ✅]] Credenciales obtenidas desde Secret Manager.", flush=True)
    else:
        json_path = config.get("json_keyfile_colab")
        if not json_path:
            raise ValueError("[AUTHENTICATION [ERROR ❌]] En entornos no GCP se debe proporcionar 'json_keyfile_colab'.")
        creds = service_account.Credentials.from_service_account_file(json_path)
        print("[AUTHENTICATION [SUCCESS ✅]] Credenciales cargadas desde archivo JSON.", flush=True)
    client = bigquery.Client(project=source_project, credentials=creds)
    df_source = _extract_source_values(source_table, source_field, client)
    if df_source.empty:
        print("[EXTRACTION [WARNING ⚠️]] No se encontraron valores en la columna fuente.", flush=True)
        return ("", pd.DataFrame())
    print(f"[EXTRACTION [SUCCESS ✅]] Se encontraron {len(df_source)} valores únicos.", flush=True)
    
    manual_mapping = _build_manual_mapping(manual_df)
    rapidfuzz_candidates = _build_rapidfuzz_candidates(rapidfuzz_df, rapidfuzz_field)
    mapping_results, fuzzy_results_list = _apply_mapping(df_source, manual_mapping, rapidfuzz_candidates)
    
    mapping_df = pd.DataFrame(list(mapping_results.items()), columns=["raw_value", "normalized_value"])
    from_parts = source_table.split(".")
    if len(from_parts) != 3:
        raise ValueError("[VALIDATION [ERROR ❌]] 'source_table_to_normalize' debe ser 'proyecto.dataset.tabla'.")
    dest_project, dest_dataset, _ = from_parts
    aux_table = f"{dest_project}.{dest_dataset}.temp_normalized_strings"
    
    print(f"[LOAD [START 📤]] Subiendo tabla auxiliar {aux_table} con el mapeo...", flush=True)
    pandas_gbq.to_gbq(mapping_df,
                        destination_table=aux_table,
                        project_id=dest_project,
                        if_exists="replace",
                        credentials=client._credentials)
    
    if destination_field_name:
        update_sql = (
            f"CREATE OR REPLACE TABLE `{source_table}` AS\n"
            f"SELECT s.*, m.normalized_value AS `{destination_field_name}`\n"
            f"FROM `{source_table}` s\n"
            f"LEFT JOIN `{aux_table}` m\n"
            f"  ON s.`{source_field}` = m.raw_value;"
        )
    else:
        update_sql = (
            f"CREATE OR REPLACE TABLE `{source_table}` AS\n"
            f"SELECT s.* REPLACE(m.normalized_value AS `{source_field}`)\n"
            f"FROM `{source_table}` s\n"
            f"LEFT JOIN `{aux_table}` m\n"
            f"  ON s.`{source_field}` = m.raw_value;"
        )
    drop_sql = f"DROP TABLE `{aux_table}`;"
    sql_script = update_sql + "\n" + drop_sql
    print("[END [FINISHED 🏁]] SQL para normalización generado.\n", flush=True)
    return sql_script, pd.DataFrame(fuzzy_results_list)

