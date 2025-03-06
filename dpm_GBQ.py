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

# __________________________________________________________________________________________________________________________________________________________
# GBQ_tables_schema_df
# __________________________________________________________________________________________________________________________________________________________
def GBQ_tables_schema_df(config: dict) -> pd.DataFrame:
    """
    Retorna un DataFrame con la información de datasets, tablas y campos de un proyecto de BigQuery,
    añadiendo al final las columnas 'fecha_actualizacion_GBQ' (fecha en la que la tabla fue creada o modificada)
    y 'fecha_actualizacion_df' (fecha en la que se creó el DataFrame).

    Args:
        config (dict):
            - project_id (str) [requerido]: El ID del proyecto de BigQuery.
            - datasets (list) [opcional]: Lista de los IDs de los datasets a consultar. Si no se proporciona,
              se consultan todos los disponibles en el proyecto.
            - include_tables (bool) [opcional]: Indica si se deben incluir las tablas en el esquema. Por defecto es True.
            - json_keyfile_GCP_secret_id (str, requerido en entornos GCP): Secret ID del JSON de credenciales alojado en Secret Manager.
            - json_keyfile_colab (str, requerido en entornos no GCP): Ruta al archivo JSON de credenciales.

    Returns:
        pd.DataFrame: DataFrame con las columnas:
            [
                'project_id',
                'dataset_id',
                'table_name',
                'field_name',
                'field_type',
                'num_rows',
                'num_columns',
                'size_mb',
                'fecha_actualizacion_GBQ',
                'fecha_actualizacion_df'
            ]

    Raises:
        ValueError: Si faltan parámetros obligatorios o se produce un error en la autenticación.
        RuntimeError: Si ocurre un error durante la extracción o transformación de datos.
    """
    # ────────────────────────────── Importaciones Locales ──────────────────────────────
    import os
    import json
    from google.cloud import bigquery
    import pandas as pd
    from google.oauth2.service_account import Credentials

    # ────────────────────────────── INICIO DEL PROCESO ──────────────────────────────
    print("\n🔹🔹🔹 [START ▶️] Inicio del proceso de extracción del esquema de BigQuery 🔹🔹🔹\n", flush=True)

    # ────────────────────────────── AUTENTICACIÓN ──────────────────────────────
    is_gcp = bool(os.environ.get("GOOGLE_CLOUD_PROJECT"))
    if is_gcp:
        json_keyfile_GCP_secret_id_str = config.get("json_keyfile_GCP_secret_id")
        if not json_keyfile_GCP_secret_id_str:
            raise ValueError("[VALIDATION [ERROR ❌]] En entornos GCP se debe proporcionar 'json_keyfile_GCP_secret_id' en config.")
        print("[AUTHENTICATION [START ▶️]] Iniciando autenticación en entorno GCP mediante Secret Manager...", flush=True)
        from google.cloud import secretmanager
        project_id_env = os.environ.get("GOOGLE_CLOUD_PROJECT")
        if not project_id_env:
            raise ValueError("[VALIDATION [ERROR ❌]] No se encontró la variable de entorno 'GOOGLE_CLOUD_PROJECT'.")
        try:
            client_sm = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{project_id_env}/secrets/{json_keyfile_GCP_secret_id_str}/versions/latest"
            response = client_sm.access_secret_version(name=secret_name)
            secret_string = response.payload.data.decode("UTF-8")
            secret_info = json.loads(secret_string)
            creds = Credentials.from_service_account_info(secret_info)
            print(f"[AUTHENTICATION [SUCCESS ✅]] Autenticación en entorno GCP completada. (Secret Manager: {json_keyfile_GCP_secret_id_str})", flush=True)
        except Exception as e:
            raise ValueError(f"[AUTHENTICATION [ERROR ❌]] Error durante la autenticación en GCP: {e}")
    else:
        json_keyfile_colab_str = config.get("json_keyfile_colab")
        if not json_keyfile_colab_str:
            raise ValueError("[VALIDATION [ERROR ❌]] En entornos local/Colab se debe proporcionar 'json_keyfile_colab' en config.")
        print("[AUTHENTICATION [START ▶️]] Iniciando autenticación en entorno local/Colab mediante JSON de credenciales...", flush=True)
        try:
            creds = Credentials.from_service_account_file(json_keyfile_colab_str)
            print("[AUTHENTICATION [SUCCESS ✅]] Autenticación en entorno local/Colab completada.", flush=True)
        except Exception as e:
            raise ValueError(f"[AUTHENTICATION [ERROR ❌]] Error durante la autenticación en entorno local/Colab: {e}")

    # ────────────────────────────── VALIDACIÓN DE PARÁMETROS ──────────────────────────────
    project_id_str = config.get('project_id')
    if not project_id_str:
        raise ValueError("[VALIDATION [ERROR ❌]] El 'project_id' es un argumento requerido en la configuración.")
    print(f"[METRICS [INFO ℹ️]] Proyecto de BigQuery: {project_id_str}", flush=True)
    datasets_incluidos_list = config.get('datasets', None)
    include_tables_bool = config.get('include_tables', True)

    # ────────────────────────────── INICIALIZACIÓN DEL CLIENTE BIGQUERY ──────────────────────────────
    print("[START ▶️] Inicializando cliente de BigQuery...", flush=True)
    try:
        client = bigquery.Client(project=project_id_str, credentials=creds)
        print("[LOAD [SUCCESS ✅]] Cliente de BigQuery inicializado correctamente.", flush=True)
    except Exception as e:
        raise RuntimeError(f"[LOAD [ERROR ❌]] Error al inicializar el cliente de BigQuery: {e}")

    # ────────────────────────────── OBTENCIÓN DE DATASETS ──────────────────────────────
    print("[EXTRACTION [START ▶️]] Obteniendo datasets del proyecto...", flush=True)
    try:
        if datasets_incluidos_list:
            datasets = [client.get_dataset(f"{project_id_str}.{dataset_id}") for dataset_id in datasets_incluidos_list]
            print(f"[EXTRACTION [INFO ℹ️]] Se especificaron {len(datasets_incluidos_list)} datasets para consulta.", flush=True)
        else:
            datasets = list(client.list_datasets(project=project_id_str))
            print(f"[EXTRACTION [INFO ℹ️]] Se encontraron {len(datasets)} datasets en el proyecto.", flush=True)
    except Exception as e:
        raise RuntimeError(f"[EXTRACTION [ERROR ❌]] Error al obtener los datasets: {e}")

    # ────────────────────────────── RECOPILACIÓN DE INFORMACIÓN DE TABLAS Y CAMPOS ──────────────────────────────
    tables_info_list = []
    for dataset in datasets:
        dataset_id_str = dataset.dataset_id
        full_dataset_id_str = f"{project_id_str}.{dataset_id_str}"
        print(f"\n[EXTRACTION [START ▶️]] Procesando dataset: {full_dataset_id_str}", flush=True)
        if include_tables_bool:
            print(f"[EXTRACTION [START ▶️]] Listando tablas para {full_dataset_id_str}...", flush=True)
            try:
                tables = list(client.list_tables(full_dataset_id_str))
                print(f"[EXTRACTION [SUCCESS ✅]] Se encontraron {len(tables)} tablas en {full_dataset_id_str}.", flush=True)
            except Exception as e:
                print(f"[EXTRACTION [ERROR ❌]] Error al listar tablas en {full_dataset_id_str}: {e}", flush=True)
                continue

            for table_item in tables:
                try:
                    table_ref = client.get_table(table_item.reference)
                    table_name_str = table_item.table_id
                    num_rows_int = table_ref.num_rows
                    num_columns_int = len(table_ref.schema)
                    size_mb_float = table_ref.num_bytes / (1024 * 1024)
                    
                    # Se obtiene la fecha de actualización: se prefiere 'created', si no se encuentra se utiliza 'modified'
                    fecha_actualizacion_GBQ_str = None
                    if hasattr(table_ref, 'created') and table_ref.created:
                        fecha_actualizacion_GBQ_str = table_ref.created.strftime("%Y-%m-%d %H:%M:%S")
                    elif hasattr(table_ref, 'modified') and table_ref.modified:
                        fecha_actualizacion_GBQ_str = table_ref.modified.strftime("%Y-%m-%d %H:%M:%S")
                    
                    print(f"[METRICS [INFO ℹ️]] Procesando tabla: {table_name_str} | Filas: {num_rows_int} | Columnas: {num_columns_int} | Tamaño: {round(size_mb_float,2)} MB", flush=True)
                    if table_ref.schema:
                        for field in table_ref.schema:
                            tables_info_list.append({
                                'project_id': project_id_str,
                                'dataset_id': dataset_id_str,
                                'table_name': table_name_str,
                                'field_name': field.name,
                                'field_type': field.field_type,
                                'num_rows': num_rows_int,
                                'num_columns': num_columns_int,
                                'size_mb': round(size_mb_float, 2),
                                'fecha_actualizacion_GBQ': fecha_actualizacion_GBQ_str
                            })
                    else:
                        tables_info_list.append({
                            'project_id': project_id_str,
                            'dataset_id': dataset_id_str,
                            'table_name': table_name_str,
                            'field_name': None,
                            'field_type': None,
                            'num_rows': num_rows_int,
                            'num_columns': num_columns_int,
                            'size_mb': round(size_mb_float, 2),
                            'fecha_actualizacion_GBQ': fecha_actualizacion_GBQ_str
                        })
                except Exception as e:
                    print(f"[EXTRACTION [ERROR ❌]] Error al procesar la tabla en {full_dataset_id_str}: {e}", flush=True)
        else:
            print(f"[EXTRACTION [INFO ℹ️]] Se omiten las tablas para {full_dataset_id_str}.", flush=True)
            tables_info_list.append({
                'project_id': project_id_str,
                'dataset_id': dataset_id_str,
                'table_name': None,
                'field_name': None,
                'field_type': None,
                'num_rows': None,
                'num_columns': None,
                'size_mb': None,
                'fecha_actualizacion_GBQ': None
            })

    # ────────────────────────────── CONVERSIÓN A DATAFRAME ──────────────────────────────
    print("\n[TRANSFORMATION [START ▶️]] Convirtiendo información recopilada a DataFrame...", flush=True)
    try:
        df_tables_fields = pd.DataFrame(tables_info_list)
        print(f"[TRANSFORMATION [SUCCESS ✅]] DataFrame generado exitosamente con {df_tables_fields.shape[0]} registros.", flush=True)
    except Exception as e:
        raise RuntimeError(f"[TRANSFORMATION [ERROR ❌]] Error al convertir la información a DataFrame: {e}")

    # Se añade la fecha de creación del DataFrame (constante para todas las filas)
    df_tables_fields["fecha_actualizacion_df"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    print("\n🔹🔹🔹 [END [FINISHED 🏁]] Esquema de BigQuery extraído y procesado correctamente. 🔹🔹🔹\n", flush=True)
    return df_tables_fields
