# __________________________________________________________________________________________________________________________________________________________
# Repositorio de funciones
# __________________________________________________________________________________________________________________________________________________________
from google.cloud import bigquery
import pandas as pd
import pandas_gbq

import unicodedata
import re
import time
import os
import io
import json

# ----------------------------------------------------------------------------
# _ini_authenticate_API()
# ----------------------------------------------------------------------------
def _ini_authenticate_API(config: dict, project_id: str):
    """
    Autentica utilizando el diccionario común en config.
    
    Dependiendo de 'ini_environment_identificated' se utiliza:
      - "LOCAL": usa la key "json_keyfile_local"
      - "COLAB": usa la key "json_keyfile_colab"
      - Para GCP (por ejemplo, "COLAB_ENTERPRISE" o un project_id): usa "json_keyfile_GCP_secret_id"
      
    Args:
      config (dict): Diccionario de configuración.
      project_id (str): ID del proyecto GCP (se usa en la autenticación GCP).
      
    Returns:
      Credentials: Objeto de credenciales para la autenticación.
    """
    from google.oauth2 import service_account

    env = config.get("ini_environment_identificated", "COLAB")
    if env == "LOCAL":
        json_path = config.get("json_keyfile_local")
        if not json_path:
            raise ValueError("[AUTHENTICATION ERROR ❌] Falta 'json_keyfile_local' en config para entorno LOCAL.")
        credentials = service_account.Credentials.from_service_account_file(json_path)
    elif env == "COLAB":
        json_path = config.get("json_keyfile_colab")
        if not json_path:
            raise ValueError("[AUTHENTICATION ERROR ❌] Falta 'json_keyfile_colab' en config para entorno COLAB.")
        credentials = service_account.Credentials.from_service_account_file(json_path)
    else:
        # Asumimos que para GCP (COLAB_ENTERPRISE o si se pasa un project_id distinto) se usa Secret Manager.
        secret_id = config.get("json_keyfile_GCP_secret_id")
        if not secret_id:
            raise ValueError("[AUTHENTICATION ERROR ❌] Falta 'json_keyfile_GCP_secret_id' en config para entornos GCP.")
        from google.cloud import secretmanager
        client_sm = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client_sm.access_secret_version(name=secret_name)
        secret_str = response.payload.data.decode("UTF-8")
        import json
        secret_info = json.loads(secret_str)
        credentials = service_account.Credentials.from_service_account_info(secret_info)
    return credentials













# ----------------------------------------------------------------------------
# fields_name_format()
# ----------------------------------------------------------------------------
def fields_name_format(config):
    """
    Formatea nombres de campos de datos según configuraciones específicas.
    
    Parámetros en config:
      - fields_name_raw_list (list): Lista de nombres de campos.
      - formato_final (str, opcional): 'CamelCase', 'snake_case', 'Sentence case', o None.
      - reemplazos (dict, opcional): Diccionario de términos a reemplazar.
      - siglas (list, opcional): Lista de siglas que deben mantenerse en mayúsculas.
    
    Retorna:
        pd.DataFrame: DataFrame con columnas 'Campo Original' y 'Campo Formateado'.
    """
    print("[START 🚀] Iniciando formateo de nombres de campos...", flush=True)
    
    def aplicar_reemplazos(field, reemplazos):
        for key, value in sorted(reemplazos.items(), key=lambda x: -len(x[0])):
            if key in field:
                field = field.replace(key, value)
        return field

    def formatear_campo(field, formato, siglas):
        if formato is None or formato is False:
            return field
        words = [w for w in re.split(r'[_\-\s]+', field) if w]
        if formato == 'CamelCase':
            return ''.join(
                word.upper() if word.upper() in siglas
                else word.capitalize() if idx == 0
                else word.lower()
                for idx, word in enumerate(words)
            )
        elif formato == 'snake_case':
            return '_'.join(
                word.upper() if word.upper() in siglas
                else word.lower() for word in words
            )
        elif formato == 'Sentence case':
            return ' '.join(
                word.upper() if word.upper() in siglas
                else word.capitalize() if idx == 0
                else word.lower()
                for idx, word in enumerate(words)
            )
        else:
            raise ValueError(f"Formato '{formato}' no soportado.")
    
    resultado = []
    for field in config.get('fields_name_raw_list', []):
        original_field = field
        field = aplicar_reemplazos(field, config.get('reemplazos', {}))
        formatted_field = formatear_campo(field, config.get('formato_final', 'CamelCase'), [sig.upper() for sig in config.get('siglas', [])])
        resultado.append({'Campo Original': original_field, 'Campo Formateado': formatted_field})
    
    df_result = pd.DataFrame(resultado)
    print("[END [FINISHED 🏁]] Formateo de nombres completado.\n", flush=True)
    return df_result






















# ----------------------------------------------------------------------------
# table_various_sources_to_DF()
# ----------------------------------------------------------------------------
def table_various_sources_to_DF(params: dict) -> pd.DataFrame:
    """
    Extrae datos desde distintos orígenes (archivo, Google Sheets, BigQuery o GCS) y los convierte en un DataFrame.
    
    Parámetros en params:
      - (ver docstring completo en la versión original)
    
    Retorna:
      pd.DataFrame: DataFrame con los datos extraídos y procesados.
    
    Raises:
      RuntimeError: Si ocurre un error al extraer o procesar los datos.
      ValueError: Si faltan parámetros obligatorios para identificar el origen de datos.
    """
    import re
    import io
    import time
    import pandas as pd

    # Para Google Sheets y otros servicios de Google
    import gspread
    try:
        from google.colab import files
    except ImportError:
        pass

    # ────────────────────────────── Utilidades Comunes ──────────────────────────────
    def _imprimir_encabezado(mensaje: str) -> None:
        print(f"\n🔹🔹🔹 {mensaje} 🔹🔹🔹\n", flush=True)

    def _validar_comun(params: dict) -> None:
        if not (params.get('json_keyfile_GCP_secret_id') or params.get('json_keyfile_colab')):
            raise ValueError("[VALIDATION [ERROR ❌]] Falta el parámetro obligatorio 'json_keyfile_GCP_secret_id' o 'json_keyfile_colab' para autenticación.")

    def _apply_common_filters(df: pd.DataFrame, params: dict) -> pd.DataFrame:
        # Filtrado de filas (si corresponde)
        row_start = params.get('source_table_row_start', 0)
        row_end = params.get('source_table_row_end', None)
        if row_end is not None:
            df = df.iloc[row_start:row_end]
        else:
            df = df.iloc[row_start:]
        
        # Filtrado de columnas por posición
        col_start = params.get('source_table_col_start', 0)
        col_end = params.get('source_table_col_end', None)
        if col_end is not None:
            df = df.iloc[:, col_start:col_end]
        else:
            df = df.iloc[:, col_start:]
        
        # Selección de campos específicos si se indica
        if 'source_table_fields_list' in params:
            fields = params['source_table_fields_list']
            fields = [f for f in fields if f in df.columns]
            if fields:
                df = df[fields]
        return df

    def _auto_convert(df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            col_lower = col.lower()
            if "fecha" in col_lower or col_lower == "valor":
                try:
                    df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
                except Exception as e:
                    print(f"[TRANSFORMATION [WARNING ⚠️]] Error al convertir la columna '{col}' a datetime: {e}", flush=True)
            elif col_lower in ['importe', 'saldo']:
                try:
                    df[col] = df[col].apply(lambda x: float(x.replace('.', '').replace(',', '.')) if isinstance(x, str) and x.strip() != '' else x)
                except Exception as e:
                    print(f"[TRANSFORMATION [WARNING ⚠️]] Error al convertir la columna '{col}' a float: {e}", flush=True)
        return df

    # ────────────────────────────── Detección del Origen ──────────────────────────────
    def _es_fuente_archivo(params: dict) -> bool:
        return bool(params.get('file_source_table_path', '').strip())

    def _es_fuente_gsheet(params: dict) -> bool:
        return (not _es_fuente_archivo(params)) and (
            bool(params.get('spreadsheet_source_table_id', '').strip()) and 
            bool(params.get('spreadsheet_source_table_worksheet_name', '').strip())
        )

    def _es_fuente_gbq(params: dict) -> bool:
        return bool(params.get('GBQ_source_table_name', '').strip())

    def _es_fuente_gcs(params: dict) -> bool:
        return bool(params.get('GCS_source_table_bucket_name', '').strip()) and bool(params.get('GCS_source_table_file_path', '').strip())

    # ────────────────────────────── Fuente – Archivo Local ──────────────────────────────
    def _leer_archivo(params: dict) -> pd.DataFrame:
        _imprimir_encabezado("[START 🚀] Iniciando carga del archivo")
        file_path = params.get('file_source_table_path')
        row_skip_empty = params.get('source_table_filter_skip_row_empty_use', True)
        row_start = params.get('source_table_row_start', 0)
        row_end = params.get('source_table_row_end', None)
        nrows = (row_end - row_start) if row_end is not None else None
        col_start = params.get('source_table_col_start', 0)
        col_end = params.get('source_table_col_end', None)

        if not file_path:
            print("[EXTRACTION [WARNING ⚠️]] No se proporcionó 'file_source_table_path'. Suba un archivo desde su ordenador:", flush=True)
            uploaded = files.upload()
            file_path = list(uploaded.keys())[0]
            file_input = io.BytesIO(uploaded[file_path])
            print(f"[EXTRACTION [SUCCESS ✅]] Archivo '{file_path}' subido exitosamente.", flush=True)
        else:
            file_input = file_path

        _, ext = os.path.splitext(file_path)
        ext = ext.lower()

        try:
            print(f"[EXTRACTION [START ⏳]] Leyendo archivo '{file_path}'...", flush=True)
            if ext in ['.xls', '.xlsx']:
                engine = 'xlrd' if ext == '.xls' else 'openpyxl'
                df = pd.read_excel(file_input, engine=engine, skiprows=row_start, nrows=nrows)
            elif ext == '.csv':
                df = pd.read_csv(file_input, skiprows=row_start, nrows=nrows, sep=',')
            elif ext == '.tsv':
                df = pd.read_csv(file_input, skiprows=row_start, nrows=nrows, sep='\t')
            else:
                raise RuntimeError(f"[EXTRACTION [ERROR ❌]] Extensión de archivo '{ext}' no soportada.")
            
            if col_end is not None:
                df = df.iloc[:, col_start:col_end]
            else:
                df = df.iloc[:, col_start:]
            
            if row_skip_empty:
                initial_rows = len(df)
                df.dropna(how='all', inplace=True)
                removed_rows = initial_rows - len(df)
                print(f"[TRANSFORMATION [SUCCESS ✅]] Se eliminaron {removed_rows} filas vacías.", flush=True)
            
            df = df.convert_dtypes()
            df = _auto_convert(df)

            print("\n[METRICS [INFO 📊]] INFORME ESTADÍSTICO DEL DATAFRAME:")
            print(f"  - Total filas: {df.shape[0]}")
            print(f"  - Total columnas: {df.shape[1]}")
            print("  - Tipos de datos por columna:")
            print(df.dtypes)
            print("  - Resumen estadístico (numérico):")
            print(df.describe())
            print("  - Resumen estadístico (incluyendo variables categóricas):")
            print(df.describe(include='all'))
            print(f"\n[END [FINISHED 🏁]] Archivo '{file_path}' cargado correctamente. Filas: {df.shape[0]}, Columnas: {df.shape[1]}", flush=True)
            return df

        except Exception as e:
            error_message = f"[EXTRACTION [ERROR ❌]] Error al leer el archivo '{file_path}': {e}"
            print(error_message, flush=True)
            raise RuntimeError(error_message)

    # ────────────────────────────── Fuente – Google Sheets ──────────────────────────────
    def _leer_google_sheet(params: dict) -> pd.DataFrame:
        from googleapiclient.discovery import build

        spreadsheet_id_raw = params.get("spreadsheet_source_table_id")
        if "spreadsheets/d/" in spreadsheet_id_raw:
            match = re.search(r"/d/([a-zA-Z0-9-_]+)", spreadsheet_id_raw)
            if match:
                spreadsheet_id = match.group(1)
            else:
                raise ValueError("[VALIDATION [ERROR ❌]] No se pudo extraer el ID de la hoja de cálculo desde la URL proporcionada.")
        else:
            spreadsheet_id = spreadsheet_id_raw

        worksheet_name = params.get("spreadsheet_source_table_worksheet_name")
        if not spreadsheet_id or not worksheet_name:
            raise ValueError("[VALIDATION [ERROR ❌]] Faltan 'spreadsheet_source_table_id' o 'spreadsheet_source_table_worksheet_name'.")

        try:
            scope_list = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            # Determinación del project_id según el entorno
            ini_env = params.get("ini_environment_identificated")
            if not ini_env:
                raise ValueError("[VALIDATION [ERROR ❌]] Falta la key 'ini_environment_identificated' en params.")
            project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") if ini_env == "COLAB_ENTERPRISE" else ini_env

            # Uso de _ini_authenticate_API para la autenticación
            creds = _ini_authenticate_API(params, project_id)
            creds = creds.with_scopes(scope_list)
            
            service = build('sheets', 'v4', credentials=creds)
            range_name = f"{worksheet_name}"
            print("[EXTRACTION [START ⏳]] Extrayendo datos de Google Sheets...", flush=True)
            result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
            data = result.get('values', [])
            if not data:
                print("[EXTRACTION [WARNING ⚠️]] No se encontraron datos en la hoja especificada.", flush=True)
                return pd.DataFrame()
            
            # Se obtiene el encabezado y se ajustan las filas para que todas tengan la misma longitud.
            header = data[0]
            n_columns = len(header)
            data_fixed = []
            for row in data[1:]:
                if len(row) < n_columns:
                    row = row + [None] * (n_columns - len(row))
                elif len(row) > n_columns:
                    row = row[:n_columns]
                data_fixed.append(row)

            df = pd.DataFrame(data_fixed, columns=header)
            print(f"[EXTRACTION [SUCCESS ✅]] Datos extraídos con éxito de la hoja '{worksheet_name}'.", flush=True)
            return df

        except Exception as e:
            raise ValueError(f"[EXTRACTION [ERROR ❌]] Error al extraer datos de Google Sheets: {e}")

    # ────────────────────────────── Fuente – BigQuery ──────────────────────────────
    def _leer_gbq(params: dict) -> pd.DataFrame:
        scope_list = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]
        ini_env = params.get("ini_environment_identificated")
        if not ini_env:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta la key 'ini_environment_identificated' en params.")
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") if ini_env == "COLAB_ENTERPRISE" else ini_env

        # Uso de _ini_authenticate_API para la autenticación en BigQuery
        from google.oauth2.service_account import Credentials
        creds = _ini_authenticate_API(params, project_id)
        creds = creds.with_scopes(scope_list)
    
        client_bq = bigquery.Client(credentials=creds, project=project_id)
        gbq_table = params.get("GBQ_source_table_name")
        if not gbq_table:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta el parámetro 'GBQ_source_table_name' para BigQuery.")
        try:
            query = f"SELECT * FROM `{gbq_table}`"
            print(f"[EXTRACTION [START ⏳]] Ejecutando consulta en BigQuery: {query}", flush=True)
            df = client_bq.query(query).to_dataframe()
            print("[EXTRACTION [SUCCESS ✅]] Datos extraídos con éxito de BigQuery.", flush=True)
            return df
        except Exception as e:
            raise RuntimeError(f"[EXTRACTION [ERROR ❌]] Error al extraer datos de BigQuery: {e}")

    # ────────────────────────────── Fuente – Google Cloud Storage (GCS) ──────────────────────────────
    def _leer_gcs(params: dict) -> pd.DataFrame:
        scope_list = ["https://www.googleapis.com/auth/devstorage.read_only"]
        ini_env = params.get("ini_environment_identificated")
        if not ini_env:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta la key 'ini_environment_identificated' en params.")
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") if ini_env == "COLAB_ENTERPRISE" else ini_env

        # Uso de _ini_authenticate_API para la autenticación en GCS
        from google.oauth2.service_account import Credentials
        creds = _ini_authenticate_API(params, project_id)
        creds = creds.with_scopes(scope_list)
    
        try:
            bucket_name = params.get("GCS_source_table_bucket_name")
            file_path = params.get("GCS_source_table_file_path")
            if not bucket_name or not file_path:
                raise ValueError("[VALIDATION [ERROR ❌]] Faltan 'GCS_source_table_bucket_name' o 'GCS_source_table_file_path'.")
            from google.cloud import storage
            client_storage = storage.Client(credentials=creds, project=project_id)
            bucket = client_storage.bucket(bucket_name)
            blob = bucket.blob(file_path)
            print(f"[EXTRACTION [START ⏳]] Descargando archivo '{file_path}' del bucket '{bucket_name}'...", flush=True)
            file_bytes = blob.download_as_bytes()
            _, ext = os.path.splitext(file_path)
            ext = ext.lower()
            if ext in ['.xls', '.xlsx']:
                engine = 'xlrd' if ext == '.xls' else 'openpyxl'
                df = pd.read_excel(io.BytesIO(file_bytes), engine=engine)
            elif ext == '.csv':
                df = pd.read_csv(io.BytesIO(file_bytes), sep=',')
            elif ext == '.tsv':
                df = pd.read_csv(io.BytesIO(file_bytes), sep='\t')
            else:
                raise RuntimeError(f"[EXTRACTION [ERROR ❌]] Extensión de archivo '{ext}' no soportada en GCS.")
            print("[EXTRACTION [SUCCESS ✅]] Archivo descargado y leído desde GCS.", flush=True)
            return df
        except Exception as e:
            raise RuntimeError(f"[EXTRACTION [ERROR ❌]] Error al leer archivo desde GCS: {e}")

    # ────────────────────────────── PROCESO PRINCIPAL ──────────────────────────────
    _validar_comun(params)
    if _es_fuente_archivo(params):
        df = _leer_archivo(params)
    elif _es_fuente_gsheet(params):
        df = _leer_google_sheet(params)
    elif _es_fuente_gbq(params):
        df = _leer_gbq(params)
    elif _es_fuente_gcs(params):
        df = _leer_gcs(params)
    else:
        raise ValueError(
            "[VALIDATION [ERROR ❌]] No se han proporcionado parámetros válidos para identificar el origen de datos. "
            "Defina 'file_source_table_path', 'spreadsheet_source_table_id' y 'spreadsheet_source_table_worksheet_name', "
            "'GBQ_source_table_name' o 'GCS_source_table_bucket_name' y 'GCS_source_table_file_path'."
        )

    df = _apply_common_filters(df, params)
    return df




















# ----------------------------------------------------------------------------
# table_DF_to_various_targets()
# ----------------------------------------------------------------------------
# @title table_DF_to_various_targets()
def table_DF_to_various_targets(params: dict) -> None:
    """
    Escribe un DataFrame en distintos destinos (archivo local, Google Sheets, BigQuery o GCS)
    según la configuración definida en el diccionario de entrada, permitiendo especificar el modo
    de escritura: sobrescribir (overwrite) o agregar (append).

    Args:
        params (dict):
            - (ver docstring completo en la versión original)
    
    Returns:
        None

    Raises:
        ValueError: Si faltan parámetros obligatorios para identificar el destino o el DataFrame.
        RuntimeError: Si ocurre un error durante la escritura o transformación de los datos.
    """
    from google.oauth2.service_account import Credentials

    print("\n🔹🔹🔹 [START ▶️] Iniciando escritura de DataFrame en destino configurado 🔹🔹🔹\n", flush=True)

    # VALIDACIÓN DEL DATAFRAME
    df = params.get("df")
    if df is None or not isinstance(df, pd.DataFrame):
        raise ValueError("[VALIDATION [ERROR ❌]] Se debe proporcionar el DataFrame a exportar en la clave 'df' de params.")
    print(f"[METRICS [INFO ℹ️]] DataFrame recibido con {df.shape[0]} filas y {df.shape[1]} columnas.", flush=True)

    # VALIDACIÓN DE AUTENTICACIÓN
    if not (params.get('json_keyfile_GCP_secret_id') or params.get('json_keyfile_colab')):
        raise ValueError("[VALIDATION [ERROR ❌]] Falta el parámetro obligatorio 'json_keyfile_GCP_secret_id' o 'json_keyfile_colab' para autenticación.")

    # ────────────────────────────── DETECCIÓN DEL DESTINO ──────────────────────────────
    def _es_target_archivo(params: dict) -> bool:
        return bool(params.get('file_target_table_path', '').strip())

    def _es_target_gsheet(params: dict) -> bool:
        return (not _es_target_archivo(params)) and (
            bool(params.get('spreadsheet_target_table_id', '').strip()) and 
            bool(params.get('spreadsheet_target_table_worksheet_name', '').strip())
        )

    def _es_target_gbq(params: dict) -> bool:
        return bool(params.get('GBQ_target_table_name', '').strip())

    def _es_target_gcs(params: dict) -> bool:
        return bool(params.get('GCS_target_table_bucket_name', '').strip()) and bool(params.get('GCS_target_table_file_path', '').strip())

    # ────────────────────────────── ESCRITURA – ARCHIVO LOCAL ──────────────────────────────
    def _escribir_archivo(params: dict, df: pd.DataFrame) -> None:
        print("\n[LOAD [START ▶️]] Iniciando escritura en archivo local...", flush=True)
        mode = params.get("file_target_table_overwrite_or_append", "overwrite").lower()
        file_path_str = params.get('file_target_table_path')
        _, ext = os.path.splitext(file_path_str)
        ext = ext.lower()
        try:
            print(f"[LOAD [INFO ℹ️]] Escribiendo DataFrame en: {file_path_str} con modo '{mode}'", flush=True)
            if ext in ['.xls', '.xlsx']:
                engine_str = 'openpyxl'
                if mode == "append" and os.path.exists(file_path_str):
                    df_existing = pd.read_excel(file_path_str, engine=engine_str)
                    df_combined = pd.concat([df_existing, df], ignore_index=True)
                    df_combined.to_excel(file_path_str, index=False, engine=engine_str)
                else:
                    df.to_excel(file_path_str, index=False, engine=engine_str)
            elif ext == '.csv':
                if mode == "append" and os.path.exists(file_path_str):
                    df.to_csv(file_path_str, index=False, mode='a', header=False)
                else:
                    df.to_csv(file_path_str, index=False, mode='w', header=True)
            elif ext == '.tsv':
                if mode == "append" and os.path.exists(file_path_str):
                    df.to_csv(file_path_str, sep='\t', index=False, mode='a', header=False)
                else:
                    df.to_csv(file_path_str, sep='\t', index=False, mode='w', header=True)
            else:
                raise RuntimeError(f"[LOAD [ERROR ❌]] Extensión '{ext}' no soportada para escritura en archivo local.")
            print(f"[LOAD [SUCCESS ✅]] DataFrame escrito exitosamente en '{file_path_str}'.", flush=True)
            print(f"[METRICS [INFO ℹ️]] Destino final: file://{file_path_str}", flush=True)
        except Exception as e:
            raise RuntimeError(f"[LOAD [ERROR ❌]] Error al escribir en archivo local: {e}")

    # ────────────────────────────── ESCRITURA – GOOGLE SHEETS ──────────────────────────────
    def _escribir_google_sheet(params: dict, df: pd.DataFrame) -> None:
        print("\n[LOAD [START ▶️]] Iniciando escritura en Google Sheets...", flush=True)
        import re
        from googleapiclient.discovery import build

        mode = params.get("spreadsheet_target_table_overwrite_or_append", "overwrite").lower()
        spreadsheet_id_raw = params.get("spreadsheet_target_table_id")
        if "spreadsheets/d/" in spreadsheet_id_raw:
            match = re.search(r"/d/([a-zA-Z0-9-_]+)", spreadsheet_id_raw)
            if match:
                spreadsheet_id_str = match.group(1)
            else:
                raise ValueError("[VALIDATION [ERROR ❌]] No se pudo extraer el ID de la hoja de cálculo desde la URL proporcionada.")
        else:
            spreadsheet_id_str = spreadsheet_id_raw

        worksheet_name_str = params.get("spreadsheet_target_table_worksheet_name")
        if not spreadsheet_id_str or not worksheet_name_str:
            raise ValueError("[VALIDATION [ERROR ❌]] Faltan 'spreadsheet_target_table_id' o 'spreadsheet_target_table_worksheet_name' en params.")
    
        try:
            scope_list = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            ini_env = params.get("ini_environment_identificated")
            if not ini_env:
                raise ValueError("[VALIDATION [ERROR ❌]] Falta la key 'ini_environment_identificated' en params.")
            project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") if ini_env == "COLAB_ENTERPRISE" else ini_env

            # Uso de _ini_authenticate_API para la autenticación en Google Sheets
            creds = _ini_authenticate_API(params, project_id)
            creds = creds.with_scopes(scope_list)
    
            service = build('sheets', 'v4', credentials=creds)
    
            if mode == "overwrite":
                clear_body = {}
                service.spreadsheets().values().clear(
                    spreadsheetId=spreadsheet_id_str,
                    range=worksheet_name_str,
                    body=clear_body
                ).execute()
                values_list = [df.columns.tolist()] + df.astype(str).values.tolist()
            elif mode == "append":
                values_list = df.astype(str).values.tolist()
            else:
                raise ValueError("Modo desconocido para Google Sheets: use 'overwrite' o 'append'.")
    
            body_dic = {"values": values_list}
            print(f"[LOAD [INFO ℹ️]] Actualizando hoja '{worksheet_name_str}' en la planilla '{spreadsheet_id_str}' con modo '{mode}'...", flush=True)
    
            if mode == "overwrite":
                result_dic = service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id_str,
                    range=worksheet_name_str,
                    valueInputOption="USER_ENTERED",
                    body=body_dic
                ).execute()
            elif mode == "append":
                result_dic = service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id_str,
                    range=worksheet_name_str,
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body=body_dic
                ).execute()
    
            updated_cells_int = result_dic.get('updatedCells', 'N/A')
            print(f"[LOAD [SUCCESS ✅]] Se actualizaron {updated_cells_int} celdas en Google Sheets.", flush=True)
            print(f"[METRICS [INFO ℹ️]] Destino final: https://docs.google.com/spreadsheets/d/{spreadsheet_id_str}", flush=True)
        except Exception as e:
            raise RuntimeError(f"[LOAD [ERROR ❌]] Error al escribir en Google Sheets: {e}")

    # ────────────────────────────── ESCRITURA – BIGQUERY ──────────────────────────────
    def _escribir_gbq(params: dict, df: pd.DataFrame) -> None:
        print("\n[LOAD [START ▶️]] Iniciando carga de DataFrame en BigQuery...", flush=True)
        import json
        scope_list = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]
        ini_env = params.get("ini_environment_identificated")
        if not ini_env:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta la key 'ini_environment_identificated' en params.")
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") if ini_env == "COLAB_ENTERPRISE" else ini_env

        # Uso de _ini_authenticate_API para la autenticación en BigQuery
        from google.oauth2.service_account import Credentials
        creds = _ini_authenticate_API(params, project_id)
        creds = creds.with_scopes(scope_list)
            
        client_bq = bigquery.Client(credentials=creds, project=project_id)
        gbq_table_str = params.get("GBQ_target_table_name")
        if not gbq_table_str:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta el parámetro 'GBQ_target_table_name' para BigQuery.")
            
        print(f"[LOAD [INFO ℹ️]] Cargando DataFrame en la tabla BigQuery: {gbq_table_str}...", flush=True)
        try:
            job_config = bigquery.LoadJobConfig()
            mode = params.get("GBQ_target_table_overwrite_or_append", "overwrite").lower()
            if mode == "overwrite":
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            elif mode == "append":
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            else:
                raise ValueError("Modo desconocido para BigQuery: use 'overwrite' o 'append'.")
            
            job = client_bq.load_table_from_dataframe(df, gbq_table_str, job_config=job_config)
            job.result()
            print("[LOAD [SUCCESS ✅]] DataFrame cargado exitosamente en BigQuery.", flush=True)
            print(f"[METRICS [INFO ℹ️]] Destino final: https://console.cloud.google.com/bigquery?project={project_id}&ws=!1m5!1m4!4m3!1s{gbq_table_str}", flush=True)
        except Exception as e:
            raise RuntimeError(f"[LOAD [ERROR ❌]] Error al escribir en BigQuery: {e}")

    # ────────────────────────────── ESCRITURA – GOOGLE CLOUD STORAGE (GCS) ──────────────────────────────
    def _escribir_gcs(params: dict, df: pd.DataFrame) -> None:
        print("\n[LOAD [START ▶️]] Iniciando subida de DataFrame a Google Cloud Storage (GCS)...", flush=True)
        import json
        scope_list = ["https://www.googleapis.com/auth/devstorage.read_only"]
        ini_env = params.get("ini_environment_identificated")
        if not ini_env:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta la key 'ini_environment_identificated' en params.")
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") if ini_env == "COLAB_ENTERPRISE" else ini_env

        # Uso de _ini_authenticate_API para la autenticación en GCS
        from google.oauth2.service_account import Credentials
        creds = _ini_authenticate_API(params, project_id)
        creds = creds.with_scopes(scope_list)
            
        try:
            bucket_name_str = params.get("GCS_target_table_bucket_name")
            file_path_str = params.get("GCS_target_table_file_path")
            if not bucket_name_str or not file_path_str:
                raise ValueError("[VALIDATION [ERROR ❌]] Falta 'GCS_target_table_bucket_name' o 'GCS_target_table_file_path' en params.")
            from google.cloud import storage
            client_storage = storage.Client(credentials=creds, project=project_id)
            bucket = client_storage.bucket(bucket_name_str)
            blob = bucket.blob(file_path_str)
            _, ext = os.path.splitext(file_path_str)
            ext = ext.lower()
            print(f"[LOAD [INFO ℹ️]] Procesando DataFrame para archivo con extensión '{ext}'...", flush=True)
            
            if mode == "append" and blob.exists(client_storage):
                blob_bytes = blob.download_as_string()
                if ext == '.csv':
                    df_existing = pd.read_csv(io.StringIO(blob_bytes.decode('utf-8')))
                elif ext == '.tsv':
                    df_existing = pd.read_csv(io.StringIO(blob_bytes.decode('utf-8')), sep='\t')
                elif ext in ['.xls', '.xlsx']:
                    df_existing = pd.read_excel(io.BytesIO(blob_bytes))
                else:
                    raise RuntimeError(f"[LOAD [ERROR ❌]] Extensión '{ext}' no soportada para GCS en modo append.")
                df = pd.concat([df_existing, df], ignore_index=True)
            
            if ext in ['.xls', '.xlsx']:
                engine_str = 'openpyxl'
                output = io.BytesIO()
                df.to_excel(output, index=False, engine=engine_str)
                file_bytes = output.getvalue()
                content_type_str = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            elif ext == '.csv':
                file_bytes = df.to_csv(index=False).encode('utf-8')
                content_type_str = 'text/csv'
            elif ext == '.tsv':
                file_bytes = df.to_csv(sep='\t', index=False).encode('utf-8')
                content_type_str = 'text/tab-separated-values'
            else:
                raise RuntimeError(f"[LOAD [ERROR ❌]] Extensión '{ext}' no soportada para GCS.")
            print(f"[LOAD [INFO ℹ️]] Subiendo archivo '{file_path_str}' al bucket '{bucket_name_str}'...", flush=True)
            blob.upload_from_string(file_bytes, content_type=content_type_str)
            print("[LOAD [SUCCESS ✅]] Archivo subido exitosamente a GCS.", flush=True)
            print(f"[METRICS [INFO ℹ️]] Destino final: https://console.cloud.google.com/storage/browser/{bucket_name_str}?project={project_id}", flush=True)
        except Exception as e:
            raise RuntimeError(f"[LOAD [ERROR ❌]] Error al escribir en GCS: {e}")

    try:
        if _es_target_archivo(params):
            _escribir_archivo(params, df)
        elif _es_target_gsheet(params):
            _escribir_google_sheet(params, df)
        elif _es_target_gbq(params):
            _escribir_gbq(params, df)
        elif _es_target_gcs(params):
            _escribir_gcs(params, df)
        else:
            raise ValueError(
                "[VALIDATION [ERROR ❌]] No se han proporcionado parámetros válidos para identificar el destino. "
                "Defina 'file_target_table_path', 'spreadsheet_target_table_id' y 'spreadsheet_target_table_worksheet_name', "
                "'GBQ_target_table_name' o 'GCS_target_table_bucket_name' y 'GCS_target_table_file_path'."
            )
    except Exception as error_e:
        print(f"\n🔹🔹🔹 [END [FAILED ❌]] Proceso finalizado con errores: {error_e} 🔹🔹🔹\n", flush=True)
        raise

    print("\n🔹🔹🔹 [END [FINISHED ✅]] Escritura completada exitosamente. 🔹🔹🔹\n", flush=True)
