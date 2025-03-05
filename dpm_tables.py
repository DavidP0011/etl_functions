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
      - file_source_table_path (str, opcional): Ruta al archivo. Si está vacío, se usará otra fuente.
      - spreadsheet_source_table_id (str, opcional): URL o ID de la hoja de cálculo (usado si file_source_table_path está vacío).
      - spreadsheet_source_table_worksheet_name (str, opcional): Nombre de la pestaña a extraer (requiere spreadsheet_source_table_id).
      - GBQ_source_table_name (str, opcional): Nombre de la tabla en BigQuery (ej: "animum-dev-datawarehouse.vl_01raw_01.MA_ENTIDADES").
      - GCS_source_table_bucket_name (str, opcional): Nombre del bucket en Google Cloud Storage.
      - GCS_source_table_file_path (str, opcional): Ruta al archivo en GCS.
      - source_table_row_start (int, opcional): Primera fila a leer (0-indexado). Por defecto 0.
      - source_table_row_end (int, opcional): Última fila a leer (excluyente). Si es None, lee hasta el final.
      - source_table_fields_list (list, opcional): Lista de campos a seleccionar, ej: ["Nombre_Personal", "Apellidos", "Codigo_interno"].
      - source_table_col_start (int, opcional): Primera columna a leer (0-indexado). Por defecto 0.
      - source_table_col_end (int, opcional): Última columna a leer (excluyente). Si es None, lee todas.
      - json_keyfile_GCP_secret_id (str, requerido en GCP): Secret ID del JSON de credenciales alojado en Secret Manager.
      - json_keyfile_colab (str, requerido en Colab/local): Ruta al archivo JSON de credenciales.
    
    Retorna:
      pd.DataFrame: DataFrame con los datos extraídos y procesados.
    
    Raises:
      RuntimeError: Si ocurre un error al extraer o procesar los datos.
      ValueError: Si faltan parámetros obligatorios para identificar el origen de datos.
    """
    import os
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
            # Tomar la intersección con las columnas existentes
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
        import pandas as pd
        import re

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
            is_gcp = bool(os.environ.get("GOOGLE_CLOUD_PROJECT"))
            from google.oauth2.service_account import Credentials

            if is_gcp:
                secret_id = params.get("json_keyfile_GCP_secret_id")
                if not secret_id:
                    raise ValueError("[AUTHENTICATION [ERROR ❌]] En GCP se debe proporcionar 'json_keyfile_GCP_secret_id'.")
                print("[AUTHENTICATION [INFO] 🔐] Entorno GCP detectado. Usando Secret Manager con json_keyfile_GCP_secret_id.", flush=True)
                from google.cloud import secretmanager
                import json
                project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
                if not project_id:
                    raise ValueError("No se encontró la variable de entorno 'GOOGLE_CLOUD_PROJECT'.")
                client = secretmanager.SecretManagerServiceClient()
                secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
                response = client.access_secret_version(name=secret_name)
                secret_string = response.payload.data.decode("UTF-8")
                secret_info = json.loads(secret_string)
                creds = Credentials.from_service_account_info(secret_info, scopes=scope_list)
            else:
                json_keyfile = params.get("json_keyfile_colab")
                if not json_keyfile:
                    raise ValueError("[AUTHENTICATION [ERROR ❌]] En Colab se debe proporcionar 'json_keyfile_colab'.")
                print("[AUTHENTICATION [INFO] 🔐] Entorno Colab detectado. Usando json_keyfile_colab.", flush=True)
                creds = Credentials.from_service_account_file(json_keyfile, scopes=scope_list)

            service = build('sheets', 'v4', credentials=creds)
            range_name = f"{worksheet_name}"
            print("[EXTRACTION [START ⏳]] Extrayendo datos de Google Sheets...", flush=True)
            result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
            data = result.get('values', [])
            if not data:
                print("[EXTRACTION [WARNING ⚠️]] No se encontraron datos en la hoja especificada.", flush=True)
                return pd.DataFrame()
            df = pd.DataFrame(data[1:], columns=data[0])
            print(f"[EXTRACTION [SUCCESS ✅]] Datos extraídos con éxito de la hoja '{worksheet_name}'.", flush=True)
            return df

        except Exception as e:
            raise ValueError(f"[EXTRACTION [ERROR ❌]] Error al extraer datos de Google Sheets: {e}")

    # ────────────────────────────── Fuente – BigQuery ──────────────────────────────
    def _leer_gbq(params: dict) -> pd.DataFrame:
        """
        Extrae datos desde BigQuery utilizando el parámetro 'GBQ_source_table_name'.
        """
        from google.cloud import bigquery
        scope_list = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]
        is_gcp = bool(os.environ.get("GOOGLE_CLOUD_PROJECT"))
        from google.oauth2.service_account import Credentials

        if is_gcp:
            secret_id = params.get("json_keyfile_GCP_secret_id")
            if not secret_id:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En GCP se debe proporcionar 'json_keyfile_GCP_secret_id'.")
            print("[AUTHENTICATION [INFO] 🔐] Entorno GCP detectado. Usando Secret Manager para BigQuery.", flush=True)
            from google.cloud import secretmanager
            import json
            project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
            if not project_id:
                raise ValueError("No se encontró la variable de entorno 'GOOGLE_CLOUD_PROJECT'.")
            client_sm = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client_sm.access_secret_version(name=secret_name)
            secret_string = response.payload.data.decode("UTF-8")
            secret_info = json.loads(secret_string)
            creds = Credentials.from_service_account_info(secret_info, scopes=scope_list)
        else:
            json_keyfile = params.get("json_keyfile_colab")
            if not json_keyfile:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En Colab se debe proporcionar 'json_keyfile_colab'.")
            print("[AUTHENTICATION [INFO] 🔐] Entorno Colab detectado. Usando json_keyfile_colab para BigQuery.", flush=True)
            creds = Credentials.from_service_account_file(json_keyfile, scopes=scope_list)

        client_bq = bigquery.Client(credentials=creds, project=os.environ.get("GOOGLE_CLOUD_PROJECT"))
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
        """
        Extrae datos desde GCS utilizando:
          - GCS_source_table_bucket_name
          - GCS_source_table_file_path
        """
        from google.cloud import storage
        scope_list = ["https://www.googleapis.com/auth/devstorage.read_only"]
        is_gcp = bool(os.environ.get("GOOGLE_CLOUD_PROJECT"))
        from google.oauth2.service_account import Credentials

        if is_gcp:
            secret_id = params.get("json_keyfile_GCP_secret_id")
            if not secret_id:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En GCP se debe proporcionar 'json_keyfile_GCP_secret_id'.")
            print("[AUTHENTICATION [INFO] 🔐] Entorno GCP detectado. Usando Secret Manager para GCS.", flush=True)
            from google.cloud import secretmanager
            import json
            project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
            if not project_id:
                raise ValueError("No se encontró la variable de entorno 'GOOGLE_CLOUD_PROJECT'.")
            client_sm = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client_sm.access_secret_version(name=secret_name)
            secret_string = response.payload.data.decode("UTF-8")
            secret_info = json.loads(secret_string)
            creds = Credentials.from_service_account_info(secret_info, scopes=scope_list)
        else:
            json_keyfile = params.get("json_keyfile_colab")
            if not json_keyfile:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En Colab se debe proporcionar 'json_keyfile_colab'.")
            print("[AUTHENTICATION [INFO] 🔐] Entorno Colab detectado. Usando json_keyfile_colab para GCS.", flush=True)
            creds = Credentials.from_service_account_file(json_keyfile, scopes=scope_list)

        try:
            bucket_name = params.get("GCS_source_table_bucket_name")
            file_path = params.get("GCS_source_table_file_path")
            if not bucket_name or not file_path:
                raise ValueError("[VALIDATION [ERROR ❌]] Faltan 'GCS_source_table_bucket_name' o 'GCS_source_table_file_path'.")
            client_storage = storage.Client(credentials=creds, project=os.environ.get("GOOGLE_CLOUD_PROJECT"))
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

    # ────────────────────────────── Proceso Principal ──────────────────────────────
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

    # Aplicar filtros comunes a todas las fuentes
    df = _apply_common_filters(df, params)
    return df


