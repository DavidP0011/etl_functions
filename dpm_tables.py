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
    Extrae datos desde distintos orígenes (archivo o Google Sheets) y los convierte en un DataFrame.

    Parámetros en params:
      - source_table_file_path (str, opcional): Ruta al archivo. Si está vacío, se usará Google Sheets.
      - source_table_spreadsheet_id (str, opcional): URL o ID de la hoja de cálculo (usado si source_table_file_path está vacío).
      - source_table_worksheet_name (str, opcional): Nombre de la pestaña a extraer (requiere source_table_spreadsheet_id).
      - source_table_row_start (int, opcional): Primera fila a leer (0-indexado). Por defecto 0.
      - source_table_row_end (int, opcional): Última fila a leer (excluyente). Si es None, lee hasta el final.
      - source_table_filter_skip_row_empty_use (bool, opcional): Si True, elimina filas completamente vacías. Por defecto True.
      - source_table_col_start (int, opcional): Primera columna a leer (0-indexado). Por defecto 0.
      - source_table_col_end (int, opcional): Última columna a leer (excluyente). Si es None, lee todas.
      - json_keyfile (str, requerido): Ruta al archivo JSON de credenciales de GCP.

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

    # Para Google Sheets
    import gspread
    from google.auth import default
    from oauth2client.service_account import ServiceAccountCredentials
    from google.auth.exceptions import DefaultCredentialsError

    # Para archivos (en Colab)
    from google.colab import files

    # ────────────────────────────── Grupo: Encabezados ──────────────────────────────
    def _imprimir_encabezado(mensaje: str) -> None:
        print(f"\n🔹🔹🔹 {mensaje} 🔹🔹🔹\n", flush=True)

    # ────────────────────────────── Grupo: Validación de Parámetros ──────────────────────────────
    def _validar_comun(params: dict) -> None:
        if 'json_keyfile' not in params or not params['json_keyfile']:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta el parámetro obligatorio 'json_keyfile' para autenticación.")

    # ────────────────────────────── Grupo: Determinación del Origen ──────────────────────────────
    def _es_fuente_archivo(params: dict) -> bool:
        return bool(params.get('source_table_file_path', '').strip())

    def _es_fuente_gsheet(params: dict) -> bool:
        return (not _es_fuente_archivo(params)) and (
            bool(params.get('source_table_spreadsheet_id', '').strip()) and 
            bool(params.get('source_table_worksheet_name', '').strip())
        )

    # ────────────────────────────── Grupo: Fuente – Archivo ──────────────────────────────
    def _leer_archivo(params: dict) -> pd.DataFrame:
        _imprimir_encabezado("[START 🚀] Iniciando carga del archivo")
        # Parámetros específicos del archivo
        file_path = params.get('source_table_file_path')
        row_start = params.get('source_table_row_start', 0)
        row_end = params.get('source_table_row_end', None)
        row_skip_empty = params.get('source_table_filter_skip_row_empty_use', True)
        col_start = params.get('source_table_col_start', 0)
        col_end = params.get('source_table_col_end', None)

        if not file_path:
            print("[EXTRACTION [WARNING ⚠️]] No se proporcionó 'source_table_file_path'. Suba un archivo desde su ordenador:", flush=True)
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
            nrows = (row_end - row_start) if row_end is not None else None

            if ext in ['.xls', '.xlsx']:
                engine = 'xlrd' if ext == '.xls' else 'openpyxl'
                df = pd.read_excel(file_input, engine=engine, skiprows=row_start, nrows=nrows)
            elif ext == '.csv':
                df = pd.read_csv(file_input, skiprows=row_start, nrows=nrows, sep=',')
            elif ext == '.tsv':
                df = pd.read_csv(file_input, skiprows=row_start, nrows=nrows, sep='\t')
            else:
                raise RuntimeError(f"[EXTRACTION [ERROR ❌]] Extensión de archivo '{ext}' no soportada.")

            df = df.iloc[:, col_start:col_end] if col_end is not None else df.iloc[:, col_start:]
            print("[TRANSFORMATION [START 🔄]] Aplicando procesamiento de datos...", flush=True)

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

    # ────────────────────────────── Grupo: Fuente – Google Sheets ──────────────────────────────
    
    def _leer_google_sheet(params: dict) -> pd.DataFrame:
        """
        Función para extraer datos desde Google Sheets en entornos GCP o locales.
        Soporta autenticación automática en GCP o mediante un archivo JSON en Colab/local.
        """
        from google.auth import default
        from googleapiclient.discovery import build
        import pandas as pd
        import os
    
        # Extracción de parámetros
        spreadsheet_id = params.get("source_table_spreadsheet_id")
        worksheet_name = params.get("source_table_worksheet_name")
        json_keyfile_str = params.get("json_keyfile")
    
        if not spreadsheet_id or not worksheet_name:
            raise ValueError("[VALIDATION [ERROR ❌]] Faltan 'source_table_spreadsheet_id' o 'source_table_worksheet_name'.")
    
        try:
            # Definir los scopes necesarios
            scope_list = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
    
            # Verificar si el entorno es GCP (Vertex AI/Colab Enterprise)
            is_gcp = bool(os.environ.get("GOOGLE_CLOUD_PROJECT"))
    
            if is_gcp:
                print("[AUTHENTICATION [SUCCESS ✅]] Entorno GCP detectado. Autenticación automática.", flush=True)
                creds, _ = default(scopes=scope_list)
            else:
                from google.oauth2.service_account import Credentials
                if not json_keyfile_str:
                    raise ValueError("[AUTHENTICATION [ERROR ❌]] En Colab se debe proporcionar 'json_keyfile'.")
                print("[AUTHENTICATION [INFO] 🔐] Entorno local/Colab detectado. Usando JSON de credenciales.", flush=True)
                creds = Credentials.from_service_account_file(json_keyfile_str, scopes=scope_list)
    
            # Construir el servicio de Google Sheets
            service = build('sheets', 'v4', credentials=creds)
    
            # Definir el rango de datos a extraer (por ejemplo, toda la hoja)
            range_name = f"{worksheet_name}"
    
            print("[EXTRACTION [START ⏳]] Extrayendo datos de Google Sheets...", flush=True)
    
            # Llamar a la API de Google Sheets
            result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
            data = result.get('values', [])
    
            if not data:
                print("[EXTRACTION [WARNING ⚠️]] No se encontraron datos en la hoja especificada.", flush=True)
                return pd.DataFrame()
    
            # Convertir los datos a un DataFrame
            df = pd.DataFrame(data[1:], columns=data[0])
            print(f"[EXTRACTION [SUCCESS ✅]] Datos extraídos con éxito de la hoja '{worksheet_name}'.", flush=True)
    
            return df
    
        except Exception as e:
            raise ValueError(f"[EXTRACTION [ERROR ❌]] Error al extraer datos de Google Sheets: {e}")



    # ────────────────────────────── Grupo: Proceso Principal ──────────────────────────────
    _validar_comun(params)
    if _es_fuente_archivo(params):
        return _leer_archivo(params)
    elif _es_fuente_gsheet(params):
        return _leer_google_sheet(params)
    else:
        raise ValueError("[VALIDATION [ERROR ❌]] No se han proporcionado parámetros válidos para identificar el origen de datos. "
                         "Defina 'source_table_file_path' para archivos o 'source_table_spreadsheet_id' y 'source_table_worksheet_name' para Google Sheets.")
