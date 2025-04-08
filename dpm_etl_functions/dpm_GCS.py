from dpm_common_functions import _ini_authenticate_API

import os
import json
import gzip
import zipfile
import requests
import tempfile

from google.cloud import storage, secretmanager
from google.oauth2 import service_account

# __________________________________________________________________________________________________________________________________________________________
# GCS_web_download_links_to_bucket
# __________________________________________________________________________________________________________________________________________________________

def GCS_web_download_links_to_bucket(params: dict) -> None:
    """
    Descarga archivos desde una lista de URLs y los carga en un bucket de Google Cloud Storage.
    Detecta si el archivo está comprimido (soporta .gz, .zip y .rar) y, de ser así, descomprime su contenido.
    En caso de que el archivo no esté comprimido, se sube directamente.
    Se utiliza archivos temporales para minimizar el almacenamiento en disco.

    Args:
        params (dict):
            - links (list): Lista de URLs (str) de los archivos a descargar.
            - bucket_name (str): Nombre del bucket destino en GCS.
            - project_id (str): ID del proyecto en GCP.
            - GCS_bucket_erase_previous_files (bool, opcional): Si es True, borra archivos previos en el bucket (default: False).
            - json_keyfile_GCP_secret_id (str, opcional): Secret ID para obtener credenciales desde Secret Manager (requerido en GCP).
            - json_keyfile_colab (str, opcional): Ruta al archivo JSON de credenciales (requerido en entornos no GCP).

    Returns:
        None

    Raises:
        ValueError: Si faltan parámetros obligatorios o de autenticación.
    """
    import os
    import json
    import gzip
    import requests
    import tempfile
    from google.cloud import storage, secretmanager
    from google.oauth2 import service_account
    import zipfile

    # ────────────────────────────── Validación de Parámetros ──────────────────────────────
    required_params = ['links', 'bucket_name', 'project_id']
    missing_params = [p for p in required_params if p not in params]
    if missing_params:
        raise ValueError(f"[VALIDATION [ERROR ❌]] Faltan parámetros obligatorios: {missing_params}")

    links_list = params.get('links')
    bucket_name_str = params.get('bucket_name')
    project_id_str = params.get('project_id')
    erase_previous_files_bool = params.get('GCS_bucket_erase_previous_files', False)

    # ────────────────────────────── Autenticación Dinámica ──────────────────────────────
    def _autenticar_gcp_storage(project_id: str) -> storage.Client:
        print("[AUTHENTICATION [INFO ℹ️]] Iniciando autenticación en Google Cloud Storage...", flush=True)
        if os.environ.get("GOOGLE_CLOUD_PROJECT"):
            secret_id_str = params.get("json_keyfile_GCP_secret_id")
            if not secret_id_str:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En GCP se debe proporcionar 'json_keyfile_GCP_secret_id'.")
            print("[AUTHENTICATION [INFO ℹ️]] Entorno GCP detectado. Obteniendo credenciales desde Secret Manager...", flush=True)
            client_sm = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{project_id}/secrets/{secret_id_str}/versions/latest"
            response = client_sm.access_secret_version(name=secret_name)
            secret_string = response.payload.data.decode("UTF-8")
            secret_info = json.loads(secret_string)
            credentials = service_account.Credentials.from_service_account_info(secret_info)
            print("[AUTHENTICATION [SUCCESS ✅]] Credenciales obtenidas desde Secret Manager.", flush=True)
        else:
            json_path_str = params.get("json_keyfile_colab")
            if not json_path_str:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En entornos no GCP se debe proporcionar 'json_keyfile_colab'.")
            print("[AUTHENTICATION [INFO ℹ️]] Entorno local/Colab detectado. Usando credenciales desde archivo JSON...", flush=True)
            credentials = service_account.Credentials.from_service_account_file(json_path_str)
            print("[AUTHENTICATION [SUCCESS ✅]] Credenciales cargadas desde archivo JSON.", flush=True)
        return storage.Client(credentials=credentials, project=project_id)

    try:
        client_storage = _autenticar_gcp_storage(project_id_str)
    except Exception as auth_e:
        print(f"[AUTHENTICATION [ERROR ❌]] Error en la autenticación: {auth_e}", flush=True)
        return

    # ────────────────────────────── Inicialización del Bucket ──────────────────────────────
    bucket = client_storage.bucket(bucket_name_str)

    if erase_previous_files_bool:
        print("[LOAD [START ▶️]] Eliminando archivos previos en el bucket...", flush=True)
        try:
            for blob in bucket.list_blobs():
                blob.delete()
                print(f"[LOAD [INFO ℹ️]] Eliminado: {blob.name}", flush=True)
            print("[LOAD [SUCCESS ✅]] Eliminación de archivos previos completada.", flush=True)
        except Exception as erase_e:
            print(f"[LOAD [ERROR ❌]] Error al eliminar archivos previos: {erase_e}", flush=True)

    # ────────────────────────────── Función Auxiliar: Descompresión ──────────────────────────────
    def _decompress_file(temp_file_path: str, original_filename: str) -> list:
        """
        Descomprime el archivo temporal según su extensión.
        Retorna una lista de tuplas (nombre_archivo, datos_bytes) para cada archivo descomprimido.
        """
        decompressed_files = []
        ext = os.path.splitext(original_filename)[1].lower()
        if ext == ".gz":
            target_filename = original_filename[:-3]
            with gzip.open(temp_file_path, 'rb') as f_in:
                file_data = f_in.read()
            decompressed_files.append((target_filename, file_data))
        elif ext == ".zip":
            with zipfile.ZipFile(temp_file_path, 'r') as zip_in:
                for inner_filename in zip_in.namelist():
                    if inner_filename.endswith('/'):
                        continue  # omitir directorios
                    file_data = zip_in.read(inner_filename)
                    # Se puede usar como prefijo el nombre base del archivo comprimido
                    target_filename = os.path.splitext(original_filename)[0] + "_" + os.path.basename(inner_filename)
                    decompressed_files.append((target_filename, file_data))
        elif ext == ".rar":
            try:
                import rarfile
            except ImportError:
                print(f"[DECOMPRESSION [ERROR ❌]] Módulo 'rarfile' no disponible. No se puede descomprimir {original_filename}.", flush=True)
                return []
            with rarfile.RarFile(temp_file_path, 'r') as rar_in:
                for inner_info in rar_in.infolist():
                    if inner_info.is_dir():
                        continue
                    file_data = rar_in.read(inner_info)
                    target_filename = os.path.splitext(original_filename)[0] + "_" + os.path.basename(inner_info.filename)
                    decompressed_files.append((target_filename, file_data))
        else:
            # No comprimido: leer el contenido completo
            with open(temp_file_path, 'rb') as f:
                file_data = f.read()
            decompressed_files.append((original_filename, file_data))
        return decompressed_files

    # ────────────────────────────── Proceso de Descarga, Descompresión y Carga ──────────────────────────────
    for url_str in links_list:
        try:
            print(f"[EXTRACTION [START ⏳]] Descargando {url_str} ...", flush=True)
            response = requests.get(url_str, stream=True)
            response.raise_for_status()

            total_length = response.headers.get('content-length')
            total_length_int = int(total_length) if total_length is not None else None

            # Crear archivo temporal para guardar el archivo descargado
            suffix = os.path.splitext(url_str)[1]  # p.ej. ".gz", ".zip", etc.
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
                downloaded_length_int = 0
                chunk_size_int = 8192
                for chunk in response.iter_content(chunk_size=chunk_size_int):
                    if chunk:
                        temp_file.write(chunk)
                        downloaded_length_int += len(chunk)
                        if total_length_int:
                            progress_int = int(50 * downloaded_length_int / total_length_int)
                            percent_int = int(100 * downloaded_length_int / total_length_int)
                            print(f"\r[DOWNLOAD PROGRESS] [{'█' * progress_int}{'.' * (50 - progress_int)}] {percent_int}% descargado", end='', flush=True)
                temp_file_path = temp_file.name
            print()  # Salto de línea tras barra de progreso
            print(f"[DOWNLOAD [SUCCESS ✅]] Descarga completada: {url_str}", flush=True)

            original_filename = os.path.basename(url_str)
            ext = os.path.splitext(original_filename)[1].lower()

            # ─── Descompresión (si es necesario) ───
            if ext in [".gz", ".zip", ".rar"]:
                print(f"[TRANSFORMATION [START ▶️]] Descomprimiendo {original_filename} ...", flush=True)
                decompressed_files = _decompress_file(temp_file_path, original_filename)
                if not decompressed_files:
                    print(f"[TRANSFORMATION [ERROR ❌]] No se pudo descomprimir {original_filename}. Se omitirá este archivo.", flush=True)
                    os.remove(temp_file_path)
                    continue
                print(f"[TRANSFORMATION [SUCCESS ✅]] Descompresión finalizada de {original_filename}.", flush=True)
            else:
                # Si no está comprimido, se lee el contenido completo
                with open(temp_file_path, 'rb') as f:
                    decompressed_files = [(original_filename, f.read())]

            # Borrar el archivo temporal descargado
            os.remove(temp_file_path)
            print(f"[CLEANUP [INFO ℹ️]] Archivo temporal borrado: {original_filename}", flush=True)

            # ─── Subida a GCS ───
            for upload_filename, file_data in decompressed_files:
                print(f"[LOAD [START ▶️]] Subiendo {upload_filename} al bucket '{bucket_name_str}' ...", flush=True)
                blob = bucket.blob(upload_filename)
                blob.upload_from_string(file_data, content_type='application/octet-stream')
                print(f"[LOAD [SUCCESS ✅]] Archivo {upload_filename} subido correctamente.", flush=True)

        except Exception as proc_e:
            print(f"[EXTRACTION [ERROR ❌]] Error procesando {url_str}: {proc_e}", flush=True)

    print("[END [FINISHED ✅]] Proceso completado.", flush=True)















# __________________________________________________________________________________________________________________________________________________________
# GCS_files_to_GBQ
# __________________________________________________________________________________________________________________________________________________________
def GCS_files_to_GBQ(params: dict) -> None:
    """
    Carga archivos desde GCS a BigQuery en chunks, soportando múltiples formatos (CSV, TSV, XLS, XLSX).
    Incorpora lógica de filtrado y reemplazo/sufijo en el nombre de la tabla final:
      - target_table_names_replace: dict con reemplazos para el nombre base.
      - target_table_names_suffix: str que se concatena al final, sin guion bajo automático.

    Args:
        params (dict):
            # === Credenciales / Proyecto ===
            - gcp_project_id (str): ID del proyecto en GCP.
            - gcs_bucket_name (str): Nombre del bucket en GCS.
            - gbq_dataset_id (str): Dataset de destino en BigQuery.
            - json_keyfile_GCP_secret_id (str, opcional): ID del secreto en Secret Manager (en entornos GCP).
            - json_keyfile_colab (str, opcional): Ruta al archivo JSON de credenciales (en Colab o local).

            # === Lista de archivos y/o Filtros ===
            - files_list (list[str]): Lista de archivos concretos a procesar en GCS.
              Si está vacío, se toma todo el bucket (aplicando filters_dic si 'use_bool' es True).
            - filters_dic (dict, opcional): Diccionario con filtros:
                  {
                      "use_bool": True,
                      "name_include_patterns_list": [...],
                      "name_exclude_patterns_list": [...],
                      "extension_include_patterns_list": [...],
                      "extension_exclude_patterns_list": [...],
                      "min_size_kb": 100,
                      "max_size_kb": 500000,
                      "modified_after_date": "2023-01-01",
                      "modified_before_date": None,
                      "include_subfolders_bool": True
                  }

            # === Config de inferencia / chunk ===
            - chunk_size (int, opcional): Número de filas por chunk. Default: 10000.
            - inference_threshold (float, opcional): Umbral de inferencia (0..1). Default: 0.95.
            - inference_field_type_chunk_size (int, opcional): Nº filas para inferir tipos. Default: 1000.
            - remove_local (bool, opcional): Si se elimina el archivo local tras procesarlo. Default: False.

            # === Claves para modificar el nombre final de la tabla en BigQuery ===
            - target_table_names_suffix (str): Cadena que se concatena directamente al final del nombre base.
            - target_table_names_replace (dict): Reemplazos en el nombre base. Ej. {"-utf8": "", "xxx": "YYY"}.

    Returns:
        None

    Raises:
        ValueError: Si faltan parámetros obligatorios o ocurre un error durante la autenticación/filtrado.
    """

    import os, sys, re, json
    import datetime
    import pandas as pd
    import numpy as np
    import dateparser
    import pandas_gbq
    from google.cloud import storage, bigquery, secretmanager
    from google.oauth2 import service_account
    import unicodedata

    # ─────────────────────────────────────────────
    # 1) VALIDACIÓN DE PARÁMETROS
    # ─────────────────────────────────────────────
    print("\n🔸🔸🔸 VALIDACIÓN DE PARÁMETROS 🔸🔸🔸", flush=True)
    gcp_project_id = params.get("gcp_project_id")
    gcs_bucket_name = params.get("gcs_bucket_name")
    gbq_dataset_id = params.get("gbq_dataset_id")
    files_list = params.get("files_list", [])
    filters_dic = params.get("filters_dic", {})

    chunk_size = params.get("chunk_size", 10000)
    remove_local = params.get("remove_local", False)
    inference_threshold = params.get("inference_threshold", 0.95)
    inference_field_type_chunk_size = params.get("inference_field_type_chunk_size", 1000)

    target_table_names_suffix = params.get("target_table_names_suffix", "")
    target_table_names_replace = params.get("target_table_names_replace", {})

    if not all([gcp_project_id, gcs_bucket_name, gbq_dataset_id]):
        raise ValueError("[VALIDATION [ERROR ❌]] Faltan parámetros obligatorios: 'gcp_project_id', 'gcs_bucket_name', 'gbq_dataset_id'.")

    # ─────────────────────────────────────────────
    # 2) AUTENTICACIÓN
    # ─────────────────────────────────────────────
    print("\n🔸🔸🔸 AUTENTICACIÓN 🔸🔸🔸", flush=True)
    def _autenticar_gcp(project_id: str):
        print("[AUTHENTICATION [INFO ℹ️]] Iniciando autenticación...", flush=True)
        is_colab = ('google.colab' in sys.modules)
        if is_colab:
            json_path = params.get("json_keyfile_colab")
            if not json_path:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En Colab se debe proporcionar 'json_keyfile_colab'.")
            print("[AUTHENTICATION [INFO ℹ️]] Entorno Colab detectado. Usando credenciales JSON desde:", json_path, flush=True)
            creds = service_account.Credentials.from_service_account_file(json_path)
            print("[AUTHENTICATION [SUCCESS ✅]] Credenciales cargadas (Colab).", flush=True)
            return creds
        if os.environ.get("GOOGLE_CLOUD_PROJECT"):
            secret_id = params.get("json_keyfile_GCP_secret_id")
            if not secret_id:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En GCP se debe proporcionar 'json_keyfile_GCP_secret_id'.")
            print("[AUTHENTICATION [INFO ℹ️]] Entorno GCP detectado. Obteniendo credenciales desde Secret Manager...", flush=True)
            client_sm = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client_sm.access_secret_version(name=secret_name)
            secret_string = response.payload.data.decode("UTF-8")
            secret_info = json.loads(secret_string)
            creds = service_account.Credentials.from_service_account_info(secret_info)
            print(f"[AUTHENTICATION [SUCCESS ✅]] Credenciales obtenidas desde Secret Manager (secret: {secret_id}).", flush=True)
            return creds
        json_path = params.get("json_keyfile_colab")
        if not json_path:
            raise ValueError("[AUTHENTICATION [ERROR ❌]] En entorno local se debe proporcionar 'json_keyfile_colab'.")
        print("[AUTHENTICATION [INFO ℹ️]] Entorno local detectado. Usando credenciales JSON desde:", json_path, flush=True)
        creds = service_account.Credentials.from_service_account_file(json_path)
        print("[AUTHENTICATION [SUCCESS ✅]] Credenciales cargadas (local).", flush=True)
        return creds

    credentials = _autenticar_gcp(gcp_project_id)
    storage_client = storage.Client(project=gcp_project_id, credentials=credentials)
    bq_client = bigquery.Client(project=gcp_project_id, credentials=credentials)
    print("[INFO ℹ️] Clientes GCP creados: Storage y BigQuery.", flush=True)

    # ─────────────────────────────────────────────
    # 3) FILTRO DE BLOBS
    # ─────────────────────────────────────────────
    print("\n🔸🔸🔸 FILTRO DE BLOBS EN GCS 🔸🔸🔸", flush=True)
    def _blob_passes_filters(blob, fdict: dict) -> bool:
        if not fdict.get("use_bool", False):
            return True

        blob_name = blob.name
        extension = os.path.splitext(blob_name)[1].lower()

        inc_list = fdict.get("name_include_patterns_list", [])
        if inc_list and not any(pat in blob_name for pat in inc_list):
            return False

        exc_list = fdict.get("name_exclude_patterns_list", [])
        if exc_list and any(pat in blob_name for pat in exc_list):
            return False

        ext_inc = fdict.get("extension_include_patterns_list", [])
        if ext_inc and extension not in ext_inc:
            return False

        ext_exc = fdict.get("extension_exclude_patterns_list", [])
        if ext_exc and extension in ext_exc:
            return False

        size_kb = blob.size / 1024 if blob.size else 0
        min_kb = fdict.get("min_size_kb")
        if min_kb and size_kb < min_kb:
            return False

        max_kb = fdict.get("max_size_kb")
        if max_kb and size_kb > max_kb:
            return False

        if blob.updated:
            mod_date = blob.updated.date()
            after_str = fdict.get("modified_after_date")
            if after_str:
                after_dt = datetime.datetime.strptime(after_str, "%Y-%m-%d").date()
                if mod_date < after_dt:
                    return False

            before_str = fdict.get("modified_before_date")
            if before_str:
                before_dt = datetime.datetime.strptime(before_str, "%Y-%m-%d").date()
                if mod_date > before_dt:
                    return False

        return True

    bucket = storage_client.bucket(gcs_bucket_name)
    prefix = ""
    delimiter = None
    if filters_dic.get("use_bool") and not filters_dic.get("include_subfolders_bool", False):
        delimiter = "/"

    all_blobs = list(storage_client.list_blobs(gcs_bucket_name, prefix=prefix, delimiter=delimiter))
    print(f"[INFO ℹ️] Número total de blobs en bucket '{gcs_bucket_name}': {len(all_blobs)}", flush=True)

    if not files_list:
        candidate_files = [blob.name for blob in all_blobs if _blob_passes_filters(blob, filters_dic)]
    else:
        candidate_files = []
        for blob in all_blobs:
            if blob.name in files_list and _blob_passes_filters(blob, filters_dic):
                candidate_files.append(blob.name)

    if not candidate_files:
        print("[INFO ℹ️] No se encontraron archivos que cumplan los filtros o coincidencias.", flush=True)
        return

    print(f"[INFO ℹ️] Archivos candidatos a procesar: {candidate_files}", flush=True)

    # ─────────────────────────────────────────────
    # 4) NORMALIZAR COLUMNAS
    # ─────────────────────────────────────────────
    print("\n🔸🔸🔸 NORMALIZACIÓN DE COLUMNAS 🔸🔸🔸", flush=True)
    def normalize_column_name(name: str) -> str:
        name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')
        name = name.replace('ñ', 'n').replace('Ñ', 'N')
        normalized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        normalized = re.sub(r"_+", "_", normalized).strip("_")
        return normalized[:300]

    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [normalize_column_name(c) for c in df.columns]
        obj_cols = df.select_dtypes(include=["object"]).columns
        for col in obj_cols:
            df[col] = df[col].str.replace('"', '', regex=False).str.strip()
        return df

    # ─────────────────────────────────────────────
    # 5) INFERENCIA DE TIPOS DE DATOS
    # ─────────────────────────────────────────────
    print("\n🔸🔸🔸 INFERENCIA DE TIPOS DE DATOS 🔸🔸🔸", flush=True)
    def _es_fecha(valor) -> bool:
        val_str = str(valor).strip()
        if not any(ch.isdigit() for ch in val_str):
            return False
        try:
            parsed = dateparser.parse(val_str, languages=['en','es'], settings={'DATE_ORDER': 'DMY'})
            return parsed is not None
        except Exception:
            return False

    def _es_booleano(valor) -> bool:
        if isinstance(valor, bool):
            return True
        if isinstance(valor, (int, float)):
            return valor in [0, 1]
        if isinstance(valor, str):
            return valor.strip().lower() in {"true", "false", "yes", "no", "si", "0", "1"}
        return False

    def _normalizar_bool(valor):
        if isinstance(valor, bool):
            return valor
        if isinstance(valor, (int, float)):
            return (valor == 1)
        if isinstance(valor, str):
            v = valor.strip().lower()
            if v in {"true", "yes", "si", "1"}:
                return True
            elif v in {"false", "no", "0"}:
                return False
        return None

    def _es_entero(valor) -> bool:
        try:
            if isinstance(valor, (int, np.integer)):
                return True
            if isinstance(valor, float) and valor.is_integer():
                return True
            int(str(valor))
            return True
        except:
            return False

    def _es_flotante(valor) -> bool:
        try:
            float(valor)
            return True
        except:
            return False

    def _inferir_tipo_serie(serie: pd.Series, threshold: float) -> str:
        datos = serie.dropna()
        if datos.empty:
            return "STRING"
        total = len(datos)
        bool_count = datos.apply(_es_booleano).sum()
        if (bool_count / total) >= threshold:
            bool_values = datos[datos.apply(_es_booleano)].apply(_normalizar_bool)
            if bool_values.nunique() <= 2:
                return "BOOL"
        if (datos.apply(_es_entero).sum() / total) >= threshold:
            return "INT64"
        if (datos.apply(_es_flotante).sum() / total) >= threshold:
            return "FLOAT64"
        if (datos.apply(_es_fecha).sum() / total) >= threshold:
            return "TIMESTAMP"
        return "STRING"

    def _inferir_esquema(df: pd.DataFrame, threshold: float) -> dict:
        esquema = {}
        for col in df.columns:
            col_l = col.lower()
            if "fecha" in col_l or "date" in col_l:
                esquema[col] = "TIMESTAMP"
            else:
                esquema[col] = _inferir_tipo_serie(df[col], threshold)
        return esquema

    # ─────────────────────────────────────────────
    # 6) CONVERSIÓN DE CHUNKS
    # ─────────────────────────────────────────────
    print("\n🔸🔸🔸 CONVERSIÓN DE CHUNKS 🔸🔸🔸", flush=True)
    def _convertir_chunk(chunk_df: pd.DataFrame, esquema: dict) -> pd.DataFrame:
        for col, tipo in esquema.items():
            if tipo == "INT64":
                try:
                    converted = pd.to_numeric(chunk_df[col], errors="coerce")
                    if not (converted.dropna() % 1 == 0).all():
                        raise ValueError("La columna contiene valores decimales.")
                    chunk_df[col] = converted.astype("Int64")
                except Exception as ex:
                    print(f"[WARNING ⚠️] Error al convertir '{col}' a INT64 ({ex}); se convertirá a STRING.", flush=True)
                    chunk_df[col] = chunk_df[col].astype(str)
            elif tipo == "FLOAT64":
                chunk_df[col] = pd.to_numeric(chunk_df[col], errors="coerce")
            elif tipo == "TIMESTAMP":
                chunk_df[col] = pd.to_datetime(chunk_df[col], errors="coerce", dayfirst=True)
            elif tipo == "BOOL":
                chunk_df[col] = chunk_df[col].apply(_normalizar_bool)
            else:
                # Para STRING: se reemplaza "\N" por np.nan
                chunk_df[col] = chunk_df[col].astype(str)
                chunk_df[col] = chunk_df[col].replace(r'\N', np.nan)
        return chunk_df

    # ─────────────────────────────────────────────
    # 7) PROCESAR ARCHIVOS
    # ─────────────────────────────────────────────
    print("\n🔸🔸🔸 INICIO DEL PROCESAMIENTO DE ARCHIVOS 🔸🔸🔸", flush=True)
    for file_path in candidate_files:
        print("\n\n======================================================", flush=True)
        print(f"🔹🔹🔹 PROCESADO DEL ARCHIVO {file_path} 🔹🔹🔹", flush=True)
        print("======================================================", flush=True)
        
        # Extracción: descarga el blob y muestra detalles
        local_filename = file_path.replace("/", "_")
        try:
            blob = bucket.blob(file_path)
            blob.download_to_filename(local_filename)
            print(f"[EXTRACTION [SUCCESS ✅]] Archivo descargado localmente: {local_filename}", flush=True)
            print(f"[INFO ℹ️] Detalles del blob - Tamaño: {blob.size} bytes, Última actualización: {blob.updated}", flush=True)
        except Exception as e:
            print(f"[EXTRACTION [ERROR ❌]] Error al descargar {file_path}: {e}", flush=True)
            continue

        ext = os.path.splitext(local_filename)[1].lower()

        # 7A) Lectura de muestra para inferencia
        print("\n🔹🔹🔹 LECTURA DE MUESTRA PARA INFERENCIA 🔹🔹🔹", flush=True)
        print(f"[TRANSFORMATION [INFO ℹ️]] Leyendo {inference_field_type_chunk_size} filas para inferencia desde {local_filename}", flush=True)
        df_inferencia = None
        try:
            if ext in [".csv", ".tsv"]:
                sep = ";" if ext == ".csv" else "\t"
                df_inferencia = pd.read_csv(
                    local_filename,
                    nrows=inference_field_type_chunk_size,
                    delimiter=sep,
                    encoding="utf-8",
                    low_memory=False,
                    dtype=str
                )
            elif ext in [".xls", ".xlsx"]:
                df_inferencia = pd.read_excel(local_filename, nrows=inference_field_type_chunk_size, dtype=str)
            else:
                print(f"[EXTRACTION [ERROR ❌]] Tipo de archivo no soportado: {ext}", flush=True)
                continue
        except Exception as e:
            print(f"[TRANSFORMATION [ERROR ❌]] No se pudo leer la muestra para inferencia: {e}", flush=True)
            continue

        df_inferencia = _normalize_columns(df_inferencia)
        esquema_inferido = _inferir_esquema(df_inferencia, inference_threshold)
        print(f"[TRANSFORMATION [SUCCESS ✅]] Esquema inferido para muestra de {len(df_inferencia)} filas: {esquema_inferido}", flush=True)

        # 7B) Lectura completa en chunks
        print("\n🔹🔹🔹 LECTURA COMPLETA EN CHUNKS 🔹🔹🔹", flush=True)
        def _get_df_iterator():
            if ext in [".csv", ".tsv"]:
                sep = ";" if ext == ".csv" else "\t"
                it = pd.read_csv(local_filename,
                                 chunksize=chunk_size,
                                 delimiter=sep,
                                 encoding="utf-8",
                                 low_memory=False,
                                 dtype=str)
                for chunk in it:
                    yield _normalize_columns(chunk)
            else:
                df_full = pd.read_excel(local_filename, dtype=str)
                df_full = _normalize_columns(df_full)
                for i in range(0, len(df_full), chunk_size):
                    yield df_full[i:i+chunk_size]

        try:
            df_iterator = _get_df_iterator()
            print("[TRANSFORMATION [INFO ℹ️]] Iterador de chunks creado exitosamente.", flush=True)
        except Exception as e:
            print(f"[TRANSFORMATION [ERROR ❌]] Error al crear iterador de chunks: {e}", flush=True)
            continue

        # 7C) Construcción del nombre final de la tabla
        print("\n🔹🔹🔹 CONSTRUCCIÓN DEL NOMBRE FINAL DE LA TABLA 🔹🔹🔹", flush=True)
        base_table_name = os.path.splitext(local_filename)[0]
        for old_str, new_str in target_table_names_replace.items():
            base_table_name = base_table_name.replace(old_str, new_str)

        final_table_name = base_table_name + target_table_names_suffix
        final_table_name = re.sub(r"[^a-zA-Z0-9_]", "_", final_table_name)
        final_table_name = re.sub(r"_+", "_", final_table_name).strip("_")[:300]

        table_id = f"{gbq_dataset_id}.{final_table_name}"
        print(f"[INFO ℹ️] Tabla de destino en BigQuery: {table_id}", flush=True)

        chunk_index = 0
        proceso_exitoso = True

        # 7D) Procesar cada chunk y cargar a BigQuery
        print("\n🔹🔹🔹 PROCESAMIENTO DE CHUNKS Y CARGA A BIGQUERY 🔹🔹🔹", flush=True)
        for chunk_df in df_iterator:
            chunk_df.dropna(how='all', inplace=True)
            n_rows = len(chunk_df)
            print(f"[TRANSFORMATION [INFO ℹ️]] Chunk #{chunk_index} contiene {n_rows} filas", flush=True)
            if n_rows == 0:
                chunk_index += 1
                continue

            chunk_df = _convertir_chunk(chunk_df, esquema_inferido)
            modo_existencia = "replace" if chunk_index == 0 else "append"
            try:
                pandas_gbq.to_gbq(
                    chunk_df,
                    table_id,
                    project_id=gcp_project_id,
                    if_exists=modo_existencia,
                    credentials=credentials,
                    verbose=False
                )
                print(f"[LOAD [SUCCESS ✅]] Chunk #{chunk_index} cargado en {table_id}", flush=True)
            except Exception as e:
                print(f"[LOAD [ERROR ❌]] Error al cargar chunk #{chunk_index} en {table_id}: {e}", flush=True)
                proceso_exitoso = False
                break

            chunk_index += 1

        # 7E) Limpieza local
        print("\n🔹🔹🔹 LIMPIEZA LOCAL 🔹🔹🔹", flush=True)
        if remove_local:
            try:
                os.remove(local_filename)
                print(f"[CLEANUP [INFO ℹ️]] Archivo local eliminado: {local_filename}", flush=True)
            except Exception as e:
                print(f"[CLEANUP [WARNING ⚠️]] No se pudo eliminar {local_filename}: {e}", flush=True)

        if proceso_exitoso:
            print(f"\n[END [FINISHED ✅]] Archivo {file_path} procesado exitosamente.", flush=True)
        else:
            print(f"\n[END [FAILED ❌]] Archivo {file_path} no se procesó correctamente.", flush=True)

    print("\n🔸🔸🔸 PROCESO GCS_files_to_GBQ() COMPLETADO 🔸🔸🔸\n", flush=True)
