import os
import json
import gzip
import requests
import tempfile
from google.cloud import storage, secretmanager
from google.oauth2 import service_account
import zipfile

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
