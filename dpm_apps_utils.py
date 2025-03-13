# __________________________________________________________________________________________________________________________________________________________
# files_path_collect_df
# __________________________________________________________________________________________________________________________________________________________
def files_path_collect_df(config: dict) -> "pd.DataFrame":
    """
    Busca archivos de video en una ruta (local o Google Drive), extrae sus propiedades usando ffprobe
    y devuelve un DataFrame con los resultados.

    Args:
        config (dict):
            - video_files_root_path (str): Ruta raíz donde buscar archivos de video. Puede ser una ruta local o una URL de Google Drive.
            - video_files_target_search_folder (list): Lista de subcarpetas de interés dentro de la ruta raíz.
            - video_files_target_search_extension (list): Lista de extensiones de archivo (e.g., [".mp4"]).
            - ini_environment_identificated (str): Entorno de ejecución ("LOCAL", "COLAB", "COLAB_ENTERPRISE" o un project_id).
            - json_keyfile_local (str): Ruta del archivo JSON de credenciales para entorno LOCAL.
            - json_keyfile_colab (str): Ruta del archivo JSON de credenciales para entorno COLAB.
            - json_keyfile_GCP_secret_id (str): Identificador de secreto para entornos GCP.

    Returns:
        pd.DataFrame: DataFrame con la información de los archivos de video encontrados y sus propiedades.

    Raises:
        ValueError: Si falta algún parámetro obligatorio o no se encuentran archivos que cumplan los criterios.
        NotImplementedError: Si se intenta procesar una ruta de Google Drive.
        Exception: Para errores inesperados durante el proceso.
    """
    import os
    import subprocess
    import json
    import pandas as pd
    from datetime import datetime
    from time import time

    try:
        print("\n[PROCESS START ▶️] Iniciando la recolección de archivos.", flush=True)
        start_time = time()

        # ─────────── Validación de Parámetros ───────────
        video_root_path = config.get("video_files_root_path")
        if not video_root_path:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta 'video_files_root_path' en config.")

        video_target_folders = config.get("video_files_target_search_folder", [])
        video_exts = config.get("video_files_target_search_extension", [])
        if not video_exts:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta 'video_files_target_search_extension' o está vacío en config.")

        env_ident = config.get("ini_environment_identificated")
        if not env_ident:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta 'ini_environment_identificated' en config.")

        if env_ident == "LOCAL":
            json_keyfile = config.get("json_keyfile_local")
        elif env_ident == "COLAB":
            json_keyfile = config.get("json_keyfile_colab")
        else:  # Para COLAB_ENTERPRISE o project_id
            json_keyfile = config.get("json_keyfile_GCP_secret_id")
        if not json_keyfile:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta la clave de credenciales correspondiente para el entorno especificado.")

        print("[VALIDATION SUCCESS ✅] Parámetros validados correctamente.", flush=True)

        # ─────────── Gestión de la Ruta (Local o Google Drive) ───────────
        if video_root_path.startswith("https://"):
            print("[FILE SEARCH WARNING ⚠️] Funcionalidad para rutas de Google Drive no implementada.", flush=True)
            raise NotImplementedError("Extracción de archivos desde Google Drive no está implementada.")

        # ─────────── Función Interna: Búsqueda de Archivos ───────────
        def _find_files_in_folders(root_path: str, target_folders: list, file_exts: list) -> pd.DataFrame:
            results = []
            for current_root, dirs, files in os.walk(root_path):
                current_folder = os.path.basename(current_root)
                if target_folders and current_folder not in target_folders:
                    continue
                for file in files:
                    file_name, file_ext = os.path.splitext(file)
                    if file_ext.lower() in [ext.lower() for ext in file_exts]:
                        file_path = os.path.join(current_root, file)
                        results.append({
                            "video_file_path": file_path,
                            "video_file_name": file
                        })
                        print(f"[LOCATED FILE INFO ℹ️] Archivo localizado: {file} (Ruta: {file_path})", flush=True)
            if not results:
                print("[FILE SEARCH WARNING ⚠️] No se encontraron archivos que cumplan con los criterios.", flush=True)
                return pd.DataFrame()
            return pd.DataFrame(results)

        print(f"[FILE SEARCH START ▶️] Buscando archivos en '{video_root_path}' con extensiones: {video_exts}.", flush=True)
        df_paths = _find_files_in_folders(video_root_path, video_target_folders, video_exts)
        if df_paths.empty:
            raise ValueError("[VALIDATION [ERROR ❌]] No se encontraron archivos que cumplan con los criterios especificados.")
        print(f"[FILE SEARCH SUCCESS ✅] Total de archivos encontrados: {len(df_paths)}.", flush=True)

        # ─────────── Extracción de Metadatos ───────────
        total_files = len(df_paths)
        extracted_results = []

        print("[METADATA EXTRACTION START ▶️] Iniciando extracción de metadatos de videos.", flush=True)
        for idx, file_path in enumerate(df_paths['video_file_path'], start=1):
            metadata = {}
            try:
                file_size_mb = os.path.getsize(file_path) // (1024 * 1024)
                result = subprocess.run([
                    'ffprobe', '-v', 'error', '-print_format', 'json',
                    '-show_streams', '-show_format', file_path
                ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                info = json.loads(result.stdout)

                video_codec = audio_codec = None
                video_bitrate_kbps = audio_bitrate_kbps = 0
                video_width = video_height = None
                video_fps = 0
                audio_channels = audio_sample_rate_hz = 0
                duration_hms = "00:00:00"
                duration_ms = 0

                if 'streams' in info:
                    for stream in info['streams']:
                        if stream.get('codec_type') == 'video':
                            video_codec = stream.get('codec_name')
                            video_bitrate_kbps = int(stream.get('bit_rate', 0)) // 1000
                            video_width = stream.get('width')
                            video_height = stream.get('height')
                            if 'r_frame_rate' in stream:
                                try:
                                    num, den = map(int, stream['r_frame_rate'].split('/'))
                                    video_fps = num / den if den != 0 else 0
                                except Exception:
                                    video_fps = 0
                        elif stream.get('codec_type') == 'audio':
                            audio_codec = stream.get('codec_name')
                            audio_bitrate_kbps = int(stream.get('bit_rate', 0)) // 1000
                            audio_channels = stream.get('channels', 0)
                            audio_sample_rate_hz = int(stream.get('sample_rate', 0))
                if 'format' in info:
                    duration = float(info['format'].get('duration', 0))
                    duration_ms = int(duration * 1000)
                    duration_hms = "{:02d}:{:02d}:{:02d}".format(
                        int(duration) // 3600, (int(duration) % 3600) // 60, int(duration) % 60
                    )

                metadata = {
                    "file_name": os.path.basename(file_path),
                    "file_path": file_path,
                    "file_creation_date": datetime.fromtimestamp(os.path.getctime(file_path)).strftime('%Y-%m-%d %H:%M:%S'),
                    "file_last_modified_date": datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d %H:%M:%S'),
                    "file_scrap_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "file_size_mb": file_size_mb,
                    "duration_hms": duration_hms,
                    "duration_ms": duration_ms,
                    "video_codec": video_codec,
                    "video_bitrate_kbps": video_bitrate_kbps,
                    "video_fps": video_fps,
                    "video_resolution": f"{video_width}x{video_height}" if video_width and video_height else "unknown",
                    "audio_codec": audio_codec,
                    "audio_bitrate_kbps": audio_bitrate_kbps,
                    "audio_channels": audio_channels,
                    "audio_sample_rate_hz": audio_sample_rate_hz,
                }
                print(f"[METADATA EXTRACTION INFO ℹ️] Procesado: {metadata['file_name']} | Duración: {metadata['duration_hms']} | Resolución: {metadata['video_resolution']}", flush=True)
            except Exception as e:
                print(f"[METADATA EXTRACTION ERROR ❌] Error al procesar {os.path.basename(file_path)}: {e}", flush=True)
                metadata = {
                    "file_name": os.path.basename(file_path),
                    "file_path": file_path,
                    "file_creation_date": "unknown",
                    "file_last_modified_date": "unknown",
                    "file_scrap_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "file_size_mb": 0,
                    "duration_hms": "00:00:00",
                    "duration_ms": 0,
                    "video_codec": "unknown",
                    "video_bitrate_kbps": 0,
                    "video_fps": 0,
                    "video_resolution": "unknown",
                    "audio_codec": "unknown",
                    "audio_bitrate_kbps": 0,
                    "audio_channels": 0,
                    "audio_sample_rate_hz": 0,
                }
            extracted_results.append(metadata)

            # Mostrar barra de progreso
            progress = (idx / total_files) * 100
            progress_bar = '[' + '=' * int(progress // 5) + ' ' * (20 - int(progress // 5)) + ']'
            print(f"[METADATA EXTRACTION PROGRESS] {progress_bar} {progress:.2f}% completado ({idx}/{total_files})", flush=True)

        print("[METADATA EXTRACTION SUCCESS ✅] Extracción de metadatos completada.", flush=True)

        # ─────────── Generación del DataFrame Final ───────────
        df_paths_properties = pd.DataFrame(extracted_results)[[
            "file_name",
            "file_path",
            "file_creation_date",
            "file_last_modified_date",
            "file_scrap_date",
            "file_size_mb",
            "duration_hms",
            "duration_ms",
            "video_codec",
            "video_bitrate_kbps",
            "video_fps",
            "video_resolution",
            "audio_codec",
            "audio_bitrate_kbps",
            "audio_channels",
            "audio_sample_rate_hz",
        ]]

        # (Opcional) Guardar respaldo local
        backup_csv_path = "video_files_backup.csv"
        df_paths_properties.to_csv(backup_csv_path, index=False)
        print(f"[BACKUP INFO ℹ️] Datos respaldados localmente en: {backup_csv_path}", flush=True)

        total_videos = len(df_paths_properties)
        process_duration = time() - start_time
        print(f"[PROCESS METRICS INFO ℹ️] Total de videos procesados: {total_videos} | Duración: {process_duration:.2f} segundos", flush=True)

        print("[PROCESS END [FINISHED ✅]] Proceso completado exitosamente.", flush=True)
        return df_paths_properties

    except ValueError as ve:
        print(f"[PROCESS END FAILED ❌] Error de validación: {ve}", flush=True)
        return None
    except Exception as e:
        print(f"[PROCESS END FAILED ❌] Error inesperado: {e}", flush=True)
        return None
    





















# __________________________________________________________________________________________________________________________________________________________
# df_to_whisper_transcribe_to_spreadsheet
# __________________________________________________________________________________________________________________________________________________________
def df_to_whisper_transcribe_to_spreadsheet(config: dict) -> None:
    """
    Transcribe archivos de vídeo usando un modelo Whisper y escribe los resultados en una hoja de cálculo de Google Sheets.

    Args:
        config (dict): Diccionario de configuración que debe incluir:
            - source_files_path_table_df (pd.DataFrame): DataFrame con al menos la columna especificada en 'field_name_for_file_path'.
            - target_files_path_table_spreadsheet_url (str): URL de la hoja de cálculo destino.
            - target_files_path_table_spreadsheet_worksheet (str): Nombre de la worksheet destino.
            - field_name_for_file_path (str): Nombre de la columna con la ruta del vídeo.
            - whisper_model_size (str): Tamaño del modelo Whisper (ej. "small", "medium", etc.).
            - whisper_language (str, opcional): Idioma de la transcripción (default: "en").
            - ini_environment_identificated (str): Identificador del entorno ("LOCAL", "COLAB", "COLAB_ENTERPRISE" o un project_id).
            - json_keyfile_local (str): Ruta del archivo JSON de credenciales para entorno LOCAL.
            - json_keyfile_colab (str): Ruta del archivo JSON de credenciales para entorno COLAB.
            - json_keyfile_GCP_secret_id (str): ID del secreto para entornos GCP.

    Returns:
        None

    Raises:
        ValueError: Si falta algún parámetro obligatorio o si el DataFrame fuente está vacío o no contiene la columna requerida.
        Exception: Para otros errores inesperados.
    """
    import os
    import time
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    from datetime import datetime
    import whisper
    import torch

    def _trocear_texto(texto: str, max_chars: int = 50000, max_partes: int = 10) -> list:
        """
        Trocea un texto en partes de longitud <= max_chars y retorna una lista de longitud max_partes.
        """
        trozos = [texto[i:i + max_chars] for i in range(0, len(texto), max_chars)]
        trozos = trozos[:max_partes]
        if len(trozos) < max_partes:
            trozos += [""] * (max_partes - len(trozos))
        return trozos

    def _process_transcription() -> None:
        # Validación inicial de parámetros
        required_keys = [
            "source_files_path_table_df",
            "target_files_path_table_spreadsheet_url",
            "target_files_path_table_spreadsheet_worksheet",
            "field_name_for_file_path",
            "whisper_model_size"
        ]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"[VALIDATION [ERROR ❌]] Falta el parámetro obligatorio: '{key}'.")
            # Para el DataFrame se evita evaluar su veracidad de forma ambigua
            if key != "source_files_path_table_df" and not config.get(key):
                raise ValueError(f"[VALIDATION [ERROR ❌]] El parámetro '{key}' está vacío.")

        # Extraer parámetros
        source_df = config["source_files_path_table_df"]
        if source_df is None:
            raise ValueError("[VALIDATION [ERROR ❌]] El DataFrame fuente no puede ser None.")
        if source_df.empty:
            raise ValueError("[VALIDATION [ERROR ❌]] El DataFrame fuente está vacío.")
        field_name = config["field_name_for_file_path"]
        if field_name not in source_df.columns:
            raise ValueError(f"[VALIDATION [ERROR ❌]] La columna '{field_name}' no existe en el DataFrame fuente.")

        target_spreadsheet_url = config["target_files_path_table_spreadsheet_url"]
        target_worksheet_name = config["target_files_path_table_spreadsheet_worksheet"]
        whisper_model_size = config["whisper_model_size"]
        whisper_language = config.get("whisper_language", "en")

        # Configurar credenciales según el entorno
        ini_env = config.get("ini_environment_identificated")
        if ini_env == "LOCAL":
            credentials_path = config.get("json_keyfile_local")
        elif ini_env == "COLAB":
            credentials_path = config.get("json_keyfile_colab")
        else:
            credentials_path = config.get("json_keyfile_GCP_secret_id")
        if not credentials_path:
            raise ValueError("[VALIDATION [ERROR ❌]] Credenciales no definidas para el entorno especificado.")

        # Autenticación con Google Sheets
        print("🔹🔹🔹 [START ▶️] Autenticando con Google Sheets 🔹🔹🔹", flush=True)
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        client = gspread.authorize(creds)
        print("[AUTH SUCCESS ✅] Autenticación exitosa.", flush=True)

        # Preparar la hoja destino
        print(f"🔹🔹🔹 [START ▶️] Preparando hoja destino: {target_worksheet_name} 🔹🔹🔹", flush=True)
        spreadsheet_dest = client.open_by_url(target_spreadsheet_url)
        try:
            destination_sheet = spreadsheet_dest.worksheet(target_worksheet_name)
        except gspread.WorksheetNotFound:
            print("[INFO ℹ️] Hoja destino no existe. Creándola...", flush=True)
            destination_sheet = spreadsheet_dest.add_worksheet(title=target_worksheet_name, rows=1000, cols=30)
        destination_sheet.clear()
        dest_header = (
            ["file_path", "transcription_date", "transcription_duration", "whisper_model", "GPU_model"] +
            [f"transcription_part_{i}" for i in range(1, 11)] +
            [f"transcription_seg_part_{i}" for i in range(1, 11)]
        )
        destination_sheet.update("A1", [dest_header])
        print("[SHEET SUCCESS ✅] Hoja destino preparada y encabezados definidos.", flush=True)

        # Cargar modelo Whisper
        print(f"🔹🔹🔹 [START ▶️] Cargando modelo Whisper '{whisper_model_size}' 🔹🔹🔹", flush=True)
        model = whisper.load_model(whisper_model_size)
        gpu_model = torch.cuda.get_device_name(0) if torch.cuda.is_available() else "No"
        print("[MODEL SUCCESS ✅] Modelo Whisper cargado.", flush=True)

        # Convertir DataFrame a lista de diccionarios
        source_data = source_df.to_dict(orient="records")
        total_rows = len(source_data)
        print(f"[DATA INFO ℹ️] DataFrame fuente cargado. Total filas: {total_rows}", flush=True)

        # Procesar cada fila
        for idx, row_data in enumerate(source_data, start=1):
            video_path_value = row_data.get(field_name, "")
            if not video_path_value:
                continue

            print(f"[PROCESSING 🔄] ({idx}/{total_rows}) Transcribiendo: {video_path_value} (idioma='{whisper_language}')", flush=True)
            start_time_proc = time.time()
            try:
                result = model.transcribe(video_path_value, language=whisper_language)
                transcription_full = result.get("text", "")
                transcription_segments_full = "".join(
                    [f"[{seg['start']:.2f}s - {seg['end']:.2f}s]: {seg['text']}\n" for seg in result.get("segments", [])]
                )
            except Exception as e:
                print(f"[ERROR ❌] Error al transcribir {video_path_value}: {e}", flush=True)
                continue

            duration = round(time.time() - start_time_proc, 2)
            transcription_parts = _trocear_texto(transcription_full)
            transcription_seg_parts = _trocear_texto(transcription_segments_full)
            transcription_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            row_to_insert = (
                [video_path_value, transcription_date, duration, whisper_model_size, gpu_model] +
                transcription_parts + transcription_seg_parts
            )

            try:
                destination_sheet.append_row(row_to_insert, value_input_option="USER_ENTERED")
                print(f"[WRITE SUCCESS ✅] Fila {idx} escrita correctamente.", flush=True)
            except Exception as e:
                print(f"[ERROR ❌] Error al escribir la fila {idx}: {e}", flush=True)
                continue

        print("🔹🔹🔹 [FINISHED ✅] Proceso de transcripción completado.", flush=True)

    # Ejecutar el proceso de transcripción con manejo de errores
    try:
        _process_transcription()
        print("Proceso completado con éxito.", flush=True)
    except ValueError as ve:
        print(f"Error en los parámetros: {ve}", flush=True)
    except Exception as e:
        print(f"Error inesperado: {e}", flush=True)
