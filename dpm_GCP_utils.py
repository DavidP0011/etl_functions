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











# __________________________________________________________________________________________________________________________________________________________
# GCS_tables_schema_df
# __________________________________________________________________________________________________________________________________________________________
def GCS_tables_schema_df(config: dict) -> pd.DataFrame:
    """
    Retorna un DataFrame con información detallada de:
      - Buckets de Google Cloud Storage (GCS)
      - Objetos (archivos) de cada bucket (si `include_objects` es True)

    Incluye, entre otras, las siguientes propiedades a nivel de bucket:
      - fecha_creacion_bucket
      - tipo_ubicacion (REGIONAL, MULTI_REGIONAL, etc.)
      - ubicacion (p.e. us-central1, EU, etc.)
      - clase_almacenamiento_predeterminada
      - fecha_ultima_modificacion_bucket
      - acceso_publico (bool)
      - control_acceso (IAM vs. ACL, usando uniform bucket-level access)
      - proteccion (public_access_prevention, versioning, etc.)
      - espacio_nombres_jerarquico (referencia, en GCS no hay verdadero árbol)
      - retencion_buckets (retention_period, en segundos)
      - reglas_ciclo_vida (lifecycle_rules)
      - etiquetas (labels)
      - pagos_solicitante (requester_pays)
      - replicacion (rpo, para dual-region)
      - encriptacion (default_kms_key_name o "Google-managed")
      - estadisticas_seguridad (placeholder)

    Y a nivel de objeto:
      - object_name
      - content_type
      - size_mb
      - fecha_actualizacion_GCS (time_created / updated)
      - etc.

    Se añade 'fecha_actualizacion_df' (fecha en la que se crea este DataFrame) para todas las filas.

    Args:
        config (dict):
            - project_id (str) [requerido]: ID del proyecto de GCP.
            - buckets (list) [opcional]: Nombres de los buckets a consultar. Si no se proporciona,
              se listan todos los buckets disponibles en el proyecto.
            - include_objects (bool) [opcional]: Si True, detalla también los objetos en cada bucket. Por defecto True.
            - json_keyfile_GCP_secret_id (str, requerido en GCP): ID del secreto en Secret Manager que contiene las credenciales.
            - json_keyfile_colab (str, requerido fuera de GCP): Ruta local al JSON de credenciales.
    
    Returns:
        pd.DataFrame:
            Con columnas a nivel de bucket y, si procede, de objetos.

    Raises:
        ValueError: Si faltan parámetros obligatorios o hay errores en la autenticación.
        RuntimeError: Si ocurre algún problema durante la extracción o transformación.
    """
    # ────────────────────────────── Importaciones ──────────────────────────────
    import os
    import json
    import pandas as pd
    from google.cloud import storage
    from google.oauth2.service_account import Credentials

    # ────────────────────────────── Mensaje de inicio ─────────────────────────
    print("\n🔹🔹🔹 [START ▶️] Inicio del proceso de extracción extendida de GCS 🔹🔹🔹\n", flush=True)

    # ────────────────────────────── AUTENTICACIÓN ──────────────────────────────
    is_gcp = bool(os.environ.get("GOOGLE_CLOUD_PROJECT"))
    if is_gcp:
        json_keyfile_GCP_secret_id_str = config.get("json_keyfile_GCP_secret_id")
        if not json_keyfile_GCP_secret_id_str:
            raise ValueError(
                "[VALIDATION [ERROR ❌]] En entornos GCP se debe proporcionar 'json_keyfile_GCP_secret_id' en config."
            )
        print("[AUTHENTICATION [START ▶️]] Autenticación en GCP mediante Secret Manager...", flush=True)
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
            print(f"[AUTHENTICATION [SUCCESS ✅]] Autenticación GCP completada. (Secret: {json_keyfile_GCP_secret_id_str})", flush=True)
        except Exception as e:
            raise ValueError(f"[AUTHENTICATION [ERROR ❌]] Error durante la autenticación en GCP: {e}")
    else:
        json_keyfile_colab_str = config.get("json_keyfile_colab")
        if not json_keyfile_colab_str:
            raise ValueError(
                "[VALIDATION [ERROR ❌]] En entornos local/Colab se debe proporcionar 'json_keyfile_colab' en config."
            )
        print("[AUTHENTICATION [START ▶️]] Autenticación en entorno local/Colab mediante JSON de credenciales...", flush=True)
        try:
            creds = Credentials.from_service_account_file(json_keyfile_colab_str)
            print("[AUTHENTICATION [SUCCESS ✅]] Autenticación local/Colab completada.", flush=True)
        except Exception as e:
            raise ValueError(f"[AUTHENTICATION [ERROR ❌]] Error durante la autenticación local/Colab: {e}")

    # ────────────────────────────── VALIDACIÓN DE PARÁMETROS ──────────────────────────────
    project_id_str = config.get('project_id')
    if not project_id_str:
        raise ValueError("[VALIDATION [ERROR ❌]] 'project_id' es obligatorio en la configuración.")
    print(f"[METRICS [INFO ℹ️]] Proyecto de GCP: {project_id_str}", flush=True)

    buckets_incluidos_list = config.get('buckets', None)
    include_objects_bool = config.get('include_objects', True)

    # ────────────────────────────── CLIENTE DE STORAGE ──────────────────────────────
    print("[START ▶️] Inicializando cliente de Google Cloud Storage...", flush=True)
    try:
        storage_client = storage.Client(project=project_id_str, credentials=creds)
        print("[LOAD [SUCCESS ✅]] Cliente de GCS inicializado correctamente.", flush=True)
    except Exception as e:
        raise RuntimeError(f"[LOAD [ERROR ❌]] Error al inicializar el cliente de GCS: {e}")

    # ────────────────────────────── OBTENCIÓN DE BUCKETS ──────────────────────────────
    print("[EXTRACTION [START ▶️]] Obteniendo buckets del proyecto...", flush=True)
    try:
        if buckets_incluidos_list:
            # Se especifican los buckets
            buckets = [storage_client.bucket(b_name) for b_name in buckets_incluidos_list]
            print(f"[EXTRACTION [INFO ℹ️]] Se han especificado {len(buckets_incluidos_list)} buckets para la consulta.", flush=True)
        else:
            # Se listan todos los buckets del proyecto
            buckets = list(storage_client.list_buckets(project=project_id_str))
            print(f"[EXTRACTION [INFO ℹ️]] Se encontraron {len(buckets)} buckets en el proyecto.", flush=True)
    except Exception as e:
        raise RuntimeError(f"[EXTRACTION [ERROR ❌]] Error al obtener los buckets: {e}")

    # ────────────────────────────── Helper: verificar acceso público ──────────────────────
    def _is_public(bucket_obj):
        """
        Retorna True si el bucket permite acceso anónimo o allAuthenticatedUsers
        mediante IAM. Requiere permiso storage.buckets.getIamPolicy.
        """
        try:
            policy = bucket_obj.get_iam_policy(requested_policy_version=3)
        except Exception:
            # Si no se puede obtener la policy (falta de permisos), devolvemos None
            return None

        for binding in policy.bindings:
            members = binding.get("members", [])
            if "allUsers" in members or "allAuthenticatedUsers" in members:
                return True
        return False

    # ────────────────────────────── RECOPILACIÓN DE INFORMACIÓN ─────────────────────────
    gcs_info_list = []

    for bucket_obj in buckets:
        bucket_name_str = bucket_obj.name

        # Forzamos una "carga" de propiedades (en caso de que no estén en memoria)
        # Nota: a veces client.bucket(...) no trae todo hasta que se llama a un método.
        try:
            bucket_obj.reload()
        except Exception as e:
            print(f"[EXTRACTION [WARN ⚠️]] No se pudieron recargar propiedades para el bucket '{bucket_name_str}': {e}")

        # ────────── Metadatos del bucket ──────────
        bucket_props = bucket_obj._properties
        # Fecha de creación
        time_created_bucket = bucket_obj.time_created  # datetime o None
        fecha_creacion_bucket_str = (time_created_bucket.strftime("%Y-%m-%d %H:%M:%S")
                                     if time_created_bucket else None)
        # Última modificación (metadatos)
        updated_str = bucket_props.get("updated")  # Es un string en ISO8601
        fecha_ultima_modificacion_bucket_str = updated_str  # Lo dejamos como str
        
        # Tipo de ubicación (REGIONAL, MULTI_REGIONAL, DUAL_REGIONAL)
        tipo_ubicacion = bucket_props.get("locationType")  # None si no está
        # Ubicación
        ubicacion = bucket_obj.location  # p.e. "US-EAST1", "EU", "ASIA"
        # Clase de almacenamiento
        clase_almacenamiento = bucket_obj.storage_class
        # Acceso público
        acceso_publico_bool = _is_public(bucket_obj)
        # Control de acceso (IAM vs ACL). 
        # - Si uniform_bucket_level_access: True => "UNIFORM", caso contrario => "FINE"
        ubla_conf = bucket_obj.iam_configuration.get("uniformBucketLevelAccess", {})
        is_ubla_enabled = ubla_conf.get("enabled", False)
        control_acceso_str = "UNIFORM" if is_ubla_enabled else "FINE"

        # Protección: public_access_prevention, versioning, etc.
        public_access_prevention = bucket_obj.iam_configuration.get("publicAccessPrevention")
        versioning_enabled = bucket_obj.versioning_enabled
        proteccion_str = f"publicAccessPrevention={public_access_prevention}, versioning={versioning_enabled}"

        # Espacio de nombres jerárquico (meramente informativo, GCS es plano)
        espacio_nombres_jerarquico = "No hay jerarquía real en GCS"
        
        # Retención de buckets
        retencion_seg = bucket_obj.retention_period  # en segundos o None
        # Reglas de ciclo de vida
        reglas_ciclo_vida = bucket_obj.lifecycle_rules  # lista de dict
        # Etiquetas
        etiquetas_dict = bucket_obj.labels  # dict o None
        # Pagos del solicitante
        pagos_solicitante_bool = bucket_obj.requester_pays
        # Replicación
        replication_rpo = bucket_obj.rpo  # "DEFAULT" o "ASYNC_TURBO" en dual-region
        # Encriptación
        #   - Si bucket_obj.default_kms_key_name es None => usa Google-managed encryption
        encriptacion_str = bucket_obj.default_kms_key_name or "Google-managed"
        # Estadísticas de seguridad (Placeholder: podríamos armar algo condicional)
        estadisticas_seguridad_str = None

        # ────────── Si include_objects: recorremos los blobs ──────────
        if include_objects_bool:
            print(f"\n[EXTRACTION [INFO ℹ️]] Listando objetos en bucket '{bucket_name_str}'...", flush=True)
            try:
                blobs = list(bucket_obj.list_blobs())
                print(f"[EXTRACTION [SUCCESS ✅]] Se encontraron {len(blobs)} objetos en '{bucket_name_str}'.", flush=True)
            except Exception as e:
                print(f"[EXTRACTION [ERROR ❌]] Error al listar objetos en '{bucket_name_str}': {e}", flush=True)
                # Si no podemos listar blobs, al menos almacenamos una fila con info del bucket
                gcs_info_list.append({
                    'project_id': project_id_str,
                    'bucket_name': bucket_name_str,
                    'fecha_creacion_bucket': fecha_creacion_bucket_str,
                    'tipo_ubicacion': tipo_ubicacion,
                    'ubicacion': ubicacion,
                    'clase_almacenamiento_predeterminada': clase_almacenamiento,
                    'fecha_ultima_modificacion_bucket': fecha_ultima_modificacion_bucket_str,
                    'acceso_publico': acceso_publico_bool,
                    'control_acceso': control_acceso_str,
                    'proteccion': proteccion_str,
                    'espacio_nombres_jerarquico': espacio_nombres_jerarquico,
                    'retencion_buckets': retencion_seg,
                    'reglas_ciclo_vida': str(reglas_ciclo_vida) if reglas_ciclo_vida else None,
                    'etiquetas': str(etiquetas_dict) if etiquetas_dict else None,
                    'pagos_solicitante': pagos_solicitante_bool,
                    'replicacion': replication_rpo,
                    'encriptacion': encriptacion_str,
                    'estadisticas_seguridad': estadisticas_seguridad_str,
                    'object_name': None,
                    'content_type': None,
                    'size_mb': None,
                    'fecha_actualizacion_GCS': None
                })
                continue

            for blob in blobs:
                object_name_str = blob.name
                content_type_str = blob.content_type
                size_mb_float = round(blob.size / (1024 * 1024), 2) if blob.size else 0.0
                # Fecha de creación/última modificación del objeto
                # time_created y updated
                time_created_obj = blob.time_created
                updated_obj = blob.updated
                # Damos prioridad a time_created
                if time_created_obj:
                    fecha_actualizacion_GCS_str = time_created_obj.strftime("%Y-%m-%d %H:%M:%S")
                elif updated_obj:
                    fecha_actualizacion_GCS_str = updated_obj.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    fecha_actualizacion_GCS_str = None

                gcs_info_list.append({
                    'project_id': project_id_str,
                    'bucket_name': bucket_name_str,
                    'fecha_creacion_bucket': fecha_creacion_bucket_str,
                    'tipo_ubicacion': tipo_ubicacion,
                    'ubicacion': ubicacion,
                    'clase_almacenamiento_predeterminada': clase_almacenamiento,
                    'fecha_ultima_modificacion_bucket': fecha_ultima_modificacion_bucket_str,
                    'acceso_publico': acceso_publico_bool,
                    'control_acceso': control_acceso_str,
                    'proteccion': proteccion_str,
                    'espacio_nombres_jerarquico': espacio_nombres_jerarquico,
                    'retencion_buckets': retencion_seg,
                    'reglas_ciclo_vida': str(reglas_ciclo_vida) if reglas_ciclo_vida else None,
                    'etiquetas': str(etiquetas_dict) if etiquetas_dict else None,
                    'pagos_solicitante': pagos_solicitante_bool,
                    'replicacion': replication_rpo,
                    'encriptacion': encriptacion_str,
                    'estadisticas_seguridad': estadisticas_seguridad_str,
                    'object_name': object_name_str,
                    'content_type': content_type_str,
                    'size_mb': size_mb_float,
                    'fecha_actualizacion_GCS': fecha_actualizacion_GCS_str
                })
        else:
            # Si no incluimos objetos, registramos solo 1 fila por bucket
            gcs_info_list.append({
                'project_id': project_id_str,
                'bucket_name': bucket_name_str,
                'fecha_creacion_bucket': fecha_creacion_bucket_str,
                'tipo_ubicacion': tipo_ubicacion,
                'ubicacion': ubicacion,
                'clase_almacenamiento_predeterminada': clase_almacenamiento,
                'fecha_ultima_modificacion_bucket': fecha_ultima_modificacion_bucket_str,
                'acceso_publico': acceso_publico_bool,
                'control_acceso': control_acceso_str,
                'proteccion': proteccion_str,
                'espacio_nombres_jerarquico': espacio_nombres_jerarquico,
                'retencion_buckets': retencion_seg,
                'reglas_ciclo_vida': str(reglas_ciclo_vida) if reglas_ciclo_vida else None,
                'etiquetas': str(etiquetas_dict) if etiquetas_dict else None,
                'pagos_solicitante': pagos_solicitante_bool,
                'replicacion': replication_rpo,
                'encriptacion': encriptacion_str,
                'estadisticas_seguridad': estadisticas_seguridad_str,
                'object_name': None,
                'content_type': None,
                'size_mb': None,
                'fecha_actualizacion_GCS': None
            })

    # ────────────────────────────── CONVERSIÓN A DATAFRAME ──────────────────────────────
    print("\n[TRANSFORMATION [START ▶️]] Convirtiendo información recopilada en DataFrame...", flush=True)
    try:
        df_gcs = pd.DataFrame(gcs_info_list)
        print(f"[TRANSFORMATION [SUCCESS ✅]] DataFrame generado con {df_gcs.shape[0]} registros.", flush=True)
    except Exception as e:
        raise RuntimeError(f"[TRANSFORMATION [ERROR ❌]] Error al convertir la información a DataFrame: {e}")

    # Añadir fecha de creación del DataFrame
    df_gcs["fecha_actualizacion_df"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    print("\n🔹🔹🔹 [END [FINISHED 🏁]] Esquema extendido de GCS obtenido correctamente. 🔹🔹🔹\n", flush=True)
    return df_gcs
