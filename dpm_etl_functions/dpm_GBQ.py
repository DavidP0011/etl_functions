# __________________________________________________________________________________________________________________________________________________________
# GBQ_delete_tables
# __________________________________________________________________________________________________________________________________________________________
def GBQ_delete_tables(config: dict) -> None:
  """
  Elimina todas las tablas de uno o varios datasets específicos en BigQuery.

  Args:
      config (dict):
          - project_id (str): ID del proyecto de Google Cloud.
          - dataset_ids (list): Lista de IDs de los datasets de BigQuery.
          - location (str, opcional): Ubicación del dataset (default: "EU").
          - ini_environment_identificated (str, opcional): Modo de autenticación.
                Opciones: "LOCAL", "COLAB", "COLAB_ENTERPRISE" o un project_id.
          - json_keyfile_local (str, opcional): Ruta al archivo JSON de credenciales para entorno LOCAL.
          - json_keyfile_colab (str, opcional): Ruta al archivo JSON de credenciales para entorno COLAB.
          - json_keyfile_GCP_secret_id (str, opcional): Identificador del secreto en Secret Manager para entornos GCP.

  Returns:
      None: Imprime el progreso y confirma la eliminación exitosa.

  Raises:
      ValueError: Si faltan parámetros obligatorios o de autenticación.
      Exception: Si ocurre un error durante el proceso.
  """
  import pandas as pd
  # ────────────────────────────── VALIDACIÓN DE PARÁMETROS ──────────────────────────────
  project_id_str = config.get('project_id')
  dataset_ids_list = config.get('dataset_ids')
  location_str = config.get('location', 'EU')

  if not project_id_str or not dataset_ids_list:
      raise ValueError("[VALIDATION [ERROR ❌]] Los parámetros 'project_id' y 'dataset_ids' son obligatorios.")

  print("\n[START ▶️] Iniciando proceso de eliminación de tablas en BigQuery...", flush=True)

  try:
      # ────────────────────────────── IMPORTACIÓN DE LIBRERÍAS ──────────────────────────────
      from google.cloud import bigquery

      # ────────────────────────────── AUTENTICACIÓN DINÁMICA ──────────────────────────────
      print("[AUTHENTICATION [INFO ℹ️]] Inicializando cliente de BigQuery...", flush=True)
      ini_env_str = config.get("ini_environment_identificated", "").upper()

      if ini_env_str == "LOCAL":
          json_keyfile_local_str = config.get("json_keyfile_local")
          if not json_keyfile_local_str:
              raise ValueError("[VALIDATION [ERROR ❌]] Falta 'json_keyfile_local' para autenticación LOCAL.")
          client = bigquery.Client.from_service_account_json(json_keyfile_local_str, location=location_str)

      elif ini_env_str == "COLAB":
          json_keyfile_colab_str = config.get("json_keyfile_colab")
          if not json_keyfile_colab_str:
              raise ValueError("[VALIDATION [ERROR ❌]] Falta 'json_keyfile_colab' para autenticación COLAB.")
          client = bigquery.Client.from_service_account_json(json_keyfile_colab_str, location=location_str)

      elif ini_env_str == "COLAB_ENTERPRISE" or (ini_env_str not in ["LOCAL", "COLAB"] and ini_env_str):
          json_keyfile_GCP_secret_id_str = config.get("json_keyfile_GCP_secret_id")
          if not json_keyfile_GCP_secret_id_str:
              raise ValueError("[VALIDATION [ERROR ❌]] Falta 'json_keyfile_GCP_secret_id' para autenticación GCP.")
          if ini_env_str == "COLAB_ENTERPRISE":
              import os
              project_id_str = os.environ.get("GOOGLE_CLOUD_PROJECT", project_id_str)
          client = bigquery.Client(project=project_id_str, location=location_str)
      else:
          client = bigquery.Client(location=location_str)

      print("[AUTHENTICATION [SUCCESS ✅]] Cliente de BigQuery inicializado correctamente.", flush=True)

      # ────────────────────────────── PROCESAMIENTO DE DATASETS ──────────────────────────────
      for dataset_id_str in dataset_ids_list:
          print(f"\n[EXTRACTION [START ▶️]] Procesando dataset: {dataset_id_str}", flush=True)

          # Referencia al dataset
          dataset_ref = client.dataset(dataset_id_str, project=project_id_str)

          # Listar todas las tablas en el dataset
          tables = client.list_tables(dataset_ref)
          table_list = list(tables)

          if not table_list:
              print(f"[EXTRACTION [INFO ℹ️]] No se encontraron tablas en el dataset '{dataset_id_str}'.", flush=True)
          else:
              # ────────── ELIMINACIÓN DE TABLAS ──────────
              for table in table_list:
                  table_id_str = f"{project_id_str}.{dataset_id_str}.{table.table_id}"
                  print(f"[LOAD [INFO ℹ️]] Eliminando tabla: {table_id_str}", flush=True)
                  client.delete_table(table, not_found_ok=True)
              print(f"[END [FINISHED ✅]] Todas las tablas en el dataset '{dataset_id_str}' han sido eliminadas exitosamente.", flush=True)

      print("\n[END [FINISHED ✅]] Proceso de eliminación completado.", flush=True)

  except Exception as exc:
      print(f"\n[END [FAILED ❌]] Error durante la eliminación de tablas: {exc}", flush=True)
      raise
