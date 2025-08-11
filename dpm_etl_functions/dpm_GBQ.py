from dpm_common_functions import _ini_authenticate_API


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












# @title GBQ_generate_join_tables()
import pandas as pd

def GBQ_generate_join_tables(params: dict) -> str:
    """
    Genera una consulta SQL para unir (mediante UNION ALL) todas las tablas listadas en el diccionario
    contenido en params["source_tables_and_field_names_filter_dic"] y guardar el resultado en la tabla
    destino especificada en params["destination_table"].

    Cada entrada del diccionario representa una tabla de origen y su lista de campos a seleccionar.
    Los nombres finales de los campos (alias) se toman de la lista de la primera tabla del diccionario.

    Parámetros en el diccionario `params`:
      - "source_tables_and_field_names_filter_dic" (dict): Diccionario donde cada clave es el nombre completo
         (fully qualified) de la tabla a unir y cada valor es una lista de campos a seleccionar de esa tabla.
      - "destination_table" (str): Nombre de la tabla destino en BigQuery.
      - "if_exists" (str, opcional): Comportamiento si la tabla destino ya existe. Opciones:
                                    'fail', 'replace' o 'append'. (Por defecto: 'fail').

    Returns:
      str: SQL generado que une las tablas mediante UNION ALL y crea (o inserta en) la tabla destino.

    Raises:
      ValueError: Si faltan parámetros esenciales, si el diccionario está vacío, o si las listas de campos
                  no tienen la misma longitud.
    """

    def _validar_parametros(params: dict) -> (dict, str, str):
        """Valida los parámetros obligatorios y retorna el diccionario de origen, la tabla destino y el modo."""
        source_dic = params.get("source_tables_and_field_names_filter_dic")
        destination_table = params.get("destination_table")
        if_exists = params.get("if_exists", "fail").lower()

        if source_dic is None or not isinstance(source_dic, dict):
            raise ValueError("❌ Error: 'source_tables_and_field_names_filter_dic' debe ser un diccionario válido en params.")
        if not source_dic:
            raise ValueError("❌ Error: 'source_tables_and_field_names_filter_dic' no puede estar vacío.")
        if not destination_table or not isinstance(destination_table, str):
            raise ValueError("❌ Error: 'destination_table' debe ser un string válido en params.")
        if if_exists not in {"fail", "replace", "append"}:
            raise ValueError("❌ Error: 'if_exists' debe ser 'fail', 'replace' o 'append'.")

        return source_dic, destination_table, if_exists

    def _procesar_diccionario(source_dic: dict) -> (list, list):
        """
        Procesa el diccionario de tablas y campos.
        Retorna la lista de tablas y la lista de campos finales a usar (tomada de la primera tabla).
        """
        # Mantener el orden de inserción (Python 3.7+)
        tables = list(source_dic.keys())
        if not tables:
            raise ValueError("❌ Error: No se encontraron tablas en 'source_tables_and_field_names_filter_dic'.")

        final_field_names = source_dic[tables[0]]
        if not isinstance(final_field_names, list) or not final_field_names:
            raise ValueError("❌ Error: La lista de campos de la primera tabla no es válida o está vacía.")

        print(f"✅ La tabla base es '{tables[0]}' con campos finales: {', '.join(final_field_names)}", flush=True)
        return tables, final_field_names

    def _construir_union_sql(source_dic: dict, tables: list, final_field_names: list) -> str:
        """
        Construye la parte de la consulta que realiza el UNION ALL entre todas las tablas.
        """
        union_parts = []
        for table in tables:
            fields = source_dic[table]
            if not isinstance(fields, list) or not fields:
                raise ValueError(f"❌ Error: La lista de campos para la tabla '{table}' no es válida o está vacía.")
            if len(fields) != len(final_field_names):
                raise ValueError(f"❌ Error: La cantidad de campos en la tabla '{table}' ({len(fields)}) no coincide con la cantidad de campos en la tabla base ({len(final_field_names)}).")

            # Se arma el SELECT para cada tabla asignando alias según la lista de la primera tabla
            select_clauses = [f"{src_field} AS {final_field}" for src_field, final_field in zip(fields, final_field_names)]
            select_clause = ", ".join(select_clauses)
            union_parts.append(f"SELECT {select_clause} FROM `{table}`")

        union_sql = "\nUNION ALL\n".join(union_parts)
        return union_sql

    def _construir_consulta_final(union_sql: str, destination_table: str, if_exists: str) -> str:
        """
        Construye la consulta SQL final según el modo de inserción o creación de la tabla destino.
        """
        print(f"⚙️ Modo seleccionado: {if_exists.upper()}...", flush=True)
        if if_exists == "replace":
            print(f"🔄 La tabla `{destination_table}` será reemplazada si ya existe.", flush=True)
            return f"CREATE OR REPLACE TABLE `{destination_table}` AS\n{union_sql}"
        elif if_exists == "append":
            print(f"➕ Los datos se agregarán a la tabla `{destination_table}` si ya existe.", flush=True)
            return f"INSERT INTO `{destination_table}`\n{union_sql}"
        elif if_exists == "fail":
            print(f"🚨 Se generó una consulta SQL sin crear la tabla destino. La tabla `{destination_table}` debe crearse manualmente.", flush=True)
            return f"-- Asegúrate de que la tabla `{destination_table}` no exista antes de ejecutar la siguiente consulta\n{union_sql}"

    # Inicio del proceso
    print("\n📌 Iniciando generación de consulta SQL para UNION ALL en BigQuery...", flush=True)

    source_dic, destination_table, if_exists = _validar_parametros(params)
    print("🔍 Procesando diccionario de tablas y campos...", flush=True)
    tables, final_field_names = _procesar_diccionario(source_dic)

    union_sql = _construir_union_sql(source_dic, tables, final_field_names)

    print("🔧 Construyendo consulta SQL...", flush=True)
    sql = _construir_consulta_final(union_sql, destination_table, if_exists)

    print("✅ Consulta SQL generada exitosamente.\n", flush=True)
    return sql
