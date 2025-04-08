from dpm_common_functions import _ini_authenticate_API



# __________________________________________________________________________________________________________________________________________________________
def GBQ_execute_SQL(config: dict) -> None:
    """
    Ejecuta un script SQL en Google BigQuery y muestra un resumen detallado de la ejecución.

    Args:
        config (dict):
            - GCP_project_id (str): ID del proyecto de GCP.
            - SQL_script (str): Script SQL a ejecutar.
            - destination_table (str, opcional): Tabla destino para obtener estadísticas adicionales.
            Además, config debe incluir las claves del diccionario común para autenticación.

    Raises:
        ValueError: Si faltan parámetros obligatorios o ocurre un error durante la autenticación o ejecución del script.

    Returns:
        None
    """
    print("[START ▶️] Ejecución de script SQL en BigQuery", flush=True)
    import time
    from google.cloud import bigquery

    def _validate_parameters(cfg: dict) -> tuple:
        project_id = cfg.get('GCP_project_id')
        sql_script = cfg.get('SQL_script')
        if not project_id or not sql_script:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta 'GCP_project_id' o 'SQL_script' en config.")
        destination_table = cfg.get('destination_table')
        return project_id, sql_script, destination_table

    proj_id, sql_script, dest_table = _validate_parameters(config)
    creds = _ini_authenticate_API(config, proj_id)
    client = bigquery.Client(project=proj_id, credentials=creds)

    def _show_script_summary(sql_script: str) -> None:
        action = sql_script.strip().split()[0]
        print(f"[EXTRACTION INFO ℹ️] Acción detectada en el script SQL: {action}", flush=True)
        print("[EXTRACTION INFO ℹ️] Resumen (primeras 5 líneas):", flush=True)
        for line in sql_script.strip().split('\n')[:5]:
            print(line, flush=True)
        print("...", flush=True)

    def _execute_query(client: bigquery.Client, sql_script: str, start_time: float):
        print("[TRANSFORMATION START ▶️] Ejecutando el script SQL...", flush=True)
        query_job = client.query(sql_script)
        query_job.result()
        elapsed = time.time() - start_time
        print("[TRANSFORMATION SUCCESS ✅] Consulta ejecutada exitosamente.", flush=True)
        return query_job, elapsed

    def _show_job_details(client: bigquery.Client, query_job, elapsed: float, dest_table: str) -> None:
        print("[METRICS INFO ℹ️] Detalles del job de BigQuery:", flush=True)
        print(f"  - ID del job: {query_job.job_id}", flush=True)
        print(f"  - Estado: {query_job.state}", flush=True)
        print(f"  - Tiempo de creación: {query_job.created}", flush=True)
        if hasattr(query_job, 'started'):
            print(f"  - Tiempo de inicio: {query_job.started}", flush=True)
        if hasattr(query_job, 'ended'):
            print(f"  - Tiempo de finalización: {query_job.ended}", flush=True)
        print(f"  - Bytes procesados: {query_job.total_bytes_processed or 'N/A'}", flush=True)
        print(f"  - Bytes facturados: {query_job.total_bytes_billed or 'N/A'}", flush=True)
        print(f"  - Cache hit: {query_job.cache_hit}", flush=True)
        if dest_table:
            try:
                count_query = f"SELECT COUNT(*) AS total_rows FROM `{dest_table}`"
                count_result = client.query(count_query).result()
                rows_in_table = [row['total_rows'] for row in count_result][0]
                print(f"  - Filas en la tabla destino: {rows_in_table}", flush=True)
            except Exception as e:
                print(f"[METRICS WARNING ⚠️] No se pudo obtener información de la tabla destino: {e}", flush=True)
        print(f"[END FINISHED ✅] Tiempo total de ejecución: {elapsed:.2f} segundos", flush=True)

    _show_script_summary(sql_script)
    start = time.time()
    try:
        query_job, elapsed = _execute_query(client, sql_script, start)
        _show_job_details(client, query_job, elapsed, dest_table)
    except Exception as e:
        print(f"[TRANSFORMATION ERROR ❌] Error al ejecutar el script SQL: {str(e)}", flush=True)
        raise

















# __________________________________________________________________________________________________________________________________________________________
def SQL_generate_academic_date_str(params) -> str:
    """
    Genera una sentencia SQL para crear o reemplazar una tabla que incluye campos de fecha académica/fiscal,
    basándose en reglas de corte sobre un campo fecha existente.

    Parámetros en params:
      - table_source (str): Tabla de origen.
      - table_destination (str): Tabla destino.
      - custom_fields_config (dict): Configuración de campos y reglas de corte.
      - (Opcional) json_keyfile: Se espera que params incluya las claves comunes para autenticación.

    Retorna:
        str: Sentencia SQL generada.
    """
    # No se requieren imports adicionales para esta función.
    print("[START ▶️] Generando SQL para fechas académicas/fiscales...", flush=True)
    table_source = params["table_source"]
    table_destination = params["table_destination"]
    custom_fields_config = params["custom_fields_config"]

    print(f"[EXTRACTION [INFO ℹ️]] table_source: {table_source}", flush=True)
    print(f"[EXTRACTION [INFO ℹ️]] table_destination: {table_destination}", flush=True)
    print("[TRANSFORMATION [INFO ℹ️]] Procesando configuración de fechas...", flush=True)

    additional_expressions = []
    for field, rules in custom_fields_config.items():
        for rule in rules:
            start_month = rule.get("start_month", 9)
            start_day = rule.get("start_day", 1)
            suffix = rule.get("suffix", "custom")
            new_field = f"{field}_{suffix}"
            expression = (
                f"CASE\n"
                f"  WHEN (EXTRACT(MONTH FROM `{field}`) > {start_month})\n"
                f"       OR (EXTRACT(MONTH FROM `{field}`) = {start_month} AND EXTRACT(DAY FROM `{field}`) >= {start_day}) THEN\n"
                f"    CONCAT(\n"
                f"      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) - 2000) AS STRING), 2, '0'),\n"
                f"      '-',\n"
                f"      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) + 1 - 2000) AS STRING), 2, '0')\n"
                f"    )\n"
                f"  ELSE\n"
                f"    CONCAT(\n"
                f"      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) - 1 - 2000) AS STRING), 2, '0'),\n"
                f"      '-',\n"
                f"      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) - 2000) AS STRING), 2, '0')\n"
                f"    )\n"
                f"END AS `{new_field}`"
            )
            additional_expressions.append(expression)
            print(f"[TRANSFORMATION [INFO ℹ️]] Expresión generada para '{field}' con suffix '{suffix}'.", flush=True)
    
    additional_select = ",\n  ".join(additional_expressions) if additional_expressions else ""
    if additional_select:
        sql_script = (
            f"CREATE OR REPLACE TABLE `{table_destination}` AS\n"
            f"SELECT\n"
            f"  s.*,\n"
            f"  {additional_select}\n"
            f"FROM `{table_source}` s\n"
            f";"
        )
    else:
        sql_script = (
            f"CREATE OR REPLACE TABLE `{table_destination}` AS\n"
            f"SELECT\n"
            f"  *\n"
            f"FROM `{table_source}`;\n"
        )
    print("[END [FINISHED ✅]] SQL generado exitosamente.\n", flush=True)
    return sql_script

















# __________________________________________________________________________________________________________________________________________________________
def SQL_generate_cleaning_str(params: dict) -> str:
    """
    Genera una sentencia SQL para crear o sobrescribir una tabla de staging aplicando mapeos a los nombres de campos,
    filtros por rango de fechas, exclusión de registros por palabras clave y limpieza de espacios.
    
    Adicionalmente, si se activa, elimina registros duplicados (fusionados) en base a los siguientes campos:
      - merged_object_ids_field_name: Campo que contiene uno o varios IDs fusionados, separados por un delimitador.
      - merged_calculated_vids_field_name: Campo que contiene pares ID:VID, separados por delimitadores.
    
    Parámetros en params:
      - table_source (str): Tabla fuente.
      - table_destination (str): Tabla destino.
      - fields_mapped_use (bool): Si True, usa el nombre formateado.
      - fields_mapped_df (pd.DataFrame): DataFrame con columnas ["Campo Original", "Campo Formateado"].
      - fields_destination_prefix (str, opcional)
      - exclude_records_by_creation_date_bool (bool)
      - exclude_records_by_creation_date_field (str, opcional)
      - exclude_records_by_creation_date_range (dict, opcional)
      - exclude_records_by_keywords_bool (bool)
      - exclude_records_by_keywords_fields (list, opcional)
      - exclude_records_by_keywords_words (list, opcional)
      - fields_to_trim (list, opcional)
      
      -- Nuevas keys para eliminación de duplicados:
      - remove_duplicates_bool (bool): Indica si se debe aplicar eliminación de duplicados.
      - merged_object_ids_field_name (str): Campo que contiene los IDs fusionados.
      - merged_object_ids_delimiter (str, opcional, default ";")
      - merged_calculated_vids_field_name (str): Campo que contiene los pares ID:VID.
      - merged_calculated_vids_pair_delimiter (str, opcional, default ";")
      - merged_calculated_vids_value_delimiter (str, opcional, default ":")
    
    Retorna:
        str: Sentencia SQL generada.
    """
    # Importar librerías necesarias para procesamiento de cadenas y expresiones regulares
    import unicodedata
    import re
    print("[START ▶️] Generando SQL de limpieza...", flush=True)
    
    table_source = params.get("table_source")
    table_destination = params.get("table_destination")
    fields_mapped_df = params.get("fields_mapped_df")
    fields_mapped_use = params.get("fields_mapped_use", True)
    fields_destination_prefix = params.get("fields_destination_prefix", "")
    exclude_by_date_bool = params.get("exclude_records_by_creation_date_bool", False)
    exclude_by_date_field = params.get("exclude_records_by_creation_date_field", "")
    exclude_by_date_range = params.get("exclude_records_by_creation_date_range", {})
    exclude_by_keywords_bool = params.get("exclude_records_by_keywords_bool", False)
    exclude_by_keywords_fields = params.get("exclude_records_by_keywords_fields", [])
    exclude_by_keywords_words = params.get("exclude_records_by_keywords_words", [])
    fields_to_trim = params.get("fields_to_trim", [])
    
    remove_duplicates_bool = params.get("remove_duplicates_bool", False)
    merged_object_ids_field = params.get("merged_object_ids_field_name", "")
    merged_calculated_vids_field = params.get("merged_calculated_vids_field_name", "")
    merged_object_ids_delimiter = params.get("merged_object_ids_delimiter", ";")
    merged_calculated_vids_pair_delimiter = params.get("merged_calculated_vids_pair_delimiter", ";")
    merged_calculated_vids_value_delimiter = params.get("merged_calculated_vids_value_delimiter", ":")
    
    # Generar cláusulas SELECT con limpieza de espacios
    select_clauses = []
    for _, row in fields_mapped_df.iterrows():
        campo_origen = row['Campo Original']
        alias = f"{fields_destination_prefix}{row['Campo Formateado']}" if fields_mapped_use else f"{fields_destination_prefix}{campo_origen}"
        if campo_origen in fields_to_trim:
            select_clause = f"TRIM(REPLACE(`{campo_origen}`, '  ', ' ')) AS `{alias}`"
        else:
            select_clause = f"`{campo_origen}` AS `{alias}`"
        select_clauses.append(select_clause)
    select_part = ",\n  ".join(select_clauses)
    
    where_filters = []
    if exclude_by_date_bool and exclude_by_date_field:
        date_from = exclude_by_date_range.get("from", "")
        date_to = exclude_by_date_range.get("to", "")
        if date_from:
            where_filters.append(f"`{exclude_by_date_field}` >= '{date_from}'")
        if date_to:
            where_filters.append(f"`{exclude_by_date_field}` <= '{date_to}'")
    if exclude_by_keywords_bool and exclude_by_keywords_fields and exclude_by_keywords_words:
        for field in exclude_by_keywords_fields:
            where_filters.extend([f"`{field}` NOT LIKE '%{word}%'" for word in exclude_by_keywords_words])
    where_clause = " AND ".join(where_filters) if where_filters else "TRUE"
    
    base_query = (
        f"SELECT\n"
        f"  {select_part}\n"
        f"FROM `{table_source}`\n"
        f"WHERE {where_clause}\n"
    )
    
    if remove_duplicates_bool and merged_object_ids_field and merged_calculated_vids_field:
        dedup_query = (
            f"WITH dedup AS (\n"
            f"  SELECT\n"
            f"    {select_part},\n"
            f"    ROW_NUMBER() OVER (\n"
            f"      PARTITION BY CASE\n"
            f"        WHEN `{merged_object_ids_field}` IS NULL OR `{merged_object_ids_field}` = '' THEN CAST(`id` AS STRING)\n"
            f"        ELSE SPLIT(`{merged_object_ids_field}`, '{merged_object_ids_delimiter}')[OFFSET(0)]\n"
            f"      END\n"
            f"      ORDER BY IF(`{merged_calculated_vids_field}` IS NOT NULL AND `{merged_calculated_vids_field}` <> '', 0, 1)\n"
            f"    ) AS rn\n"
            f"  FROM `{table_source}`\n"
            f"  WHERE {where_clause}\n"
            f")\n"
            f"SELECT * EXCEPT(rn) FROM dedup\n"
            f"WHERE rn = 1\n"
            f";"
        )
        sql_script = f"CREATE OR REPLACE TABLE `{table_destination}` AS\n{dedup_query}"
    else:
        sql_script = (
            f"CREATE OR REPLACE TABLE `{table_destination}` AS\n"
            f"{base_query}"
            f";"
        )
    
    print("[END [FINISHED ✅]] SQL de limpieza generado.\n", flush=True)
    return sql_script












# __________________________________________________________________________________________________________________________________________________________
def SQL_generate_country_from_phone(config: dict) -> str:
    """
    Genera un script SQL para actualizar una tabla destino a partir de datos extraídos de números telefónicos y estatus de llamadas.
    Se procesa el número (añadiendo un prefijo si es necesario y determinando el país) y se extraen los estatus de llamadas para realizar un JOIN con la tabla destino.

    Parámetros en config:
      - source_table (str): Tabla de contactos en formato "proyecto.dataset.tabla".
      - source_contact_phone_field (str): Campo que contiene el número telefónico.
      - source_contact_id_field_name (str): Campo identificador del contacto.
      
      - source_engagement_call_table (str): Tabla de llamadas en formato "proyecto.dataset.tabla".
      - source_engagement_call_id_match_contact_field_name (str): Campo para relacionar llamadas y contactos.
      - source_engagement_call_status_field_name (str): Nombre original del campo de estatus de llamada.
      - source_engagement_call_status_values_list (list): Lista de estatus permitidos (ej.: ['COMPLETED', 'IN_PROGRESS', 'QUEUED']).
      - source_engagement_createdate_field_name (str): Nombre original del campo de fecha de creación de la llamada.
      
      - target_table (str): Tabla destino en formato "proyecto.dataset.tabla".
      - target_id_match_contact_field_name (str): Campo en la tabla destino para hacer match con el contacto.
      - target_country_mapped_field_name (str): Campo donde se almacenará el país obtenido.
      - target_call_status_field_name (str): Campo donde se almacenará el estatus de la llamada. Si está vacío, no se crea.
      
      - temp_table_name (str): Nombre de la tabla auxiliar.
      - temp_table_erase (bool): Si True, se elimina la tabla auxiliar tras el JOIN.
      
      - Además, se deben incluir las claves de autenticación:
        - ini_environment_identificated
        - json_keyfile_local
        - json_keyfile_colab
        - json_keyfile_GCP_secret_id

    Retorna:
        str: Script SQL final (incluye JOIN y, opcionalmente, DROP TABLE).
    """
    print("[START ▶️] Procesando números telefónicos para determinar países...", flush=True)
    # Importar las librerías necesarias
    import math
    import pandas as pd
    import pandas_gbq
    from google.cloud import bigquery
    
    # Parámetros de configuración para contactos
    table_source = config["source_table"]
    source_phone_field = config["source_contact_phone_field"]
    source_contact_id = config["source_contact_id_field_name"]
    
    # Parámetros de llamadas
    call_table = config["source_engagement_call_table"]
    call_match_field = config["source_engagement_call_id_match_contact_field_name"]
    call_status_field = config["source_engagement_call_status_field_name"]
    call_status_values_list = config["source_engagement_call_status_values_list"]
    call_createdate_field = config["source_engagement_createdate_field_name"]
    
    # Parámetros de la tabla destino
    target_table = config["target_table"]
    target_id_match_field = config["target_id_match_contact_field_name"]
    target_country_field = config["target_country_mapped_field_name"]
    target_call_status_field = config["target_call_status_field_name"]

    default_prefix = config.get("default_phone_prefix", "+34")
    
    print("[AUTHENTICATION [INFO] ℹ️] Iniciando autenticación...", flush=True)
    project = table_source.split(".")[0]
    creds = _ini_authenticate_API(config, project)
    client = bigquery.Client(project=project, credentials=creds)
    
    print("[EXTRACTION START ▶️] Extrayendo datos de contactos...", flush=True)
    query_source = f"SELECT {source_contact_id}, {source_phone_field} FROM {table_source}"
    df_contacts = client.query(query_source).to_dataframe()
    if df_contacts.empty:
        print("[EXTRACTION [WARNING ⚠️]] No se encontraron datos en la tabla de contactos.", flush=True)
        return ""
    df_contacts.rename(columns={source_phone_field: "phone"}, inplace=True)
    df_contacts = df_contacts[df_contacts["phone"].notna() & (df_contacts["phone"].str.strip() != "")]
    
    print("[TRANSFORMATION START ▶️] Procesando teléfonos en lotes...", flush=True)
    def _preprocess_phone(phone: str, default_prefix: str = default_prefix) -> str:
        if phone and isinstance(phone, str):
            phone = phone.strip()
            if not phone.startswith("+"):
                phone = default_prefix + phone
        return phone

    def _get_country_from_phone(phone: str) -> str:
        try:
            import phonenumbers
            phone_obj = phonenumbers.parse(phone, None)
            country_code = phonenumbers.region_code_for_number(phone_obj)
            if country_code:
                try:
                    import pycountry
                    country_obj = pycountry.countries.get(alpha_2=country_code)
                    return country_obj.name if country_obj else country_code
                except Exception:
                    return country_code
            return None
        except Exception:
            return None

    def _process_phone_numbers(series: pd.Series, batch_size: int = 1000) -> tuple:
        total = len(series)
        num_batches = math.ceil(total / batch_size)
        results = [None] * total
        error_batches = 0
        for i in range(num_batches):
            start = i * batch_size
            end = min((i+1) * batch_size, total)
            batch = series.iloc[start:end].copy()
            try:
                batch = batch.apply(lambda x: _preprocess_phone(x))
                results[start:end] = batch.apply(_get_country_from_phone).tolist()
            except Exception as e:
                error_batches += 1
                print(f"[EXTRACTION [ERROR ❌]] Error en el lote {i+1}: {e}", flush=True)
            print(f"[METRICS [INFO ℹ️]] Lote {i+1}/{num_batches} procesado.", flush=True)
        return pd.Series(results, index=series.index), num_batches, error_batches

    df_contacts["country_name_iso"], num_batches, error_batches = _process_phone_numbers(df_contacts["phone"], batch_size=1000)
    
    print("[EXTRACTION START ▶️] Extrayendo estatus de llamadas...", flush=True)
    query_calls = (
        f"SELECT {call_match_field} AS {source_contact_id},\n"
        f"       {call_status_field} AS call_status,\n"
        f"       {call_createdate_field} AS call_createdate\n"
        f"FROM {call_table}\n"
        f"WHERE {call_match_field} IS NOT NULL"
    )
    df_calls = client.query(query_calls).to_dataframe()
    print("[EXTRACTION SUCCESS ✅] Estatus de llamadas extraídos.", flush=True)
    
    mapping_df = pd.merge(
        df_contacts[[source_contact_id, "phone", "country_name_iso"]],
        df_calls[[source_contact_id, "call_status", "call_createdate"]],
        on=source_contact_id, how="left"
    )
    mapping_df = mapping_df.dropna(subset=["country_name_iso", "call_status"], how="all")
    
    total_reg = len(df_contacts)
    exitos = df_contacts["country_name_iso"].notna().sum()
    fallidos = total_reg - exitos
    print(f"[METRICS [INFO ℹ️]] Estadísticas: Total: {total_reg}, Exitosos: {exitos} ({(exitos/total_reg)*100:.2f}%), Fallidos: {fallidos} ({(fallidos/total_reg)*100:.2f}%)", flush=True)
    
    parts = target_table.split(".")
    if len(parts) != 3:
        raise ValueError("[VALIDATION [ERROR ❌]] 'target_table' debe ser 'proyecto.dataset.tabla'.")
    dest_project, dest_dataset, _ = parts
    temp_table = config.get("temp_table_name", "temp_country_mapping_from_phone")
    aux_table = f"{dest_project}.{dest_dataset}.{temp_table}"
    
    print(f"[LOAD START ▶️] Subiendo tabla auxiliar {aux_table}...", flush=True)
    pandas_gbq.to_gbq(mapping_df,
                      destination_table=aux_table,
                      project_id=dest_project,
                      if_exists="replace",
                      credentials=creds)
    
    def _build_update_sql(aux_tbl: str, client: bigquery.Client) -> str:
        try:
            target_tbl_obj = client.get_table(target_table)
            target_fields = [field.name for field in target_tbl_obj.schema]
        except Exception:
            target_fields = []
        
        if isinstance(call_status_values_list, list):
            call_status_sql = ", ".join(f"'{status}'" for status in call_status_values_list)
        else:
            call_status_sql = call_status_values_list
        
        if target_call_status_field.strip() == "":
            additional_field = f", l.country_name_iso AS {target_country_field}"
        else:
            additional_field = f", l.country_name_iso AS {target_country_field}, l.call_status AS {target_call_status_field}"
        
        join_sql = (
            f"CREATE OR REPLACE TABLE {target_table} AS\n"
            f"WITH latest_calls AS (\n"
            f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY {source_contact_id} ORDER BY call_createdate DESC) AS rn\n"
            f"  FROM {aux_tbl}\n"
            f"  WHERE call_status IN ({call_status_sql})\n"
            f")\n"
            f"SELECT d.*{additional_field}\n"
            f"FROM {target_table} d\n"
            f"LEFT JOIN latest_calls l\n"
            f"  ON d.{target_id_match_field} = l.{source_contact_id} AND l.rn = 1;"
        )
        drop_sql = ""
        if config.get("temp_table_erase", True):
            drop_sql = f"\nDROP TABLE {aux_tbl};"
        return join_sql + "\n" + drop_sql

    sql_script = _build_update_sql(aux_table, client)
    print("[TRANSFORMATION [SUCCESS ✅]] SQL generado para actualizar la tabla destino.", flush=True)
    print(sql_script, flush=True)
    print("[END FINISHED ✅] Proceso finalizado.\n", flush=True)
    return sql_script















# __________________________________________________________________________________________________________________________________________________________
def SQL_generate_deal_ordinal_str(params) -> str:
    """
    Genera un script SQL que crea o reemplaza una tabla con un campo ordinal para negocios por contacto,
    basándose en la fecha de creación y filtrando por un campo determinado.

    Parámetros en params:
      - table_source (str): Tabla origen.
      - table_destination (str): Tabla destino.
      - contact_id_field (str): Campo identificador del contacto.
      - deal_id_field (str): Campo identificador del negocio.
      - deal_createdate_field (str): Campo con la fecha de creación.
      - deal_filter_field (str): Campo para filtrar.
      - deal_filter_values (list): Valores permitidos para el filtro.
      - deal_ordinal_field_name (str): Nombre del campo ordinal.

    Retorna:
        str: Script SQL generado.
    """
    print("[START ▶️] Generando SQL para campo ordinal de negocios...", flush=True)
    table_source = params["table_source"]
    table_destination = params["table_destination"]
    contact_id_field = params["contact_id_field"]
    deal_id_field = params["deal_id_field"]
    deal_createdate_field = params["deal_createdate_field"]
    deal_filter_field = params["deal_filter_field"]
    deal_filter_values = params["deal_filter_values"]
    deal_ordinal_field_name = params["deal_ordinal_field_name"]

    filter_str = ", ".join([f"'{v}'" for v in deal_filter_values])
    sql_script = (
        f"CREATE OR REPLACE TABLE `{table_destination}` AS\n"
        f"WITH deals_filtered AS (\n"
        f"  SELECT\n"
        f"    {contact_id_field},\n"
        f"    {deal_id_field},\n"
        f"    ROW_NUMBER() OVER (\n"
        f"      PARTITION BY {contact_id_field}\n"
        f"      ORDER BY {deal_createdate_field}\n"
        f"    ) AS {deal_ordinal_field_name}\n"
        f"  FROM `{table_source}`\n"
        f"  WHERE {deal_filter_field} IN ({filter_str})\n"
        f")\n"
        f"SELECT\n"
        f"  src.*,\n"
        f"  f.{deal_ordinal_field_name}\n"
        f"FROM `{table_source}` src\n"
        f"LEFT JOIN deals_filtered f\n"
        f"  ON src.{contact_id_field} = f.{contact_id_field}\n"
        f"  AND src.{deal_id_field} = f.{deal_id_field}\n"
        f";"
    )
    print("[END FINISHED ✅] SQL para ordinal de negocios generado.\n", flush=True)
    return sql_script











# __________________________________________________________________________________________________________________________________________________________
def SQL_generate_new_columns_from_mapping(config: dict) -> tuple:
    """
    Genera un script SQL que agrega nuevas columnas a una tabla de BigQuery a partir de un mapeo definido en un DataFrame de referencia.
    Sube una tabla auxiliar con el mapeo y realiza un JOIN para incorporar las nuevas columnas.

    Parámetros en config:
      - source_table_to_add_fields (str): Tabla fuente.
      - source_table_to_add_fields_reference_field_name (str): Campo de unión.
      - referece_table_for_new_values_df (pd.DataFrame): DataFrame de referencia.
      - referece_table_for_new_values_field_names_dic (dict): Diccionario de campos a incorporar.
      - values_non_matched_result (str): Valor para registros sin match.
      Además, config debe incluir las claves comunes para autenticación.

    Retorna:
        tuple: (sql_script (str), mapping_df (pd.DataFrame))
    """
    # Importar pandas y re, además de unicodedata para la normalización
    import pandas as pd
    import pandas_gbq
    import re
    import unicodedata
    print("[START ▶️] Generando SQL para agregar nuevas columnas desde mapeo...", flush=True)
    source_table = config.get("source_table_to_add_fields")
    source_field = config.get("source_table_to_add_fields_reference_field_name")
    ref_df = config.get("referece_table_for_new_values_df")
    ref_fields_dic = config.get("referece_table_for_new_values_field_names_dic")
    non_matched = config.get("values_non_matched_result", "descartado")
    if not (isinstance(source_table, str) and source_table):
        raise ValueError("[VALIDATION [ERROR ❌]] 'source_table_to_add_fields' es obligatorio.")
    if not (isinstance(source_field, str) and source_field):
        raise ValueError("[VALIDATION [ERROR ❌]] 'source_table_to_add_fields_reference_field_name' es obligatorio.")
    if not isinstance(ref_df, pd.DataFrame) or ref_df.empty:
        raise ValueError("[VALIDATION [ERROR ❌]] 'referece_table_for_new_values_df' debe ser un DataFrame válido y no vacío.")
    if not isinstance(ref_fields_dic, dict) or not ref_fields_dic:
        raise ValueError("[VALIDATION [ERROR ❌]] 'referece_table_for_new_values_field_names_dic' debe ser un diccionario no vacío.")
    
    print("[METRICS [INFO ℹ️]] Parámetros validados.", flush=True)

    def _normalize_text(text: str) -> str:
        text = text.lower().strip()
        text = unicodedata.normalize('NFD', text)
        return ''.join(c for c in text if unicodedata.category(c) != 'Mn').strip()

    def _sanitize_field_name(name: str) -> str:
        name = name.lower().strip()
        name = unicodedata.normalize('NFD', name)
        name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
        name = re.sub(r'\s+', '_', name)
        name = re.sub(r'[^a-z0-9_]', '', name)
        if re.match(r'^\d', name):
            name = '_' + name
        return name

    ref_field_names = list(ref_fields_dic.keys())
    sanitized_columns = {col: _sanitize_field_name(col) for col in ref_field_names}

    # Asumimos que _extract_source_values utiliza el cliente de BigQuery; este cliente se obtiene mediante _ini_authenticate_API
    def _extract_source_values(tbl: str, field: str, client) -> pd.DataFrame:
        query = f"SELECT DISTINCT `{field}` AS raw_value FROM `{tbl}`"
        return client.query(query).to_dataframe()

    def _build_reference_mapping(df_ref: pd.DataFrame, fields: list) -> dict:
        mapping = {}
        match_field = fields[0]
        for _, row in df_ref.iterrows():
            key_val = row.get(match_field)
            if isinstance(key_val, str):
                norm_key = _normalize_text(key_val)
                mapping[norm_key] = {col: row.get(col, non_matched) for col in fields}
        return mapping

    def _apply_mapping(df_src: pd.DataFrame, ref_mapping: dict) -> dict:
        mapping_results = {}
        for raw in df_src["raw_value"]:
            if not isinstance(raw, str) or not raw:
                mapping_results[raw] = {col: non_matched for col in ref_field_names}
                continue
            norm_raw = _normalize_text(raw)
            if norm_raw in ref_mapping:
                mapping_results[raw] = ref_mapping[norm_raw]
                print(f"[TRANSFORMATION SUCCESS ✅] [MATCH] '{raw}' encontrado en referencia.", flush=True)
            else:
                mapping_results[raw] = {col: non_matched for col in ref_field_names}
                print(f"[TRANSFORMATION WARNING ⚠️] [NO MATCH] '{raw}' no encontrado.", flush=True)
        return mapping_results

    # Obtener cliente de BigQuery
    from google.cloud import bigquery
    client = bigquery.Client(project=source_table.split(".")[0], credentials=_ini_authenticate_API(config, source_table.split(".")[0]))
    df_source = _extract_source_values(source_table, source_field, client)
    if df_source.empty:
        print("[EXTRACTION WARNING ⚠️] No se encontraron valores en la tabla source.", flush=True)
        return ("", pd.DataFrame())
    print(f"[EXTRACTION SUCCESS ✅] Se encontraron {len(df_source)} valores únicos.", flush=True)

    ref_mapping = _build_reference_mapping(ref_df, ref_field_names)
    mapping_results = _apply_mapping(df_source, ref_mapping)
    mapping_rows = []
    for raw, mapping_dict in mapping_results.items():
        row = {"raw_value": raw}
        for col, value in mapping_dict.items():
            sanitized_col = sanitized_columns.get(col, col)
            row[sanitized_col] = value
        mapping_rows.append(row)
    mapping_df = pd.DataFrame(mapping_rows)
    
    parts = source_table.split(".")
    if len(parts) != 3:
        raise ValueError("[VALIDATION ERROR ❌] 'source_table_to_add_fields' debe tener el formato 'proyecto.dataset.tabla'.")
    dest_project, dest_dataset, _ = parts
    aux_table = f"{dest_project}.{dest_dataset}.temp_new_columns_mapping"
    
    pandas_gbq.to_gbq(mapping_df, destination_table=aux_table, project_id=dest_project, if_exists="replace", credentials=client._credentials)
    
    join_fields = [col for col in ref_field_names if ref_fields_dic.get(col, False)]
    new_columns_sql = ",\n".join([f"m.`{sanitized_columns[col]}` AS `{sanitized_columns[col]}`" for col in join_fields])
    update_sql = (
        f"CREATE OR REPLACE TABLE `{source_table}` AS\n"
        f"SELECT s.*, {new_columns_sql}\n"
        f"FROM `{source_table}` s\n"
        f"LEFT JOIN `{aux_table}` m\n"
        f"  ON s.`{source_field}` = m.raw_value;"
    )
    drop_sql = f"DROP TABLE `{aux_table}`;"
    sql_script = update_sql + "\n" + drop_sql
    print("[END FINISHED ✅] SQL para nuevas columnas generado.", flush=True)
    return sql_script, mapping_df










# __________________________________________________________________________________________________________________________________________________________
def SQL_generation_normalize_strings(config: dict) -> tuple:
    """
    Normaliza los valores de una columna en una tabla de BigQuery utilizando mapeo manual y fuzzy matching.
    Sube una tabla auxiliar con el mapeo y genera un script SQL para actualizar la tabla fuente.

    Parámetros en config:
      - source_table_to_normalize (str): Tabla fuente.
      - source_table_to_normalize_field_name (str): Columna a normalizar.
      - referece_table_for_normalization_manual_df (pd.DataFrame): DataFrame con columnas 'Bruto' y 'Normalizado'.
      - referece_table_for_normalization_rapidfuzz_df (pd.DataFrame): DataFrame para fuzzy matching.
      - referece_table_for_normalization_rapidfuzz_field_name (str): Columna candidata para fuzzy matching.
      - rapidfuzz_score_filter_use (bool)
      - rapidfuzz_score_filter_min_value (int/float)
      - rapidfuzz_score_filter_no_pass_mapping (str)
      - destination_field_name (str, opcional): Nombre del campo donde se almacenará el valor normalizado.
      Además, config debe incluir las claves comunes para autenticación.

    Retorna:
        tuple: (sql_script (str), df_fuzzy_results (pd.DataFrame))
    """
    # Importar librerías necesarias
    import pandas as pd
    import pandas_gbq
    import unicodedata
    import re
    print("[NORMALIZATION START ▶️] Iniciando normalización de cadenas...", flush=True)
    source_table = config.get("source_table_to_normalize")
    source_field = config.get("source_table_to_normalize_field_name")
    manual_df = config.get("referece_table_for_normalization_manual_df")
    rapidfuzz_df = config.get("referece_table_for_normalization_rapidfuzz_df")
    rapidfuzz_field = config.get("referece_table_for_normalization_rapidfuzz_field_name")
    rapidfuzz_filter_use = config.get("rapidfuzz_score_filter_use", False)
    rapidfuzz_min_score = config.get("rapidfuzz_score_filter_min_value", 0)
    rapidfuzz_no_pass_value = config.get("rapidfuzz_score_filter_no_pass_mapping", "descartado")
    destination_field_name = config.get("destination_field_name", "").strip()
    
    if not (isinstance(source_table, str) and source_table):
        raise ValueError("[VALIDATION [ERROR ❌]] 'source_table_to_normalize' es obligatorio.")
    if not (isinstance(source_field, str) and source_field):
        raise ValueError("[VALIDATION [ERROR ❌]] 'source_table_to_normalize_field_name' es obligatorio.")
    if not isinstance(manual_df, pd.DataFrame) or manual_df.empty:
        raise ValueError("[VALIDATION [ERROR ❌]] 'referece_table_for_normalization_manual_df' debe ser un DataFrame no vacío.")
    if not isinstance(rapidfuzz_df, pd.DataFrame) or rapidfuzz_df.empty:
        raise ValueError("[VALIDATION [ERROR ❌]] 'referece_table_for_normalization_rapidfuzz_df' debe ser un DataFrame no vacío.")
    if not (isinstance(rapidfuzz_field, str) and rapidfuzz_field):
        raise ValueError("[VALIDATION [ERROR ❌]] 'referece_table_for_normalization_rapidfuzz_field_name' es obligatorio.")

    def _normalize_text(text: str) -> str:
        text = text.lower().strip()
        text = unicodedata.normalize('NFD', text)
        return ''.join(c for c in text if unicodedata.category(c) != 'Mn').strip()

    def _extract_source_values(tbl: str, field: str, client) -> pd.DataFrame:
        query = f"SELECT DISTINCT `{field}` AS raw_value FROM `{tbl}`"
        return client.query(query).to_dataframe()

    def _build_manual_mapping(df_manual: pd.DataFrame) -> dict:
        mapping = {}
        for _, row in df_manual.iterrows():
            bruto = row["Bruto"]
            normalizado = row["Normalizado"]
            if isinstance(bruto, str):
                mapping[_normalize_text(bruto)] = normalizado
        return mapping

    def _build_rapidfuzz_candidates(df_rapid: pd.DataFrame, field: str) -> dict:
        candidates = {}
        for _, row in df_rapid.iterrows():
            candidate = row[field]
            if isinstance(candidate, str):
                candidates[_normalize_text(candidate)] = candidate
        return candidates

    def _apply_mapping(df_src: pd.DataFrame, manual_map: dict, rapid_candidates: dict) -> tuple:
        from rapidfuzz import process, fuzz
        mapping_results = {}
        fuzzy_results_list = []
        candidate_keys = list(rapid_candidates.keys())
        for raw in df_src["raw_value"]:
            if not isinstance(raw, str) or not raw:
                mapping_results[raw] = raw
                continue
            normalized_raw = _normalize_text(raw)
            if normalized_raw in manual_map:
                mapping_results[raw] = manual_map[normalized_raw]
                print(f"[TRANSFORMATION SUCCESS ✅] [MANUAL] '{raw}' mapeado a: {manual_map[normalized_raw]}", flush=True)
            else:
                best_match = process.extractOne(normalized_raw, candidate_keys, scorer=fuzz.ratio)
                if best_match:
                    match_key, score, _ = best_match
                    if rapidfuzz_filter_use and score < rapidfuzz_min_score:
                        mapping_results[raw] = rapidfuzz_no_pass_value
                        fuzzy_results_list.append({source_field: raw, "normalized_value": rapidfuzz_no_pass_value, "Rapidfuzz score": score})
                        print(f"[TRANSFORMATION WARNING ⚠️] [FUZZY] '{raw}' obtuvo score {score} (< {rapidfuzz_min_score}). Se asigna: {rapidfuzz_no_pass_value}", flush=True)
                    else:
                        mapping_results[raw] = rapid_candidates[match_key]
                        fuzzy_results_list.append({source_field: raw, "normalized_value": rapid_candidates[match_key], "Rapidfuzz score": score})
                        print(f"[TRANSFORMATION SUCCESS ✅] [FUZZY] '{raw}' mapeado a: {rapid_candidates[match_key]} (Score: {score})", flush=True)
                else:
                    mapping_results[raw] = rapidfuzz_no_pass_value
                    fuzzy_results_list.append({source_field: raw, "normalized_value": rapidfuzz_no_pass_value, "Rapidfuzz score": None})
                    print(f"[TRANSFORMATION ERROR ❌] No se encontró mapeo para '{raw}'. Se asigna: {rapidfuzz_no_pass_value}", flush=True)
        return mapping_results, fuzzy_results_list

    print(f"[EXTRACTION START ▶️] Extrayendo valores únicos de `{source_field}` desde {source_table}...", flush=True)
    from google.cloud import bigquery
    client = bigquery.Client(project=source_table.split(".")[0], credentials=_ini_authenticate_API(config, source_table.split(".")[0]))
    df_source = _extract_source_values(source_table, source_field, client)
    if df_source.empty:
        print("[EXTRACTION WARNING ⚠️] No se encontraron valores en la columna fuente.", flush=True)
        return ("", pd.DataFrame())
    print(f"[EXTRACTION SUCCESS ✅] Se encontraron {len(df_source)} valores únicos.", flush=True)

    manual_map = _build_manual_mapping(manual_df)
    rapid_candidates = _build_rapidfuzz_candidates(rapidfuzz_df, rapidfuzz_field)
    mapping_results, fuzzy_results_list = _apply_mapping(df_source, manual_map, rapid_candidates)
    
    mapping_df = pd.DataFrame(list(mapping_results.items()), columns=["raw_value", "normalized_value"])
    parts = source_table.split(".")
    if len(parts) != 3:
        raise ValueError("[VALIDATION ERROR ❌] 'source_table_to_normalize' debe tener el formato 'proyecto.dataset.tabla'.")
    dest_project, dest_dataset, _ = parts
    aux_table = f"{dest_project}.{dest_dataset}.temp_normalized_strings"
    
    pandas_gbq.to_gbq(mapping_df,
                      destination_table=aux_table,
                      project_id=dest_project,
                      if_exists="replace",
                      credentials=client._credentials)
    
    if destination_field_name:
        update_sql = (
            f"CREATE OR REPLACE TABLE `{source_table}` AS\n"
            f"SELECT s.*, m.normalized_value AS `{destination_field_name}`\n"
            f"FROM `{source_table}` s\n"
            f"LEFT JOIN `{aux_table}` m\n"
            f"  ON s.`{source_field}` = m.raw_value;"
        )
    else:
        update_sql = (
            f"CREATE OR REPLACE TABLE `{source_table}` AS\n"
            f"SELECT s.* REPLACE(m.normalized_value AS `{source_field}`)\n"
            f"FROM `{source_table}` s\n"
            f"LEFT JOIN `{aux_table}` m\n"
            f"  ON s.`{source_field}` = m.raw_value;"
        )
    drop_sql = f"DROP TABLE `{aux_table}`;"
    sql_script = update_sql + "\n" + drop_sql
    print("[END FINISHED ✅] SQL para normalización generado.", flush=True)
    return sql_script, pd.DataFrame(fuzzy_results_list)










# __________________________________________________________________________________________________________________________________________________________
def SQL_generate_join_tables_str(params: dict) -> str:
    """
    Crea o reemplaza una tabla uniendo una tabla primaria, una secundaria y opcionalmente una tabla puente,
    aplicando prefijos a las columnas para evitar duplicados.

    Parámetros en params:
      - table_source_primary (str): Tabla primaria.
      - table_source_primary_id_field (str): Campo de unión en la tabla primaria.
      - table_source_secondary (str): Tabla secundaria.
      - table_source_secondary_id (str): Campo de unión en la tabla secundaria.
      - table_source_bridge_use (bool): Indica si se utiliza tabla puente.
      - table_source_bridge (str, opcional): Tabla puente.
      - table_source_bridge_ids_fields (dict, opcional): Diccionario con keys 'primary_id' y 'secondary_id'.
      - join_type (str, opcional): Tipo de JOIN (por defecto "LEFT").
      - join_field_prefixes (dict, opcional): Prefijos para las columnas.
      - table_destination (str): Tabla destino.
      Además, se espera que params incluya las claves comunes para autenticación.

    Retorna:
        str: Sentencia SQL generada.
    """
    print("[START ▶️] Generando SQL para unión de tablas...", flush=True)
    import os
    # Función interna para dividir el nombre completo de una tabla
    def split_table(full_name: str):
        parts = full_name.split(".")
        if len(parts) == 2:
            project = params.get("GCP_project_id") or os.environ.get("GOOGLE_CLOUD_PROJECT")
            if not project:
                raise ValueError("[VALIDATION [ERROR ❌]] Se requiere 'GCP_project_id' para formato 'dataset.table'.")
            return project, parts[0], parts[1]
        elif len(parts) == 3:
            return parts[0], parts[1], parts[2]
        else:
            raise ValueError(f"[VALIDATION [ERROR ❌]] Nombre de tabla inválido: {full_name}")

    def get_columns(full_table: str):
        proj, dataset, table = split_table(full_table)
        print(f"[EXTRACTION [START ▶️]] Obteniendo columnas de {full_table}...", flush=True)
        # Importar librerías necesarias para autenticación interna
        if os.environ.get("GOOGLE_CLOUD_PROJECT"):
            from google.cloud import secretmanager
            secret_id = params.get("json_keyfile_GCP_secret_id")
            if not secret_id:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] En GCP se debe proporcionar 'json_keyfile_GCP_secret_id'.")
            client_sm = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{proj}/secrets/{secret_id}/versions/latest"
            response = client_sm.access_secret_version(name=secret_name)
            secret_str = response.payload.data.decode("UTF-8")
            import json
            secret_info = json.loads(secret_str)
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_info(secret_info)
            print("[AUTHENTICATION [SUCCESS ✅]] Credenciales obtenidas desde Secret Manager.", flush=True)
        else:
            json_path = params.get("json_keyfile_colab")
            if not json_path:
                raise ValueError("[AUTHENTICATION [ERROR ❌]] Se debe proporcionar 'json_keyfile_colab' en entornos no GCP.")
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(json_path)
            print("[AUTHENTICATION [SUCCESS ✅]] Credenciales cargadas desde JSON.", flush=True)
        from google.cloud import bigquery
        client = bigquery.Client(project=proj, credentials=creds)
        query = (
            f"SELECT column_name\n"
            f"FROM `{proj}.{dataset}.INFORMATION_SCHEMA.COLUMNS`\n"
            f"WHERE table_name = '{table}'\n"
            f"ORDER BY ordinal_position"
        )
        rows = client.query(query).result()
        cols = [row.column_name for row in rows]
        print(f"[EXTRACTION [SUCCESS ✅]] {len(cols)} columnas encontradas en {full_table}.", flush=True)
        return cols

    table_primary = params["table_source_primary"]
    primary_id_field = params["table_source_primary_id_field"]
    table_secondary = params["table_source_secondary"]
    secondary_id_field = params["table_source_secondary_id"]
    bridge_use = params.get("table_source_bridge_use", False)
    table_bridge = params.get("table_source_bridge", "")
    bridge_ids_fields = params.get("table_source_bridge_ids_fields", {})
    join_type = params.get("join_type", "LEFT").upper()
    valid_joins = ["INNER", "LEFT", "RIGHT", "FULL", "CROSS"]
    if join_type not in valid_joins:
        raise ValueError(f"[VALIDATION [ERROR ❌]] join_type '{join_type}' no es válido. Debe ser uno de {valid_joins}.")
    prefixes = params.get("join_field_prefixes", {"primary": "p_", "secondary": "s_", "bridge": "b_"})
    table_destination = params["table_destination"]

    primary_cols = get_columns(table_primary)
    secondary_cols = get_columns(table_secondary)
    bridge_cols = []
    if bridge_use and table_bridge:
        bridge_cols = get_columns(table_bridge)
    
    primary_select = [f"{prefixes['primary']}.{col} AS {prefixes['primary']}{col}" for col in primary_cols]
    secondary_select = [f"{prefixes['secondary']}.{col} AS {prefixes['secondary']}{col}" for col in secondary_cols]
    bridge_select = []
    if bridge_use and bridge_cols:
        bridge_select = [f"{prefixes['bridge']}.{col} AS {prefixes['bridge']}{col}" for col in bridge_cols]
    
    all_select = primary_select + secondary_select + bridge_select
    select_clause = ",\n  ".join(all_select)
    
    if bridge_use and table_bridge:
        join_clause = (
            f"FROM `{table_primary}` AS {prefixes['primary']}\n"
            f"{join_type} JOIN `{table_bridge}` AS {prefixes['bridge']}\n"
            f"  ON {prefixes['bridge']}.{bridge_ids_fields['primary_id']} = {prefixes['primary']}.{primary_id_field}\n"
            f"{join_type} JOIN `{table_secondary}` AS {prefixes['secondary']}\n"
            f"  ON {prefixes['bridge']}.{bridge_ids_fields['secondary_id']} = {prefixes['secondary']}.{secondary_id_field}\n"
        )
    else:
        join_clause = (
            f"FROM `{table_primary}` AS {prefixes['primary']}\n"
            f"{join_type} JOIN `{table_secondary}` AS {prefixes['secondary']}\n"
            f"  ON {prefixes['primary']}.{primary_id_field} = {prefixes['secondary']}.{secondary_id_field}\n"
        )
    sql_script = (
        f"CREATE OR REPLACE TABLE `{table_destination}` AS\n"
        f"SELECT\n"
        f"  {select_clause}\n"
        f"{join_clause}"
        f";"
    )
    print("[END FINISHED ✅] SQL para unión de tablas generado.\n", flush=True)
    return sql_script



















# __________________________________________________________________________________________________________________________________________________________
def SQL_generate_BI_view_str(params: dict) -> str:
    """
    Crea o reemplaza una vista BI a partir de una tabla fuente, aplicando mapeos y filtros (rango de fechas, exclusión de registros borrados).

    Parámetros en params:
      - table_source (str): Tabla origen.
      - table_destination (str): Vista o tabla destino.
      - fields_mapped_df (pd.DataFrame): DataFrame con columnas ["Campo Original", "Campo Formateado"].
      - use_mapped_names (bool): Si True, usa nombres formateados.
      - creation_date_field (str, opcional): Campo de fecha.
      - use_date_range (bool): Si True, filtra por rango de fechas.
      - date_range (dict, opcional): {"from": "YYYY-MM-DD", "to": "YYYY-MM-DD"}.
      - exclude_deleted_records_bool (bool): Si True, excluye registros marcados como borrados.
      - exclude_deleted_records_field_name (str, opcional)
      - exclude_deleted_records_field_value: Valor que indica borrado.

    Retorna:
        str: Sentencia SQL generada.
    """
    # Importar pandas para validar el DataFrame
    import pandas as pd
    print("[START ▶️] Generando vista BI...", flush=True)
    table_source = params.get("table_source")
    table_destination = params.get("table_destination")
    fields_mapped_df = params.get("fields_mapped_df")
    if not table_source or not table_destination or not isinstance(fields_mapped_df, pd.DataFrame):
        raise ValueError("[VALIDATION [ERROR ❌]] Faltan parámetros obligatorios: 'table_source', 'table_destination' o 'fields_mapped_df'.")
    
    use_mapped_names = params.get("use_mapped_names", True)
    creation_date_field = params.get("creation_date_field", "")
    date_range = params.get("date_range", {})
    use_date_range = params.get("use_date_range", False)
    exclude_deleted_records_bool = params.get("exclude_deleted_records_bool", False)
    exclude_deleted_records_field_name = params.get("exclude_deleted_records_field_name", "")
    exclude_deleted_records_field_value = params.get("exclude_deleted_records_field_value", None)
    
    select_cols = []
    for _, row in fields_mapped_df.iterrows():
        original = row["Campo Original"]
        mapped = row["Campo Formateado"]
        if use_mapped_names:
            select_cols.append(f"`{original}` AS `{mapped}`")
        else:
            select_cols.append(f"`{original}`")
    select_clause = ",\n  ".join(select_cols)
    
    where_filters = []
    if use_date_range and creation_date_field:
        date_from = date_range.get("from", "")
        date_to = date_range.get("to", "")
        if date_from and date_to:
            where_filters.append(f"`{creation_date_field}` BETWEEN '{date_from}' AND '{date_to}'")
        elif date_from:
            where_filters.append(f"`{creation_date_field}` >= '{date_from}'")
        elif date_to:
            where_filters.append(f"`{creation_date_field}` <= '{date_to}'")
    if exclude_deleted_records_bool and exclude_deleted_records_field_name and exclude_deleted_records_field_value is not None:
        where_filters.append(f"(`{exclude_deleted_records_field_name}` IS NULL OR `{exclude_deleted_records_field_name}` != {exclude_deleted_records_field_value})")
    where_clause = " AND ".join(where_filters) if where_filters else "TRUE"
    
    sql_script = (
        f"CREATE OR REPLACE VIEW `{table_destination}` AS\n"
        f"SELECT\n"
        f"  {select_clause}\n"
        f"FROM `{table_source}`\n"
        f"WHERE {where_clause}\n"
        f";"
    )
    print("[END [FINISHED ✅]] Vista BI generada.\n", flush=True)
    return sql_script








# __________________________________________________________________________________________________________________________________________________________
def SQL_generate_CPL_to_contacts_str(params: dict) -> str:
    """
    Genera una sentencia SQL para crear o reemplazar una tabla que combina datos de contactos, métricas agregadas y métricas publicitarias,
    calculando indicadores como el CPL (costo por lead) a nivel de contacto.

    Parámetros en params:
      - table_destination (str): Tabla resultado.
      - table_source (str): Tabla de contactos.
      - table_aggregated (str): Tabla agregada de métricas.
      - join_field (str): Campo de fecha para unión.
      - join_on_source (str): Campo de fuente para unión.
      - contact_creation_number (str): Campo que indica el número de contactos creados.
      - ad_platforms (list): Lista de diccionarios con configuraciones de plataformas.

    Retorna:
        str: Sentencia SQL generada.
    """
    print("[START ▶️] Generando SQL para CPL a contacts...", flush=True)
    table_destination = params["table_destination"]
    table_source = params["table_source"]
    table_aggregated = params["table_aggregated"]
    join_field = params["join_field"]
    join_on_source = params["join_on_source"]
    contact_creation_number = params["contact_creation_number"]
    ad_platforms = params["ad_platforms"]

    from_clause = (
        f"FROM `{table_source}` o\n"
        f"LEFT JOIN `{table_aggregated}` a\n"
        f"  ON DATE(o.{join_field}) = a.{join_field}\n"
        f"  AND o.{join_on_source} = a.{join_on_source}\n"
    )
    joins = []
    select_platform_metrics = []
    for idx, plat in enumerate(ad_platforms, start=1):
        alias = f"p{idx}"
        prefix = plat["prefix"]
        table = plat["table"]
        date_field = plat["date_field"]
        source_value = plat["source_value"]
        joins.append(
            f"LEFT JOIN `{table}` {alias}\n"
            f"  ON a.{join_field} = {alias}.{date_field}\n"
        )
        for key, value in plat.items():
            if key.startswith("total_"):
                metric = key.replace("total_", "")
                col_name = f"contact_Ads_{prefix}_{metric}_by_day"
                expr = (
                    f"CASE\n"
                    f"  WHEN a.{join_on_source} = \"{source_value}\" AND a.{contact_creation_number} > 0\n"
                    f"    THEN {alias}.{value} / a.{contact_creation_number}\n"
                    f"  ELSE NULL\n"
                    f"END AS {col_name}"
                )
                select_platform_metrics.append(expr)
    final_select = ",\n".join(["o.*"] + select_platform_metrics)
    join_clause = "".join(joins)
    sql_script = (
        f"CREATE OR REPLACE TABLE `{table_destination}` AS\n"
        f"SELECT\n"
        f"  {final_select}\n"
        f"{from_clause}"
        f"{join_clause}\n"
        f";"
    )
    print("[END [FINISHED ✅]] SQL para CPL a contacts generado.\n", flush=True)
    return sql_script













