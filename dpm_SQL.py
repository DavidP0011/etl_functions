#__________________________________________________________________________________________________________________________________________________________
# @title IMPORTACIÓN DE LIBRERÍAS
#__________________________________________________________________________________________________________________________________________________________
from google.cloud import bigquery
import pandas as pd
import pandas_gbq
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


#__________________________________________________________________________________________________________________________________________________________
# @title SQL_generate_country_name_mapping()
#__________________________________________________________________________________________________________________________________________________________
def SQL_generate_country_from_phone(config: dict) -> str:
    """
    Función unificada que:
      1. Extrae datos de la tabla de contactos y obtiene el país a partir de su número de teléfono.
      2. Extrae el estatus de llamada (engagement_call) para cada contacto, determinando el estatus más relevante
         según la escala definida:
              'COMPLETED'   -> 1
              'IN_PROGRESS' -> 2
              'CONNECTING'  -> 3
              'QUEUED'      -> 4
              'BUSY'        -> 5
              'NO_ANSWER'   -> 6
              'FAILED'      -> 7
              'CANCELED'    -> 8
              Otros         -> 9
         Si un contacto tiene varias llamadas, se selecciona el estatus con menor ranking (más relevante).
      3. Se procesa en lotes para permitir interrupciones en entornos como Google Colab.
      4. Se filtran registros:
           - Se eliminan aquellos en los que el teléfono esté vacío.
           - Se eliminan los registros donde tanto el país (country_name_iso) como el estatus de llamada (best_call_status) sean nulos.
      5. Se sube una tabla auxiliar en el mismo dataset de destination_table con los datos de mapeo:
           - {source_contact_id_field_name}: identificador del contacto.
           - {source_contact_phone_field}: teléfono.
           - country_name_iso: país derivado del teléfono.
           - best_call_status: estatus de llamada relevante.
      6. Se genera el script SQL para actualizar la tabla destino:
            - Si las columnas destino ya existen, se usan con REPLACE; de lo contrario se añaden.
            - Se incluyen las columnas de país y de estatus de llamada.
            - Se incluye el SQL para eliminar la tabla auxiliar (DROP TABLE).
         (La ejecución del script se hará desde otra función, por ejemplo, GBQ_execute_SQL()).
    
    Parámetros en config:
      - source_table (str): Tabla de contactos en formato `proyecto.dataset.tabla`.
      - source_contact_phone_field (str): Campo del teléfono en la tabla de contactos.
      - source_contact_id_field_name (str): Campo identificador en la tabla de contactos.
      
      - source_engagement_call_table (str): Tabla de llamadas en formato `proyecto.dataset.tabla`.
      - source_engagement_call_id_match_contact_field_name (str): Campo de la tabla de llamadas que relaciona al contacto.
      - source_engagement_call_status_field_name (str): Campo de la tabla de llamadas con el estatus de llamada.
      
      - destination_table (str): Tabla destino en formato `proyecto.dataset.tabla`.
      - destination_id_match_contact_field_name (str): Campo identificador en la tabla destino (mismo que en contactos).
      - destination_country_mapped_field_name (str): Nombre de la columna para almacenar el país obtenido.
      - destination_call_status_field_name (str): Nombre de la columna para almacenar el estatus de llamada.
      
      - json_keyfile (str, opcional): Ruta del archivo JSON de credenciales (requerido en entornos locales/Colab).
      - default_phone_prefix (str, opcional): Prefijo a añadir si el teléfono no comienza con "+". Por defecto, "+34".
    
    Retorna:
        str: El script SQL completo (JOIN y DROP) listo para ejecutarse.
    """
    from google.cloud import bigquery
    import pandas as pd
    import pandas_gbq
    import phonenumbers
    import pycountry
    import re
    import unicodedata
    import os
    import math
    import time
    from google.auth import default as gauth_default
    from google.oauth2 import service_account
    # --- Validación de parámetros ---
    source_table = config.get("source_table")
    source_contact_phone_field = config.get("source_contact_phone_field")
    source_contact_id_field_name = config.get("source_contact_id_field_name")
    
    source_engagement_call_table = config.get("source_engagement_call_table")
    source_engagement_call_id_match_contact_field_name = config.get("source_engagement_call_id_match_contact_field_name")
    source_engagement_call_status_field_name = config.get("source_engagement_call_status_field_name")
    
    destination_table = config.get("destination_table")
    destination_id_match_contact_field_name = config.get("destination_id_match_contact_field_name")
    destination_country_mapped_field_name = config.get("destination_country_mapped_field_name")
    destination_call_status_field_name = config.get("destination_call_status_field_name")
    
    default_phone_prefix = config.get("default_phone_prefix", "+34")
    
    required_fields = [
        source_table, source_contact_phone_field, source_contact_id_field_name,
        source_engagement_call_table, source_engagement_call_id_match_contact_field_name, source_engagement_call_status_field_name,
        destination_table, destination_id_match_contact_field_name, destination_country_mapped_field_name, destination_call_status_field_name
    ]
    if not all(isinstance(x, str) and x for x in required_fields):
        raise ValueError("Se deben proporcionar todas las keys obligatorias como cadenas válidas.")
    
    # --- Autenticación consolidada ---
    print("[INFO] Autenticando...", flush=True)
    is_gcp = bool(os.environ.get("GOOGLE_CLOUD_PROJECT"))
    if is_gcp:
        print("[INFO] Entorno GCP detectado. Usando autenticación automática.", flush=True)
        creds, _ = gauth_default()
    else:
        json_keyfile = config.get("json_keyfile")
        if not json_keyfile:
            raise ValueError("En entornos no GCP se debe proporcionar 'json_keyfile' con la ruta al archivo de credenciales.")
        print("[INFO] Entorno local detectado. Usando autenticación con JSON de credenciales.", flush=True)
        creds = service_account.Credentials.from_service_account_file(json_keyfile)
    
    # Crear cliente de BigQuery con autenticación
    source_project = source_table.split(".")[0]
    client = bigquery.Client(project=source_project, credentials=creds)
    
    # --- Subfunción: Normalización de texto ---
    def _normalize_text(texto: str) -> str:
        texto = texto.lower().strip()
        texto = unicodedata.normalize('NFD', texto)
        texto = ''.join(c for c in texto if unicodedata.category(c) != 'Mn')
        texto = re.sub(r'[^a-z0-9\s]', '', texto)
        return texto

    # --- Subfunción: Preprocesamiento del teléfono ---
    def _preprocess_phone(phone: str, default_prefix: str = default_phone_prefix) -> str:
        if phone and isinstance(phone, str):
            phone = phone.strip()
            if not phone.startswith("+"):
                phone = default_prefix + phone
        return phone

    # --- Subfunción: Obtener país a partir del teléfono ---
    def _get_country_from_phone(phone: str) -> str:
        if not phone or not isinstance(phone, str):
            return None
        try:
            phone_obj = phonenumbers.parse(phone, None)
            country_code = phonenumbers.region_code_for_number(phone_obj)
            if country_code:
                try:
                    country_obj = pycountry.countries.get(alpha_2=country_code)
                    return country_obj.name if country_obj else country_code
                except Exception:
                    return country_code
            else:
                return None
        except Exception as e:
            return None

    # --- Subfunción: Procesar teléfonos en lotes ---
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
            except KeyboardInterrupt:
                print(f"[INFO] Proceso interrumpido por el usuario en el lote {i+1} de {num_batches}.", flush=True)
                break
            except Exception as e:
                error_batches += 1
                print(f"[ERROR] Error en el lote {i+1}: {e}", flush=True)
            print(f"[INFO] Procesado lote {i+1}/{num_batches}.", flush=True)
        return pd.Series(results, index=series.index), num_batches, error_batches

    # --- Subfunción: Extraer el estatus de llamada más relevante por contacto ---
    def _get_best_call_status():
        query = f"""
        SELECT contact_id, ANY_VALUE(call_status) AS best_call_status FROM (
          SELECT {source_engagement_call_id_match_contact_field_name} AS contact_id,
                 {source_engagement_call_status_field_name} AS call_status,
                 CASE {source_engagement_call_status_field_name}
                     WHEN 'COMPLETED' THEN 1
                     WHEN 'IN_PROGRESS' THEN 2
                     WHEN 'CONNECTING' THEN 3
                     WHEN 'QUEUED' THEN 4
                     WHEN 'BUSY' THEN 5
                     WHEN 'NO_ANSWER' THEN 6
                     WHEN 'FAILED' THEN 7
                     WHEN 'CANCELED' THEN 8
                     ELSE 9
                 END AS ranking
          FROM `{source_engagement_call_table}`
          WHERE {source_engagement_call_id_match_contact_field_name} IS NOT NULL
        ) t
        GROUP BY contact_id
        """
        print("[INFO] Extrayendo estatus de llamadas por contacto...", flush=True)
        df_calls = client.query(query).to_dataframe()
        return df_calls

    # --- Subfunción: Generar script SQL para actualizar la tabla destino ---
    def _build_update_sql(aux_table: str, client: bigquery.Client) -> str:
        try:
            dest_table = client.get_table(destination_table)
            dest_fields = [field.name for field in dest_table.schema]
        except Exception as e:
            dest_fields = []
        
        if (destination_country_mapped_field_name in dest_fields and 
            destination_call_status_field_name in dest_fields):
            join_sql = (
                f"CREATE OR REPLACE TABLE `{destination_table}` AS\n"
                f"SELECT d.* REPLACE(m.country_name_iso AS `{destination_country_mapped_field_name}`,\n"
                f"                  m.best_call_status AS `{destination_call_status_field_name}`)\n"
                f"FROM `{destination_table}` d\n"
                f"LEFT JOIN `{aux_table}` m\n"
                f"  ON d.{destination_id_match_contact_field_name} = m.{source_contact_id_field_name};"
            )
        else:
            join_sql = (
                f"CREATE OR REPLACE TABLE `{destination_table}` AS\n"
                f"SELECT d.*, m.country_name_iso AS `{destination_country_mapped_field_name}`,\n"
                f"             m.best_call_status AS `{destination_call_status_field_name}`\n"
                f"FROM `{destination_table}` d\n"
                f"LEFT JOIN `{aux_table}` m\n"
                f"  ON d.{destination_id_match_contact_field_name} = m.{source_contact_id_field_name};"
            )
        drop_sql = f"DROP TABLE `{aux_table}`;"
        return join_sql + "\n" + drop_sql

    # --- Inicio del proceso principal ---
    start_time = time.time()
    print(f"[INFO] Extrayendo datos de la tabla de contactos {source_table}...", flush=True)
    query_source = f"""
        SELECT {source_contact_id_field_name}, {source_contact_phone_field}
        FROM `{source_table}`
    """
    df_contacts = client.query(query_source).to_dataframe()
    if df_contacts.empty:
        print("[WARNING] No se encontraron datos en la tabla de contactos.", flush=True)
        return ""
    
    # Agregar la columna del teléfono original para conservarlo en la tabla temporal
    df_contacts.rename(columns={source_contact_phone_field: "phone"}, inplace=True)
    
    # Filtrar registros con teléfono vacío o nulo
    df_contacts = df_contacts[df_contacts["phone"].notna() & (df_contacts["phone"].str.strip() != "")]
    
    print("[INFO] Preprocesando y procesando teléfonos en lotes...", flush=True)
    df_contacts["country_name_iso"], num_batches, error_batches = _process_phone_numbers(
        df_contacts["phone"], batch_size=1000)
    
    # --- Extraer estatus de llamada por contacto ---
    df_calls = _get_best_call_status()
    df_calls.rename(columns={"contact_id": source_contact_id_field_name}, inplace=True)
    
    # Realizar merge: unir por contacto
    mapping_df = pd.merge(df_contacts[[source_contact_id_field_name, "phone", "country_name_iso"]], 
                          df_calls[[source_contact_id_field_name, "best_call_status"]],
                          on=source_contact_id_field_name, how="left")
    
    # Filtrar registros donde ambos valores sean nulos (país y estatus de llamada)
    mapping_df = mapping_df.dropna(subset=["country_name_iso", "best_call_status"], how="all")
    
    # --- Estadísticas de procesamiento ---
    total_registros = len(df_contacts)
    exitos = df_contacts["country_name_iso"].notna().sum()
    fallidos = total_registros - exitos
    tiempo_total = time.time() - start_time

    print("\n[STATS] Resumen del procesamiento:")
    print(f"    Registros totales (con teléfono): {total_registros}")
    print(f"    Registros parseados exitosamente: {exitos} ({(exitos/total_registros)*100:.2f}%)")
    print(f"    Registros fallidos: {fallidos} ({(fallidos/total_registros)*100:.2f}%)")
    print(f"    Lotes procesados: {num_batches} (Lotes con error: {error_batches})")
    print(f"    Tiempo total de procesamiento: {tiempo_total:.2f} segundos")

    # --- Estadísticas de países identificados ---
    country_counts = df_contacts["country_name_iso"].value_counts(dropna=True)
    print("\n[STATS] Distribución por país (ordenado de mayor a menor):")
    for country, count in country_counts.items():
        porcentaje = (count/total_registros)*100
        print(f"    {country}: {count} registros ({porcentaje:.2f}%)")
    
    # --- Preparar tabla auxiliar para actualización ---
    parts = destination_table.split(".")
    if len(parts) != 3:
        raise ValueError("El formato de 'destination_table' debe ser 'proyecto.dataset.tabla'.")
    dest_project, dest_dataset, _ = parts
    aux_table = f"{dest_project}.{dest_dataset}.temp_country_phone_mapping"
    
    print(f"\n[INFO] Subiendo tabla auxiliar {aux_table} con datos de mapeo...", flush=True)
    pandas_gbq.to_gbq(mapping_df,
                        destination_table=aux_table,
                        project_id=dest_project,
                        if_exists="replace",
                        credentials=creds)
    
    sql_script = _build_update_sql(aux_table, client)
    print("\n[INFO] SQL generado para actualizar la tabla destino.", flush=True)
    print(sql_script, flush=True)
    
    return sql_script

#__________________________________________________________________________________________________________________________________________________________
# @title SQL_generate_country_name_mapping()
#__________________________________________________________________________________________________________________________________________________________

def SQL_generate_country_name_mapping(config: dict) -> str:
    """
    Función unificada que:
      1. Extrae datos de una tabla de BigQuery y obtiene la mejor opción de nombre de país según prioridad.
      2. Mapea los nombres de países en español a su equivalente en nombre ISO 3166-1.
         - Se omiten aquellos que aparezcan en country_name_skip_values_list.
         - Se puede sobrescribir manualmente mediante manual_mapping_dic.
      3. Sube una tabla auxiliar en el mismo dataset de destination_table con los datos de mapeo.
      4. Genera el script SQL para actualizar la tabla destino:
            - Si la columna destino ya existe, usa "SELECT d.* REPLACE(m.country_name_iso AS `destination_country_mapped_field_name`)".
            - Si no existe, usa "SELECT d.*, m.country_name_iso AS `destination_country_mapped_field_name`".
            - Incluye el SQL para eliminar la tabla auxiliar (DROP TABLE).
         (La ejecución del script se hará desde otra función, por ejemplo, GBQ_execute_SQL()).
    
    Parámetros en config:
      - source_table (str): Tabla origen en formato `proyecto.dataset.tabla`.
      - source_country_name_best_list (list): Lista de campos de país en orden de prioridad.
      - source_id_name_field (str): Campo identificador en la tabla origen.
      - country_name_skip_values_list (list, opcional): Lista de valores a omitir en el mapeo.
      - manual_mapping_dic (dict, opcional): Diccionario donde cada clave (nombre canónico)
             asocia una lista de posibles variantes (ej. "United States": ["usa", "u.s.a", "estados unidos", ...]).
      - destination_table (str): Tabla destino en formato `proyecto.dataset.tabla`.
      - destination_id_field_name (str): Campo identificador en la tabla destino para el JOIN.
      - destination_country_mapped_field_name (str): Nombre del campo a añadir en la tabla destino.
      - json_keyfile (str, opcional): Ruta del archivo JSON de credenciales (solo requerido en entornos no GCP).
    
    Retorna:
        str: Una cadena de texto que contiene el script SQL completo (JOIN y DROP) listo para ejecutarse.
    """
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
    
    # --- Validación de parámetros ---
    json_keyfile = config.get("json_keyfile")  # Puede ser None en GCP
    source_table = config.get("source_table")
    source_country_name_best_list = config.get("source_country_name_best_list")
    source_id_name_field = config.get("source_id_name_field")
    country_name_skip_values_list = config.get("country_name_skip_values_list", [])
    manual_mapping_dic = config.get("manual_mapping_dic", {})
    destination_table = config.get("destination_table")
    destination_id_field_name = config.get("destination_id_field_name")
    destination_country_mapped_field_name = config.get("destination_country_mapped_field_name")
    
    if not all(isinstance(x, str) and x for x in [
        source_table, source_id_name_field, destination_table,
        destination_id_field_name, destination_country_mapped_field_name]):
        raise ValueError("Las keys 'source_table', 'source_id_name_field', 'destination_table', "
                         "'destination_id_field_name' y 'destination_country_mapped_field_name' son obligatorias y deben ser cadenas válidas.")
    if not isinstance(source_country_name_best_list, list) or not source_country_name_best_list:
        raise ValueError("'source_country_name_best_list' es requerido y debe ser una lista válida.")
    if not isinstance(country_name_skip_values_list, list):
        raise ValueError("'country_name_skip_values_list' debe ser una lista si se proporciona.")
    
    # --- Autenticación consolidada ---
    is_gcp = bool(os.environ.get("GOOGLE_CLOUD_PROJECT"))
    if is_gcp:
        print("✅ Entorno detectado: GCP (autenticación automática)", flush=True)
        creds, _ = gauth_default()
    else:
        if not json_keyfile:
            raise ValueError("⚠️ En entornos no GCP, se debe proporcionar 'json_keyfile' en config.")
        print("🔄 Entorno detectado: Entorno local/Colab (usando JSON de credenciales)", flush=True)
        creds = service_account.Credentials.from_service_account_file(json_keyfile)
    
    # --- Subfunciones internas ---
    def _normalize_text(texto: str) -> str:
        """ Normaliza el texto: minúsculas, sin acentos y sin caracteres especiales """
        texto = texto.lower().strip()
        texto = unicodedata.normalize('NFD', texto)
        texto = ''.join(c for c in texto if unicodedata.category(c) != 'Mn')
        texto = re.sub(r'[^a-z0-9\s]', '', texto)
        return texto

    def _get_best_country(row) -> str:
        """ Retorna la primera opción no nula en la lista de campos de país """
        for field in source_country_name_best_list:
            if pd.notna(row[field]) and row[field]:
                return row[field]
        return None

    def translate_batch_custom(words, prefix="El país llamado ", separator="|||", max_length=4000):
        """
        Traduce una lista de palabras de español a inglés en pocas peticiones (agrupadas en chunks).
        Se antepone a cada palabra el prefijo y, tras traducir en bloque, se elimina dicho prefijo.
        Retorna un diccionario {palabra_original: traducción_sin prefijo}.
        """
        translator = Translator()
        english_prefix = translator.translate(prefix, src='es', dest='en').text.strip()
        results = {}
        chunk_phrases = []
        chunk_original_words = []
        current_length = 0

        def process_chunk():
            nonlocal results, chunk_phrases, chunk_original_words, current_length
            if not chunk_phrases:
                return
            try:
                translated_objects = translator.translate(chunk_phrases, src='es', dest='en')
                if not isinstance(translated_objects, list):
                    translated_objects = [translated_objects]
                translated_phrases = [obj.text for obj in translated_objects]
            except Exception as e:
                translated_phrases = [translator.translate(phrase, src='es', dest='en').text for phrase in chunk_phrases]
            if len(translated_phrases) != len(chunk_original_words):
                raise ValueError("El número de frases traducidas no coincide con el número de palabras originales en el chunk.")
            prefix_pattern = re.compile(r'^' + re.escape(english_prefix), flags=re.IGNORECASE)
            for orig, phrase in zip(chunk_original_words, translated_phrases):
                phrase = phrase.strip()
                translated_word = prefix_pattern.sub('', phrase).strip()
                results[orig] = translated_word
            chunk_phrases.clear()
            chunk_original_words.clear()
            current_length = 0

        for word in words:
            if not word:
                continue
            phrase = f"{prefix}{word}"
            phrase_length = len(phrase)
            if chunk_phrases and (current_length + len(separator) + phrase_length > max_length):
                process_chunk()
            if not chunk_phrases:
                current_length = phrase_length
            else:
                current_length += len(separator) + phrase_length
            chunk_phrases.append(phrase)
            chunk_original_words.append(word)
        if chunk_phrases:
            process_chunk()
        return results

    def _build_countries_dic():
        """ Construye un diccionario a partir de pycountry """
        countries_dic = {}
        for pais in list(pycountry.countries):
            norm_name = _normalize_text(pais.name)
            countries_dic[norm_name] = pais
            if hasattr(pais, 'official_name'):
                countries_dic[_normalize_text(pais.official_name)] = pais
            if hasattr(pais, 'common_name'):
                countries_dic[_normalize_text(pais.common_name)] = pais
        return countries_dic

    def _build_update_sql(aux_table: str, client: bigquery.Client) -> str:
        """
        Genera el script SQL para actualizar la tabla destino:
          - Realiza el JOIN de la tabla auxiliar con la tabla destino.
          - Si la columna destino ya existe, usa REPLACE; de lo contrario, la agrega.
          - Incluye el SQL para eliminar la tabla auxiliar (DROP TABLE).
        Retorna una cadena con el script completo, en el que cada sentencia finaliza con ';'.
        """
        # Verificar si la columna ya existe en la tabla destino
        try:
            dest_table = client.get_table(destination_table)
            dest_fields = [field.name for field in dest_table.schema]
        except Exception as e:
            dest_fields = []
        
        if destination_country_mapped_field_name in dest_fields:
            # Usar REPLACE si la columna ya existe
            join_sql = (
                f"CREATE OR REPLACE TABLE `{destination_table}` AS\n"
                f"SELECT d.* REPLACE(m.country_name_iso AS `{destination_country_mapped_field_name}`)\n"
                f"FROM `{destination_table}` d\n"
                f"LEFT JOIN `{aux_table}` m\n"
                f"  ON d.{destination_id_field_name} = m.{source_id_name_field};"
            )
        else:
            # Si la columna no existe, simplemente agregarla
            join_sql = (
                f"CREATE OR REPLACE TABLE `{destination_table}` AS\n"
                f"SELECT d.*, m.country_name_iso AS `{destination_country_mapped_field_name}`\n"
                f"FROM `{destination_table}` d\n"
                f"LEFT JOIN `{aux_table}` m\n"
                f"  ON d.{destination_id_field_name} = m.{source_id_name_field};"
            )
        drop_sql = f"DROP TABLE `{aux_table}`;"
        sql_script = join_sql + "\n" + drop_sql
        return sql_script

    # --- Proceso principal ---
    # Determinar el proyecto origen a partir de la tabla origen
    source_project = source_table.split(".")[0]
    print(f"[INFO] Extrayendo datos de {source_table} en el proyecto {source_project}...", flush=True)
    
    # Instanciar el cliente de BigQuery usando el mismo mecanismo de autenticación
    client = bigquery.Client(project=source_project, credentials=creds)
    
    country_fields_sql = ", ".join(source_country_name_best_list)
    query_source = f"""
        SELECT {source_id_name_field}, {country_fields_sql}
        FROM `{source_table}`
    """
    df = client.query(query_source).to_dataframe()
    if df.empty:
        print("[WARNING] No se encontraron datos en la tabla origen.", flush=True)
        return ""
    print("[INFO] Procesando la mejor opción de país...", flush=True)
    df["best_country_name"] = df.apply(_get_best_country, axis=1)
    unique_countries = df["best_country_name"].dropna().unique().tolist()
    print(f"[INFO] Se encontraron {len(unique_countries)} países únicos para mapear.", flush=True)
    
    # Preparar el conjunto de países a omitir (skip)
    skip_set = set(_normalize_text(x) for x in country_name_skip_values_list if isinstance(x, str))
    
    mapping_results = {}
    countries_to_translate = []
    for country in unique_countries:
        if not isinstance(country, str):
            mapping_results[country] = None
            continue
        if _normalize_text(country) in skip_set:
            print(f"[INFO] Saltando mapeo para '{country}' (en lista de omisión).", flush=True)
            mapping_results[country] = country
        else:
            countries_to_translate.append(country)
    
    print(f"[INFO] Traduciendo {len(countries_to_translate)} países en lote...", flush=True)
    translated_dict = translate_batch_custom(countries_to_translate, prefix="El país llamado ", separator="|||", max_length=4000)
    
    countries_dic = _build_countries_dic()
    country_iso_keys = list(countries_dic.keys())
    
    # Realizar el mapeo: usar manual_mapping_dic si corresponde; si no, utilizar fuzzy matching
    for country in countries_to_translate:
        translated_text = translated_dict.get(country, country)
        normalized_translated = _normalize_text(translated_text)
        override_found = False
        for canonical, variants in manual_mapping_dic.items():
            for variant in variants:
                if _normalize_text(variant) == normalized_translated:
                    mapping_results[country] = canonical
                    override_found = True
                    print(f"[MANUAL] '{country}' mapeado manualmente a: {canonical}", flush=True)
                    break
            if override_found:
                break
        if override_found:
            continue
        best_match = process.extractOne(normalized_translated, country_iso_keys, scorer=fuzz.ratio)
        if best_match:
            match_key, score, _ = best_match
            country_obj = countries_dic[match_key]
            # Usar common_name si existe, de lo contrario name
            if hasattr(country_obj, 'common_name'):
                mapping_results[country] = country_obj.common_name
            else:
                mapping_results[country] = country_obj.name
            print(f"[SUCCESS] '{country}' mapeado a: {mapping_results[country]} (Score: {score})", flush=True)
        else:
            print(f"[ERROR] No se encontró un mapeo válido para '{country}'", flush=True)
            mapping_results[country] = None

    df["country_name_iso"] = df["best_country_name"].map(mapping_results)
    mapping_df = df[[source_id_name_field, "country_name_iso"]].drop_duplicates()
    
    dest_parts = destination_table.split(".")
    if len(dest_parts) != 3:
        raise ValueError("El formato de 'destination_table' debe ser 'proyecto.dataset.tabla'.")
    dest_project, dest_dataset, dest_table_name = dest_parts
    aux_table = f"{dest_project}.{dest_dataset}.temp_country_mapping"
    
    print(f"[INFO] Subiendo tabla auxiliar {aux_table} con datos de mapeo...", flush=True)
    pandas_gbq.to_gbq(mapping_df,
                        destination_table=aux_table,
                        project_id=dest_project,
                        if_exists="replace",
                        credentials=creds)
    
    sql_script = _build_update_sql(aux_table, client)
    print("[INFO] SQL generado para actualizar la tabla destino.", flush=True)
    print(sql_script, flush=True)
    
    return sql_script







#__________________________________________________________________________________________________________________________________________________________
# @title GSheet_to_df()
#__________________________________________________________________________________________________________________________________________________________
def GSheet_to_df(params: dict) -> pd.DataFrame:
    """
    Extrae datos desde una hoja de cálculo de Google Sheets y los convierte en un DataFrame.
    Funciona tanto en Colab (requiere credenciales JSON) como en entornos GCP (autenticación automática).

    Args:
        params (dict):
            - spreadsheet_id (str): URL o ID de la hoja de cálculo en Google Sheets.
            - worksheet_name (str): Nombre de la hoja específica dentro del documento.
            - json_keyfile (str, opcional en GCP): Ruta del archivo JSON de credenciales (solo necesario en Colab).

    Returns:
        pd.DataFrame: DataFrame con los datos extraídos.

    Raises:
        ValueError: Si faltan parámetros.
        FileNotFoundError: Si el JSON no existe (en Colab).
        gspread.exceptions.SpreadsheetNotFound: Si la hoja de cálculo no es accesible.
    """
    import os
    import pandas as pd
    import gspread
    from google.auth.exceptions import DefaultCredentialsError
    from google.auth import default
    from oauth2client.service_account import ServiceAccountCredentials

    # Validación de parámetros
    spreadsheet_id_str = params.get("spreadsheet_id")
    worksheet_name_str = params.get("worksheet_name")
    json_keyfile_str = params.get("json_keyfile")  # Puede ser None en GCP

    if not spreadsheet_id_str or not worksheet_name_str:
        raise ValueError("Faltan parámetros obligatorios: 'spreadsheet_id' o 'worksheet_name'.")

    try:
        # Detectar si estamos en un entorno GCP
        is_gcp = bool(os.environ.get("GOOGLE_CLOUD_PROJECT"))

        if is_gcp:
            print("✅ Entorno detectado: GCP (autenticación automática)", flush=True)
            creds, _ = default()  # Autenticación automática dentro de GCP
        else:
            if not json_keyfile_str:
                raise ValueError("⚠️ En Colab, debes proporcionar 'json_keyfile'.")
            
            print("🔄 Entorno detectado: Colab (usando JSON de credenciales)", flush=True)
            scope_list = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_str, scope_list)

        # Autenticación con gspread
        client = gspread.authorize(creds)

        # Acceder a la hoja de cálculo
        spreadsheet = client.open_by_url(spreadsheet_id_str)
        worksheet = spreadsheet.worksheet(worksheet_name_str)

        # Extraer datos y convertir a DataFrame
        data_list = worksheet.get_all_records()
        df = pd.DataFrame(data_list)

        print(f"✅ Datos extraídos con éxito de '{worksheet_name_str}'.", flush=True)
        return df

    except FileNotFoundError:
        raise FileNotFoundError(f"⚠️ Archivo JSON no encontrado: {json_keyfile_str}")
    except DefaultCredentialsError:
        raise ValueError("⚠️ Error en la autenticación. Verifica credenciales.")
    except gspread.exceptions.SpreadsheetNotFound:
        raise ValueError(f"⚠️ No se encontró la hoja de cálculo con ID/URL: {spreadsheet_id_str}")

#__________________________________________________________________________________________________________________________________________________________
# @title fields_name_format()
#__________________________________________________________________________________________________________________________________________________________
def fields_name_format(config):
    """
    Formatea nombres de campos de datos según configuraciones específicas.

    Args:
        config (dict): Diccionario de configuración con los siguientes parámetros:
            - fields_name_raw_list (list): Lista de nombres de campos a formatear.
            - formato_final (str, opcional): Formato final deseado para los nombres
              ('CamelCase', 'snake_case', 'Sentence case', None o False para no formatear).
            - reemplazos (dict, opcional): Diccionario de términos a reemplazar.
            - siglas (list, opcional): Lista de siglas que deben mantenerse en mayúsculas.

    Returns:
        pandas.DataFrame: DataFrame con los campos originales y formateados.
    """
    import re
    import pandas as pd
    
    fields_name_raw_list = config.get('fields_name_raw_list', [])
    formato_final = config.get('formato_final', 'CamelCase')
    reemplazos = config.get('reemplazos', {})
    siglas = [sigla.upper() for sigla in config.get('siglas', [])]

    if not isinstance(fields_name_raw_list, list) or not fields_name_raw_list:
        raise ValueError("El parámetro 'fields_name_raw_list' debe ser una lista no vacía.")

    def aplicar_reemplazos(field, reemplazos):
        """Aplica reemplazos definidos en el diccionario."""
        for key, value in sorted(reemplazos.items(), key=lambda x: -len(x[0])):  # Orden por longitud descendente
            if key in field:
                field = field.replace(key, value)
        return field

    def formatear_campo(field, formato, siglas):
        """Formatea el campo según el formato especificado.
           Si formato es None o False, no se hace formateo alguno."""
        if formato is None or formato is False:
            # Sin formateo, se devuelve tal cual.
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
    for field in fields_name_raw_list:
        original_field = field
        # Procesar campos
        field = aplicar_reemplazos(field, reemplazos)
        formatted_field = formatear_campo(field, formato_final, siglas)

        resultado.append({'Campo Original': original_field, 'Campo Formateado': formatted_field})

    return pd.DataFrame(resultado)


#__________________________________________________________________________________________________________________________________________________________
# @title SQL_generate_cleaning_str()
#__________________________________________________________________________________________________________________________________________________________
def SQL_generate_cleaning_str(params: dict) -> str:
    """
    Genera una sentencia SQL para crear o sobrescribir una tabla de 'staging'
    aplicando mapeos de columnas, filtros y prefijo opcional a los campos destino.

    Args:
        params (dict):
            - table_source (str): Nombre de la tabla fuente (proyecto.dataset.tabla).
            - table_destination (str): Nombre de la tabla destino (proyecto.dataset.tabla).
            - fields_mapped_use (bool): Si True, usa la columna 'Campo Formateado' como nombre en el SELECT.
            - fields_mapped_df (pd.DataFrame): DataFrame con columnas ["Campo Original", "Campo Formateado"].
            - fields_destination_prefix (str, opcional): Prefijo a aplicar a los campos mapeados.
            - exclude_records_by_creation_date_bool (bool): Si True, filtra por rango de fechas.
            - exclude_records_by_creation_date_field (str, opcional): Campo con fecha de creación.
            - exclude_records_by_creation_date_range (dict, opcional): { "from": "YYYY-MM-DD", "to": "YYYY-MM-DD" }.
            - exclude_records_by_keywords_bool (bool): Si True, aplica exclusión usando palabras clave.
            - exclude_records_by_keywords_fields (list, opcional): Campos a los que se aplicará exclusión.
            - exclude_records_by_keywords_words (list, opcional): Lista de términos a excluir.
            - fields_to_trim (list, opcional): Columnas a las que se aplicará TRIM.

    Returns:
        str: Sentencia SQL generada.

    Raises:
        ValueError: Si faltan parámetros requeridos o son inválidos.
    """
    import pandas as pd

    # Validación de parámetros obligatorios
    table_source = params.get("table_source")
    table_destination = params.get("table_destination")
    fields_mapped_df = params.get("fields_mapped_df")
    fields_mapped_use = params.get("fields_mapped_use", True)
    fields_destination_prefix = params.get("fields_destination_prefix", "")

    if not table_source or not table_destination:
        raise ValueError("Los parámetros 'table_source' y 'table_destination' son obligatorios.")
    if not isinstance(fields_mapped_df, pd.DataFrame):
        raise ValueError("'fields_mapped_df' debe ser un DataFrame válido.")

    # Parámetros opcionales con valores predeterminados
    exclude_records_by_creation_date_bool = params.get("exclude_records_by_creation_date_bool", False)
    exclude_records_by_creation_date_field = params.get("exclude_records_by_creation_date_field", "")
    exclude_records_by_creation_date_range = params.get("exclude_records_by_creation_date_range", {})

    exclude_records_by_keywords_bool = params.get("exclude_records_by_keywords_bool", False)
    exclude_records_by_keywords_fields = params.get("exclude_records_by_keywords_fields", [])
    exclude_records_by_keywords_words = params.get("exclude_records_by_keywords_words", [])
    fields_to_trim = params.get("fields_to_trim", [])

    # Generar la lista de campos para el SELECT con prefijo aplicado
    select_clauses = []
    for _, row in fields_mapped_df.iterrows():
        campo_origen = row['Campo Original']
        
        # Si fields_mapped_use es True, usar el nombre formateado, si no, el original
        campo_destino = f"{fields_destination_prefix}{row['Campo Formateado']}" if fields_mapped_use else f"{fields_destination_prefix}{campo_origen}"

        if campo_origen in fields_to_trim:
            # Si el campo está en fields_to_trim, aplica TRIM y REPLACE
            select_clause = f"TRIM(REPLACE(`{campo_origen}`, '  ', ' ')) AS `{campo_destino}`"
        else:
            # Campo formateado con prefijo
            select_clause = f"`{campo_origen}` AS `{campo_destino}`"

        select_clauses.append(select_clause)

    select_part = ",\n  ".join(select_clauses)

    # Construir la cláusula WHERE en base a filtros
    where_filters = []

    # Filtro de rango de fechas
    if exclude_records_by_creation_date_bool and exclude_records_by_creation_date_field:
        date_from = exclude_records_by_creation_date_range.get("from", "")
        date_to = exclude_records_by_creation_date_range.get("to", "")
        if date_from:
            where_filters.append(f"`{exclude_records_by_creation_date_field}` >= '{date_from}'")
        if date_to:
            where_filters.append(f"`{exclude_records_by_creation_date_field}` <= '{date_to}'")

    # Filtro de exclusión de términos
    if exclude_records_by_keywords_bool and exclude_records_by_keywords_fields and exclude_records_by_keywords_words:
        for field in exclude_records_by_keywords_fields:
            where_filters.extend([f"`{field}` NOT LIKE '%{word}%'" for word in exclude_records_by_keywords_words])

    # Construcción de la cláusula WHERE
    where_clause = " AND ".join(where_filters) if where_filters else "TRUE"

    # Construcción final del SQL
    SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
SELECT
  {select_part}
FROM `{table_source}`
WHERE {where_clause}
;
""".strip()

    return SQL_script


#__________________________________________________________________________________________________________________________________________________________
# @title SQL_generation_normalize_strings()
#__________________________________________________________________________________________________________________________________________________________

def SQL_generation_normalize_strings(config: dict) -> tuple:
    """
    Función que normaliza los valores de una columna en una tabla de BigQuery utilizando dos fuentes
    de referencia:
    
      1. Mapeo manual: basado en un DataFrame de referencia (referece_table_for_normalization_manual_df)
         que debe tener las columnas 'Bruto' y 'Normalizado'. Se asume que 'Bruto' es el campo de
         entrada para la normalización y 'Normalizado' es el valor de salida deseado.
      
      2. Fuzzy matching (rapidfuzz): aplicado para aquellos valores que no se encuentren en el mapeo manual.
         Se utiliza otro DataFrame de referencia (referece_table_for_normalization_rapidfuzz_df) y se toma
         el valor de la columna indicada por 'referece_table_for_normalization_rapidfuzz_field_name' (ej. "PF DEF")
         como candidato.
    
    Además, la función permite definir una nueva key:
      - destination_field_name: Si se especifica (no está en blanco), se creará una nueva columna con ese nombre 
        para almacenar los valores mapeados. Si está en blanco, se sobrescribirá la columna original 
        (source_table_to_normalize_field_name).

    El proceso es el siguiente:
      - Se extraen los valores únicos de la columna especificada en la tabla fuente.
      - Para cada valor:
          a) Se intenta obtener la normalización mediante mapeo manual.
          b) Si no se encuentra, se aplica fuzzy matching contra los candidatos del DataFrame rapidfuzz.
             Se utiliza el umbral y la opción de filtro según:
                 * rapidfuzz_score_filter_use
                 * rapidfuzz_score_filter_min_value
                 * rapidfuzz_score_filter_no_pass_mapping
          c) Se registra el resultado del fuzzy matching en un DataFrame con las columnas:
             [<source_field>, "Nombre programa formativo", "Rapidfuzz score"].
      - Se crea un DataFrame de mapeo (raw_value, normalized_value) que se sube a una tabla auxiliar.
      - Se genera un script SQL que actualiza la tabla fuente mediante un JOIN con la tabla auxiliar.
    
    Parámetros en config:
      - source_table_to_normalize (str): Tabla fuente en formato `proyecto.dataset.tabla`.
      - source_table_to_normalize_field_name (str): Nombre de la columna en la tabla fuente a normalizar.
      - referece_table_for_normalization_manual_df (pd.DataFrame): DataFrame para mapeo manual con columnas 'Bruto' y 'Normalizado'.
      - referece_table_for_normalization_manual_field_name (str): (No se utiliza, se asume que es 'Bruto').
      - referece_table_for_normalization_rapidfuzz_df (pd.DataFrame): DataFrame para fuzzy matching.
      - referece_table_for_normalization_rapidfuzz_field_name (str): Nombre del campo en el DataFrame rapidfuzz que contiene los candidatos (ej. "PF DEF").
      - rapidfuzz_score_filter_use (bool): Si True, se aplica filtro de score en el fuzzy matching.
      - rapidfuzz_score_filter_min_value (int o float): Score mínimo aceptable en el fuzzy matching.
      - rapidfuzz_score_filter_no_pass_mapping (str): Valor a asignar si el score no supera el umbral.
      - json_keyfile (str, opcional): Ruta del archivo JSON de credenciales (requerido en entornos no GCP).
      - destination_field_name (str, opcional): Si se especifica y no está vacío, se crea una nueva columna 
            con ese nombre para almacenar el resultado mapeado. Si está vacío, se sobrescribe la columna 
            original (source_table_to_normalize_field_name).
    
    Retorna:
      tuple: (sql_script, df_fuzzy_results)
         - sql_script (str): Script SQL completo para actualizar la tabla fuente (incluye JOIN y DROP).
         - df_fuzzy_results (pd.DataFrame): DataFrame con los resultados del fuzzy matching, con columnas:
               [<source_field>, "Nombre programa formativo", "Rapidfuzz score"].
    """
    from google.cloud import bigquery
    import pandas as pd
    import pandas_gbq
    import re
    import unicodedata
    from rapidfuzz import process, fuzz
    import time
    import os
    from google.auth import default as gauth_default
    from google.oauth2 import service_account
    
    # --- Validación de parámetros ---
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
        raise ValueError("La key 'source_table_to_normalize' es obligatoria y debe ser una cadena válida.")
    if not (isinstance(source_field, str) and source_field):
        raise ValueError("La key 'source_table_to_normalize_field_name' es obligatoria y debe ser una cadena válida.")
    if not isinstance(manual_df, pd.DataFrame) or manual_df.empty:
        raise ValueError("La key 'referece_table_for_normalization_manual_df' debe ser un DataFrame válido y no vacío.")
    if not isinstance(rapidfuzz_df, pd.DataFrame) or rapidfuzz_df.empty:
        raise ValueError("La key 'referece_table_for_normalization_rapidfuzz_df' debe ser un DataFrame válido y no vacío.")
    if not (isinstance(rapidfuzz_field, str) and rapidfuzz_field):
        raise ValueError("La key 'referece_table_for_normalization_rapidfuzz_field_name' es obligatoria y debe ser una cadena válida.")
    
    # --- Subfunción: Normalización de texto ---
    def _normalize_text(texto: str) -> str:
        """
        Normaliza el texto: lo convierte a minúsculas, elimina acentos y caracteres especiales.
        """
        texto = texto.lower().strip()
        texto = unicodedata.normalize('NFD', texto)
        texto = ''.join(c for c in texto if unicodedata.category(c) != 'Mn')
        texto = re.sub(r'[^a-z0-9\s]', '', texto)
        return texto
    
    # --- Subfunción: Extraer valores únicos de la tabla fuente ---
    def _extract_source_values(source_table: str, source_field: str, client: bigquery.Client) -> pd.DataFrame:
        """
        Extrae los valores únicos de la columna 'source_field' desde la tabla 'source_table'.
        Retorna un DataFrame con una columna 'raw_value'.
        """
        query = f"""
            SELECT DISTINCT `{source_field}` AS raw_value
            FROM `{source_table}`
        """
        df = client.query(query).to_dataframe()
        return df
    
    # --- Subfunción: Construir mapeo manual ---
    def _build_manual_mapping(manual_df: pd.DataFrame) -> dict:
        """
        Construye un diccionario de mapeo manual a partir del DataFrame 'manual_df' con columnas 'Bruto' y 'Normalizado'.
        Retorna un diccionario: {texto_normalizado_de_Bruto: Normalizado}.
        """
        mapping = {}
        for _, row in manual_df.iterrows():
            bruto = row["Bruto"]
            normalizado = row["Normalizado"]
            if isinstance(bruto, str):
                mapping[_normalize_text(bruto)] = normalizado
        return mapping
    
    # --- Subfunción: Construir candidatos para fuzzy matching ---
    def _build_rapidfuzz_candidates(rapidfuzz_df: pd.DataFrame, rapidfuzz_field: str) -> dict:
        """
        Construye un diccionario de candidatos para fuzzy matching a partir del DataFrame 'rapidfuzz_df'.
        Se utiliza la columna indicada por 'rapidfuzz_field'. Retorna un diccionario:
            {texto_normalizado_del_candidato: candidato_original}.
        """
        candidates = {}
        for _, row in rapidfuzz_df.iterrows():
            candidate = row[rapidfuzz_field]
            if isinstance(candidate, str):
                candidates[_normalize_text(candidate)] = candidate
        return candidates
    
    # --- Subfunción: Aplicar mapeo manual y fuzzy matching a los valores fuente ---
    def _apply_mapping(df_source: pd.DataFrame, manual_mapping: dict, rapidfuzz_candidates: dict) -> tuple:
        """
        Para cada valor en 'df_source', intenta mapearlo utilizando el mapeo manual.
        Si no se encuentra, aplica fuzzy matching contra los candidatos de 'rapidfuzz_candidates'.
        Retorna:
            - mapping_results: dict con {valor_original: valor_normalizado} para actualización.
            - fuzzy_results_list: lista de diccionarios con resultados del fuzzy matching, con claves:
                  [source_field, "Nombre programa formativo", "Rapidfuzz score"].
        """
        mapping_results = {}
        fuzzy_results_list = []
        candidate_keys = list(rapidfuzz_candidates.keys())
        
        for raw in df_source["raw_value"]:
            if not isinstance(raw, str) or not raw:
                mapping_results[raw] = raw
                continue
            
            normalized_raw = _normalize_text(raw)
            # Intentar mapeo manual exacto
            if normalized_raw in manual_mapping:
                mapping_results[raw] = manual_mapping[normalized_raw]
                print(f"[MANUAL] '{raw}' mapeado a: {manual_mapping[normalized_raw]}", flush=True)
            else:
                # Aplicar fuzzy matching
                best_match = process.extractOne(normalized_raw, candidate_keys, scorer=fuzz.ratio)
                if best_match:
                    match_key, score, _ = best_match
                    if rapidfuzz_filter_use and score < rapidfuzz_min_score:
                        mapping_results[raw] = rapidfuzz_no_pass_value
                        fuzzy_results_list.append({
                            source_field: raw,
                            "Nombre programa formativo": rapidfuzz_no_pass_value,
                            "Rapidfuzz score": score
                        })
                        print(f"[FUZY] '{raw}' obtuvo score {score} (< {rapidfuzz_min_score}). Se asigna: {rapidfuzz_no_pass_value}", flush=True)
                    else:
                        mapping_results[raw] = rapidfuzz_candidates[match_key]
                        fuzzy_results_list.append({
                            source_field: raw,
                            "Nombre programa formativo": rapidfuzz_candidates[match_key],
                            "Rapidfuzz score": score
                        })
                        print(f"[FUZY] '{raw}' mapeado a: {rapidfuzz_candidates[match_key]} (Score: {score})", flush=True)
                else:
                    mapping_results[raw] = rapidfuzz_no_pass_value
                    fuzzy_results_list.append({
                        source_field: raw,
                        "Nombre programa formativo": rapidfuzz_no_pass_value,
                        "Rapidfuzz score": None
                    })
                    print(f"[ERROR] No se encontró mapeo para '{raw}'. Se asigna: {rapidfuzz_no_pass_value}", flush=True)
        
        return mapping_results, fuzzy_results_list

    # --- Proceso principal ---
    print(f"[INFO] Iniciando normalización de valores para la columna `{source_field}` de la tabla {source_table}...", flush=True)

    # --- Autenticación consolidada ---
    print("[INFO] Autenticando...", flush=True)
    is_gcp = bool(os.environ.get("GOOGLE_CLOUD_PROJECT"))
    if is_gcp:
        print("[INFO] Entorno GCP detectado. Usando autenticación automática.", flush=True)
        creds, _ = gauth_default()
    else:
        json_keyfile = config.get("json_keyfile")
        if not json_keyfile:
            raise ValueError("Se debe proporcionar 'json_keyfile' en entornos no GCP.")
        print("[INFO] Entorno local/Colab detectado. Usando autenticación con JSON de credenciales.", flush=True)
        creds = service_account.Credentials.from_service_account_file(json_keyfile)
    
    source_project = source_table.split(".")[0]
    client = bigquery.Client(project=source_project, credentials=creds)
    
    print(f"[INFO] Extrayendo valores únicos de la columna `{source_field}` desde {source_table}...", flush=True)
    df_source = _extract_source_values(source_table, source_field, client)
    if df_source.empty:
        print("[WARNING] No se encontraron valores en la columna fuente.", flush=True)
        return ("", pd.DataFrame())
    print(f"[INFO] Se encontraron {len(df_source)} valores únicos para normalizar.", flush=True)
    
    manual_mapping = _build_manual_mapping(manual_df)
    rapidfuzz_candidates = _build_rapidfuzz_candidates(rapidfuzz_df, rapidfuzz_field)
    
    mapping_results, fuzzy_results_list = _apply_mapping(df_source, manual_mapping, rapidfuzz_candidates)
    
    mapping_df = pd.DataFrame(list(mapping_results.items()), columns=["raw_value", "normalized_value"])
    df_fuzzy_results = pd.DataFrame(fuzzy_results_list)
    
    # --- Subir tabla auxiliar con el mapeo ---
    parts = source_table.split(".")
    if len(parts) != 3:
        raise ValueError("El formato de 'source_table_to_normalize' debe ser 'proyecto.dataset.tabla'.")
    dest_project, dest_dataset, _ = parts
    aux_table = f"{dest_project}.{dest_dataset}.temp_normalized_strings"
    
    print(f"[INFO] Subiendo tabla auxiliar {aux_table} con el mapeo de normalización...", flush=True)
    pandas_gbq.to_gbq(mapping_df,
                        destination_table=aux_table,
                        project_id=dest_project,
                        if_exists="replace",
                        credentials=creds)
    
    # --- Generar script SQL para actualizar la tabla fuente ---
    # Si destination_field_name está definido (no es vacío), se crea una nueva columna, de lo contrario se sobrescribe
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
    
    print("[INFO] SQL generado para actualizar la tabla con valores normalizados.", flush=True)
    print(sql_script, flush=True)
    
    return sql_script, df_fuzzy_results

#__________________________________________________________________________________________________________________________________________________________
# @title SQL_generate_new_columns_from_mapping()
#__________________________________________________________________________________________________________________________________________________________

def SQL_generate_new_columns_from_mapping(config: dict) -> tuple:
    """
    Función que agrega nuevas columnas a una tabla de BigQuery a partir del mapeo entre los valores de 
    una columna de la tabla source y los valores de una columna de una tabla de referencia (proporcionada 
    en un DataFrame). Se incorporan en la tabla source las columnas adicionales indicadas en 
    'referece_table_for_new_values_field_names_dic', pero se sanitizan para que sean válidas en GBQ.

    Proceso:
      1. Extrae los valores únicos de la columna 'source_table_to_add_fields_reference_field_name'
         de la tabla source ('source_table_to_add_fields').
      2. Construye un mapeo a partir del DataFrame de referencia ('referece_table_for_new_values_df'):
           - Se utiliza el primer elemento de 'referece_table_for_new_values_field_names_dic' como clave de match.
           - Se asigna para cada registro un diccionario con los valores de cada campo (para todos los keys de la
             referencia).
      3. Para cada valor extraído de la tabla source, se normaliza y se busca en el mapeo:
           - Si se encuentra, se asignan los valores correspondientes a cada columna.
           - Si no se encuentra, se asigna el valor definido en 'values_non_matched_result' para cada columna.
      4. Se construye un DataFrame con el mapeo resultante, que incluye la columna de match (raw_value) 
         y las columnas adicionales con nombres sanitizados.
      5. Se sube este DataFrame a una tabla auxiliar en BigQuery.
      6. Se genera un script SQL que actualiza la tabla source agregando las nuevas columnas mediante un JOIN 
         con la tabla auxiliar. Solo se incluyen en el JOIN aquellas columnas cuyo valor en la 
         'referece_table_for_new_values_field_names_dic' es True.
         Se incluye la sentencia para eliminar la tabla auxiliar.

    Parámetros en config:
      - source_table_to_add_fields (str): Tabla source en formato `proyecto.dataset.tabla`.
      - source_table_to_add_fields_reference_field_name (str): Nombre de la columna en la tabla source utilizada para el match.
      - referece_table_for_new_values_df (pd.DataFrame): DataFrame de referencia que contiene los valores a mapear.
      - referece_table_for_new_values_field_names_dic (dict): Diccionario donde las claves son los nombres de campo de la 
            tabla de referencia y los valores (bool) indican si ese campo se incorpora en el JOIN.
            El primer key se usa para hacer match con la columna de la tabla source.
      - values_non_matched_result (str): Valor a asignar para aquellos registros que no encuentren match.
      - json_keyfile (str, opcional): Ruta del archivo JSON de credenciales (solo requerido en entornos no GCP).

    Retorna:
      tuple: (sql_script, mapping_df)
         - sql_script (str): Script SQL completo para actualizar la tabla source (incluye JOIN y DROP).
         - mapping_df (pd.DataFrame): DataFrame con el mapeo resultante, con columnas:
               ["raw_value"] + [nombres_sanitizados_de_los_keys_de_referece_table_for_new_values_field_names_dic].
    """
    from google.cloud import bigquery
    import pandas as pd
    import pandas_gbq
    import os
    import unicodedata
    import re
    from google.auth import default as gauth_default
    from google.oauth2 import service_account

    # --- Validación de parámetros obligatorios ---
    source_table = config.get("source_table_to_add_fields")
    source_field = config.get("source_table_to_add_fields_reference_field_name")
    ref_df = config.get("referece_table_for_new_values_df")
    ref_field_names_dic = config.get("referece_table_for_new_values_field_names_dic")
    non_matched_value = config.get("values_non_matched_result", "descartado")
    
    if not (isinstance(source_table, str) and source_table):
        raise ValueError("La key 'source_table_to_add_fields' es obligatoria y debe ser una cadena válida.")
    if not (isinstance(source_field, str) and source_field):
        raise ValueError("La key 'source_table_to_add_fields_reference_field_name' es obligatoria y debe ser una cadena válida.")
    if not isinstance(ref_df, pd.DataFrame) or ref_df.empty:
        raise ValueError("La key 'referece_table_for_new_values_df' debe ser un DataFrame válido y no vacío.")
    if not isinstance(ref_field_names_dic, dict) or not ref_field_names_dic:
        raise ValueError("La key 'referece_table_for_new_values_field_names_dic' debe ser un diccionario no vacío.")
    
    print("[INFO] Parámetros obligatorios validados.", flush=True)
    
    # --- Autenticación consolidada ---
    is_gcp = bool(os.environ.get("GOOGLE_CLOUD_PROJECT"))
    if is_gcp:
        print("[INFO] Entorno detectado: GCP (autenticación automática)", flush=True)
        creds, _ = gauth_default()
    else:
        json_keyfile = config.get("json_keyfile")
        if not json_keyfile:
            raise ValueError("En entornos no GCP, se debe proporcionar 'json_keyfile' en config para la autenticación.")
        print("[INFO] Entorno detectado: Local/Colab (usando JSON de credenciales)", flush=True)
        creds = service_account.Credentials.from_service_account_file(json_keyfile)
    
    # --- Subfunción: Normalizar texto ---
    def _normalize_text(texto: str) -> str:
        """
        Normaliza el texto: lo convierte a minúsculas, elimina acentos y caracteres especiales.
        """
        texto = texto.lower().strip()
        texto = unicodedata.normalize('NFD', texto)
        texto = ''.join(c for c in texto if unicodedata.category(c) != 'Mn')
        texto = re.sub(r'[^a-z0-9\s]', '', texto)
        return texto

    # --- Subfunción: Sanitizar nombres de columnas para GBQ ---
    def _sanitize_field_name(name: str) -> str:
        """
        Convierte un nombre de campo a un formato válido para BigQuery:
          - Convierte a minúsculas.
          - Elimina acentos.
          - Reemplaza espacios por guiones bajos.
          - Elimina caracteres no alfanuméricos (excepto guiones bajos).
          - Si empieza con dígito, antepone un guion bajo.
        """
        name = name.lower().strip()
        name = unicodedata.normalize('NFD', name)
        name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
        name = re.sub(r'\s+', '_', name)
        name = re.sub(r'[^a-z0-9_]', '', name)
        if re.match(r'^\d', name):
            name = '_' + name
        return name
    
    # Crear diccionario de nombres sanitizados para cada key en el diccionario de referencia
    ref_field_names_list = list(ref_field_names_dic.keys())
    sanitized_columns = {col: _sanitize_field_name(col) for col in ref_field_names_list}
    
    # --- Subfunción: Extraer valores únicos de la tabla source ---
    def _extract_source_values(source_table: str, source_field: str, client: bigquery.Client) -> pd.DataFrame:
        """
        Extrae los valores únicos de la columna 'source_field' de la tabla 'source_table'.
        Retorna un DataFrame con una columna 'raw_value'.
        """
        query = f"""
            SELECT DISTINCT `{source_field}` AS raw_value
            FROM `{source_table}`
        """
        df = client.query(query).to_dataframe()
        return df
    
    # --- Subfunción: Construir mapeo de referencia ---
    def _build_reference_mapping(ref_df: pd.DataFrame, ref_field_names_list: list) -> dict:
        """
        Construye un diccionario de mapeo a partir del DataFrame de referencia.
        Se utiliza el primer key de 'ref_field_names_list' como clave de match.
        Retorna un diccionario:
           { texto_normalizado_del_campo_match: { col: valor, ... } }
        """
        mapping = {}
        match_field = ref_field_names_list[0]
        for _, row in ref_df.iterrows():
            key_val = row.get(match_field)
            if isinstance(key_val, str):
                norm_key = _normalize_text(key_val)
                mapping[norm_key] = { col: row.get(col, non_matched_value) for col in ref_field_names_list }
        return mapping
    
    # --- Subfunción: Aplicar mapeo a los valores fuente ---
    def _apply_mapping(df_source: pd.DataFrame, reference_mapping: dict) -> dict:
        """
        Para cada valor en 'df_source', se busca una correspondencia exacta (tras normalización)
        en el diccionario 'reference_mapping'. Si se encuentra, se asignan los valores correspondientes.
        Si no se encuentra, se asigna un diccionario con cada campo igual a non_matched_value.
        Retorna un diccionario { raw_value: mapping_dict }.
        """
        mapping_results = {}
        for raw in df_source["raw_value"]:
            if not isinstance(raw, str) or not raw:
                mapping_results[raw] = { col: non_matched_value for col in ref_field_names_list }
                continue
            norm_raw = _normalize_text(raw)
            if norm_raw in reference_mapping:
                mapping_results[raw] = reference_mapping[norm_raw]
                print(f"[MATCH] '{raw}' encontrado en referencia.", flush=True)
            else:
                mapping_results[raw] = { col: non_matched_value for col in ref_field_names_list }
                print(f"[NO MATCH] '{raw}' no encontrado. Se asigna: {non_matched_value}", flush=True)
        return mapping_results
    
    # --- Proceso principal ---
    print(f"[INFO] Extrayendo valores únicos de la columna `{source_field}` desde {source_table}...", flush=True)
    source_project = source_table.split(".")[0]
    client = bigquery.Client(project=source_project, credentials=creds)
    df_source = _extract_source_values(source_table, source_field, client)
    if df_source.empty:
        print("[WARNING] No se encontraron valores en la tabla source.", flush=True)
        return ("", pd.DataFrame())
    print(f"[INFO] Se encontraron {len(df_source)} valores únicos en la tabla source.", flush=True)
    
    reference_mapping = _build_reference_mapping(ref_df, ref_field_names_list)
    mapping_results = _apply_mapping(df_source, reference_mapping)
    
    # Construir DataFrame de mapeo: columnas: "raw_value" + (nombres sanitizados de todos los keys)
    mapping_rows = []
    for raw, mapping_dict in mapping_results.items():
        row = {"raw_value": raw}
        for col, value in mapping_dict.items():
            sanitized_col = sanitized_columns.get(col, col)
            row[sanitized_col] = value
        mapping_rows.append(row)
    mapping_df = pd.DataFrame(mapping_rows)
    
    # --- Subir tabla auxiliar con el mapeo ---
    parts = source_table.split(".")
    if len(parts) != 3:
        raise ValueError("El formato de 'source_table_to_add_fields' debe ser 'proyecto.dataset.tabla'.")
    dest_project, dest_dataset, _ = parts
    aux_table = f"{dest_project}.{dest_dataset}.temp_new_columns_mapping"
    
    print(f"[INFO] Subiendo tabla auxiliar {aux_table} con el mapeo de nuevos valores...", flush=True)
    pandas_gbq.to_gbq(mapping_df, destination_table=aux_table, project_id=dest_project, if_exists="replace", credentials=creds)
    
    # --- Generar script SQL para actualizar la tabla source con nuevas columnas ---
    # Solo se incluyen en el JOIN aquellas columnas cuyo valor en el diccionario es True.
    join_fields = [ col for col in ref_field_names_list if ref_field_names_dic.get(col, False) ]
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
    
    print("[INFO] SQL generado para actualizar la tabla con nuevas columnas.", flush=True)
    print(sql_script, flush=True)
    
    return sql_script, mapping_df






    
#__________________________________________________________________________________________________________________________________________________________
# @title SQL_generate_academic_date_str()
#__________________________________________________________________________________________________________________________________________________________

def SQL_generate_academic_date_str(params):
    """
    Crea o reemplaza una tabla donde se agregan campos de fecha "académica" o "fiscal"
    basándose en reglas de corte (start_month, start_day) sobre un campo fecha existente.

    Parámetros esperados (dict):
      - table_source (str): Tabla de origen (puede ser la misma 'staging' previa).
      - table_destination (str): Tabla destino que se creará o reemplazará.
      - custom_fields_config (dict): Estructura:
          {
            "nombre_de_campo_fecha": [
                {"start_month": 9, "start_day": 1, "suffix": "fiscal"},
                {"start_month": 10, "start_day": 1, "suffix": "academico"}
            ]
          }
      - json_keyfile (str, opcional): Ruta del archivo JSON de credenciales (solo requerido en entornos no GCP).

    Retorna:
      (str): Sentencia SQL para crear o reemplazar la tabla con las columnas adicionales.
    """
    import os
    from google.auth import default as gauth_default
    from google.oauth2 import service_account

    print("[INFO] Iniciando la generación del SQL para fechas académicas/fiscales.", flush=True)

    # --- Autenticación consolidada (aunque esta función no conecta a BigQuery, se incluye para uniformidad) ---
    is_gcp = bool(os.environ.get("GOOGLE_CLOUD_PROJECT"))
    if is_gcp:
        print("[INFO] Entorno GCP detectado. Usando autenticación automática.", flush=True)
        creds, _ = gauth_default()
    else:
        json_keyfile = params.get("json_keyfile")
        if not json_keyfile:
            print("[INFO] No se proporcionó 'json_keyfile'. Se procederá sin autenticación explícita.", flush=True)
            creds = None
        else:
            print("[INFO] Entorno local/Colab detectado. Autenticando con JSON de credenciales.", flush=True)
            creds = service_account.Credentials.from_service_account_file(json_keyfile)

    # --- Validación y extracción de parámetros ---
    table_source = params["table_source"]
    table_destination = params["table_destination"]
    custom_fields_config = params["custom_fields_config"]

    print(f"[INFO] table_source: {table_source}", flush=True)
    print(f"[INFO] table_destination: {table_destination}", flush=True)
    print("[INFO] Procesando configuración de campos personalizados para fechas.", flush=True)

    # --- Generar expresiones adicionales para cada configuración ---
    additional_expressions = []
    for field, configs in custom_fields_config.items():
        for cfg in configs:
            start_month = cfg.get("start_month", 9)
            start_day = cfg.get("start_day", 1)
            suffix = cfg.get("suffix", "custom")
            new_field = f"{field}_{suffix}"
            expression = f"""
CASE
  WHEN (EXTRACT(MONTH FROM `{field}`) > {start_month})
       OR (EXTRACT(MONTH FROM `{field}`) = {start_month} AND EXTRACT(DAY FROM `{field}`) >= {start_day}) THEN
    CONCAT(
      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) - 2000) AS STRING), 2, '0'),
      '-',
      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) + 1 - 2000) AS STRING), 2, '0')
    )
  ELSE
    CONCAT(
      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) - 1 - 2000) AS STRING), 2, '0'),
      '-',
      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) - 2000) AS STRING), 2, '0')
    )
END AS `{new_field}`
""".strip()
            additional_expressions.append(expression)
            print(f"[INFO] Expresión generada para el campo '{field}' con suffix '{suffix}'.", flush=True)

    # Unir las expresiones adicionales en un bloque SELECT
    additional_select = ",\n  ".join(additional_expressions) if additional_expressions else ""
    
    # --- Construir el script SQL final ---
    if additional_select:
        SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
SELECT
  s.*,
  {additional_select}
FROM `{table_source}` s
;
""".strip()
    else:
        SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
SELECT
  *
FROM `{table_source}`;
""".strip()

    print("[INFO] SQL generado exitosamente.", flush=True)
    print("[DEBUG] SQL_script:", flush=True)
    print(SQL_script, flush=True)
    
    return SQL_script

#__________________________________________________________________________________________________________________________________________________________
# @title SQL_generate_join_tables_str()
#__________________________________________________________________________________________________________________________________________________________

def SQL_generate_join_tables_str(params: dict) -> str:
    """
    Crea (o reemplaza) una tabla uniendo:
    - Tabla primaria (table_source_primary)
    - Tabla secundaria (table_source_secondary)
    - (Opcional) Tabla puente (table_source_bridge)

    Obtiene las columnas reales de cada tabla usando INFORMATION_SCHEMA, y les
    aplica un prefijo para evitar duplicados en el SELECT.

    Args:
        params (dict):
            - table_source_primary (str): Nombre de la tabla primaria (proyecto.dataset.tabla).
            - table_source_primary_id_field (str): Campo de unión en la tabla primaria.
            - table_source_secondary (str): Nombre de la tabla secundaria (proyecto.dataset.tabla).
            - table_source_secondary_id (str): Campo de unión en la tabla secundaria.
            - table_source_bridge_use (bool): Indica si se empleará la tabla puente.
            - table_source_bridge (str, opcional): Tabla puente, si `table_source_bridge_use`=True (proyecto.dataset.tabla).
            - table_source_bridge_ids_fields (dict, opcional): Diccionario con
              { 'primary_id': '...', 'secondary_id': '...' } para la tabla puente.
            - join_type (str, opcional): Tipo de JOIN (INNER, LEFT, RIGHT, FULL, CROSS). Por defecto "LEFT".
            - join_field_prefixes (dict, opcional): Prefijos para primary, secondary, bridge.
              Por defecto {"primary": "p_", "secondary": "s_", "bridge": "b_"}.
            - table_destination (str): Tabla destino (proyecto.dataset.tabla).
            - json_keyfile (str, opcional): Ruta del archivo JSON de credenciales (solo requerido en entornos no GCP).
            - GCP_project_id (str, opcional): ID del proyecto a usar en caso de tablas en formato "dataset.table".

    Returns:
        str: Sentencia SQL para crear o reemplazar la tabla destino.

    Raises:
        ValueError: Si los parámetros obligatorios están ausentes o son inválidos.
    """
    import re
    import os
    from google.cloud import bigquery
    from google.auth import default as gauth_default
    from google.oauth2 import service_account

    # --- Validación de parámetros básicos ---
    table_source_primary = params["table_source_primary"]
    table_source_primary_id_field = params["table_source_primary_id_field"]
    table_source_secondary = params["table_source_secondary"]
    table_source_secondary_id = params["table_source_secondary_id"]

    table_source_bridge_use = params.get("table_source_bridge_use", False)
    table_source_bridge = params.get("table_source_bridge", "")
    table_source_bridge_ids_fields = params.get("table_source_bridge_ids_fields", {})

    join_type = params.get("join_type", "LEFT").upper()
    valid_join_types = ["INNER", "LEFT", "RIGHT", "FULL", "CROSS"]
    if join_type not in valid_join_types:
        raise ValueError(f"El join_type '{join_type}' no es válido. Debe ser uno de {valid_join_types}.")

    join_field_prefixes = params.get("join_field_prefixes", {"primary": "p_", "secondary": "s_", "bridge": "b_"})
    table_destination = params["table_destination"]

    # --- Autenticación consolidada ---
    is_gcp = bool(os.environ.get("GOOGLE_CLOUD_PROJECT"))
    if is_gcp:
        print("[INFO] Entorno detectado: GCP (autenticación automática)", flush=True)
        creds, _ = gauth_default()
    else:
        json_keyfile = params.get("json_keyfile")
        if not json_keyfile:
            raise ValueError("En entornos no GCP se debe proporcionar 'json_keyfile' en params.")
        print("[INFO] Entorno detectado: Local/Colab (usando JSON de credenciales)", flush=True)
        creds = service_account.Credentials.from_service_account_file(json_keyfile)

    # --- Helper: Extraer project, dataset y table_id ---
    def split_dataset_table(full_name: str):
        """
        Espera un formato "dataset.table" o "project.dataset.table".
        Retorna (project, dataset, table) para usar en queries.
        Si solo se provee "dataset.table", se utiliza el valor de 'GCP_project_id' de params o la variable de entorno.
        """
        parts = full_name.split(".")
        if len(parts) == 2:
            project = params.get("GCP_project_id") or os.environ.get("GOOGLE_CLOUD_PROJECT")
            if not project:
                raise ValueError("Para formato 'dataset.table' se requiere 'GCP_project_id' en params o variable de entorno.")
            return (project, parts[0], parts[1])
        elif len(parts) == 3:
            return (parts[0], parts[1], parts[2])
        else:
            raise ValueError(f"Nombre de tabla inválido: {full_name}")

    # --- Helper: Obtener columnas de una tabla ---
    def get_table_columns(full_table_name: str):
        """
        Retorna la lista de columnas de `full_table_name` consultando INFORMATION_SCHEMA.
        """
        proj, dset, tbl = split_dataset_table(full_table_name)
        print(f"[INFO] Obteniendo columnas de la tabla: {full_table_name}", flush=True)
        client = bigquery.Client(project=proj, credentials=creds)
        query = f"""
        SELECT column_name
        FROM `{proj}.{dset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{tbl}'
        ORDER BY ordinal_position
        """
        rows = client.query(query).result()
        col_list = [row.column_name for row in rows]
        print(f"[INFO] Se encontraron {len(col_list)} columnas en {full_table_name}", flush=True)
        return col_list

    # --- Obtener columnas de cada tabla ---
    primary_cols = get_table_columns(table_source_primary)
    secondary_cols = get_table_columns(table_source_secondary)
    bridge_cols = []
    if table_source_bridge_use and table_source_bridge:
        bridge_cols = get_table_columns(table_source_bridge)

    # --- Construir SELECT renombrando las columnas con prefijos ---
    primary_selects = [
        f"{join_field_prefixes['primary']}.{col} AS {join_field_prefixes['primary']}{col}"
        for col in primary_cols
    ]
    secondary_selects = [
        f"{join_field_prefixes['secondary']}.{col} AS {join_field_prefixes['secondary']}{col}"
        for col in secondary_cols
    ]
    bridge_selects = []
    if table_source_bridge_use and bridge_cols:
        bridge_selects = [
            f"{join_field_prefixes['bridge']}.{col} AS {join_field_prefixes['bridge']}{col}"
            for col in bridge_cols
        ]

    all_selects = primary_selects + secondary_selects + bridge_selects
    select_clause = ",\n  ".join(all_selects)

    # --- Construir la cláusula FROM + JOIN ---
    if table_source_bridge_use and table_source_bridge:
        join_clause = f"""
FROM `{table_source_primary}` AS {join_field_prefixes['primary']}
{join_type} JOIN `{table_source_bridge}` AS {join_field_prefixes['bridge']}
  ON {join_field_prefixes['bridge']}.{table_source_bridge_ids_fields['primary_id']} = {join_field_prefixes['primary']}.{table_source_primary_id_field}
{join_type} JOIN `{table_source_secondary}` AS {join_field_prefixes['secondary']}
  ON {join_field_prefixes['bridge']}.{table_source_bridge_ids_fields['secondary_id']} = {join_field_prefixes['secondary']}.{table_source_secondary_id}
"""
    else:
        join_clause = f"""
FROM `{table_source_primary}` AS {join_field_prefixes['primary']}
{join_type} JOIN `{table_source_secondary}` AS {join_field_prefixes['secondary']}
  ON {join_field_prefixes['primary']}.{table_source_primary_id_field} = {join_field_prefixes['secondary']}.{table_source_secondary_id}
"""

    # --- Armar la sentencia final ---
    SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
SELECT
  {select_clause}
{join_clause}
;
""".strip()

    print("[INFO] SQL generado para crear o reemplazar la tabla destino.", flush=True)
    return SQL_script

#__________________________________________________________________________________________________________________________________________________________
# @title SQL_generate_CPL_to_contacts_str()
#__________________________________________________________________________________________________________________________________________________________

def SQL_generate_CPL_to_contacts_str(params: dict) -> str:
    """
    Genera una sentencia SQL para crear o reemplazar una tabla que combina:
      1) Una tabla principal de contactos (table_source).
      2) Una tabla agregada con métricas por fecha y fuente (table_aggregated).
      3) Tablas dinámicas de métricas publicitarias.

    Args:
        params (dict): Diccionario con los parámetros:
            - table_destination (str): Nombre de la tabla resultado.
            - table_source (str): Tabla con información principal de contactos.
            - table_aggregated (str): Tabla agregada con métricas por fecha y fuente.
            - join_field (str): Campo de fecha para unión con aggregated.
            - join_on_source (str): Campo de fuente para unión con aggregated.
            - contact_creation_number (str): Campo que indica cuántos contactos se crearon.
            - ad_platforms (list): Lista de dicts con configuraciones de plataformas:
                - prefix (str): Prefijo identificador de la plataforma.
                - table (str): Nombre de la tabla de métricas publicitarias.
                - date_field (str): Campo de fecha para la unión.
                - source_value (str): Valor de fuente en la tabla de contactos.
                - total_* (str): Campos con métricas agregadas (e.g., field_total_spend).

    Returns:
        str: Sentencia SQL para crear o reemplazar la tabla combinada.

    Raises:
        ValueError: Si algún parámetro requerido falta o está vacío.
    """
    # Validar parámetros requeridos
    required_keys = [
        "table_destination", "table_source", "table_aggregated",
        "join_field", "join_on_source",
        "contact_creation_number", "ad_platforms"
    ]
    for key in required_keys:
        if key not in params or not params[key]:
            raise ValueError(f"El parámetro '{key}' es obligatorio y no puede estar vacío.")

    # Asignar variables a los parámetros
    table_destination = params["table_destination"]
    table_source = params["table_source"]
    table_aggregated = params["table_aggregated"]
    join_field = params["join_field"]
    join_on_source = params["join_on_source"]
    contact_creation_number = params["contact_creation_number"]
    ad_platforms = params["ad_platforms"]

    # Generar cláusulas SQL
    from_clause = f"""
FROM `{table_source}` o
LEFT JOIN `{table_aggregated}` a
  ON DATE(o.{join_field}) = a.{join_field}
  AND o.{join_on_source} = a.{join_on_source}
"""

    joins = []
    select_platform_metrics = []
    for idx, plat in enumerate(ad_platforms, start=1):
        alias = f"p{idx}"
        prefix = plat["prefix"]
        table = plat["table"]
        date_field = plat["date_field"]
        source_value = plat["source_value"]

        joins.append(f"""
LEFT JOIN `{table}` {alias}
  ON a.{join_field} = {alias}.{date_field}
""")

        for key, value in plat.items():
            if key.startswith("total_"):
                metric = key.replace("total_", "")
                col_name = f"contact_Ads_{prefix}_{metric}_by_day"
                expr = f"""
CASE
  WHEN a.{join_on_source} = "{source_value}" AND a.{contact_creation_number} > 0
    THEN {alias}.{value} / a.{contact_creation_number}
  ELSE NULL
END AS {col_name}
""".strip()
                select_platform_metrics.append(expr)

    final_select = ",\n".join(["o.*"] + select_platform_metrics)
    join_clause = "".join(joins)

    SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
SELECT
  {final_select}
{from_clause}
{join_clause}
;
""".strip()

    return SQL_script
#__________________________________________________________________________________________________________________________________________________________
# @title SQL_generate_deal_ordinal_str()
#__________________________________________________________________________________________________________________________________________________________

def SQL_generate_deal_ordinal_str(params):
    """
    Crea o reemplaza una tabla que contiene todos los campos de la tabla fuente
    más un campo ordinal que indica el número de negocio por cada contacto (ordenado
    por la fecha de creación), filtrando por ciertos valores de un campo (p. ej. pipeline).

    Parámetros esperados (dict):
      - table_source (str): Tabla origen.
      - table_destination (str): Tabla destino.
      - contact_id_field (str): Campo que identifica al contacto.
      - deal_id_field (str): Campo que identifica al negocio.
      - deal_createdate_field (str): Campo de fecha de creación del negocio.
      - deal_filter_field (str): Campo a filtrar (p. ej. 'pipeline').
      - deal_filter_values (list): Lista de valores admitidos para el filter_field.
      - ordinal_field_name (str): Nombre de la columna ordinal resultante.

    Retorna:
      (str): Sentencia SQL para crear o reemplazar la tabla con el campo ordinal.
    """
    table_source = params["table_source"]
    table_destination = params["table_destination"]
    contact_id_field = params["contact_id_field"]
    deal_id_field = params["deal_id_field"]
    deal_createdate_field = params["deal_createdate_field"]
    deal_filter_field = params["deal_filter_field"]
    deal_filter_values = params["deal_filter_values"]
    deal_ordinal_field_name = params["deal_ordinal_field_name"]

    # Convertir la lista de valores del filtro en una cadena 'IN(...)'
    filter_str_list = ", ".join([f"'{v}'" for v in deal_filter_values])

    # Armar la subconsulta deals_filtered que calcula row_number
    # para los registros que cumplan el filtro
    SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
WITH deals_filtered AS (
  SELECT
    {contact_id_field},
    {deal_id_field},
    ROW_NUMBER() OVER (
      PARTITION BY {contact_id_field}
      ORDER BY {deal_createdate_field}
    ) AS {deal_ordinal_field_name}
  FROM `{table_source}`
  WHERE {deal_filter_field} IN ({filter_str_list})
)
SELECT
  src.*,
  f.{deal_ordinal_field_name}
FROM `{table_source}` src
LEFT JOIN deals_filtered f
  ON src.{contact_id_field} = f.{contact_id_field}
  AND src.{deal_id_field} = f.{deal_id_field}
;
""".strip()

    return SQL_script
#__________________________________________________________________________________________________________________________________________________________
# @title SQL_generate_BI_view_str()
#__________________________________________________________________________________________________________________________________________________________

def SQL_generate_BI_view_str(params: dict) -> str:
    """
    Crea o reemplaza una vista (o tabla) tipo BI, seleccionando columnas
    de una tabla fuente con posibles mapeos de nombres, filtrado por rango
    de fechas y exclusión de registros marcados como borrados.

    Args:
        params (dict):
            - table_source (str): Tabla origen.
            - table_destination (str): Nombre de la vista o tabla destino.
            - fields_mapped_df (pd.DataFrame): DataFrame con ["Campo Original", "Campo Formateado"].
            - use_mapped_names (bool): Si True, usa nombres formateados.
            - creation_date_field (str, opcional): Campo de fecha para filtrar.
            - use_date_range (bool): Si True, filtra por rango de fechas.
            - date_range (dict, opcional): { "from": "YYYY-MM-DD", "to": "YYYY-MM-DD" }
            - exclude_deleted_records_bool (bool): Si True, excluye registros donde
              'exclude_deleted_records_field_name' = 'exclude_deleted_records_field_value'.
            - exclude_deleted_records_field_name (str, opcional): Nombre del campo que indica
              si el registro está “borrado”.
            - exclude_deleted_records_field_value (str, int, bool, etc., opcional): Valor que
              indica un registro “borrado”.

    Returns:
        str: Sentencia SQL para crear o reemplazar la vista/tabla BI.

    Raises:
        ValueError: Si faltan parámetros obligatorios como 'table_source', 'table_destination'
                    o 'fields_mapped_df'.
    """
    import pandas as pd

    # Validación de parámetros obligatorios
    table_source = params.get("table_source")
    table_destination = params.get("table_destination")
    fields_mapped_df = params.get("fields_mapped_df")

    if not table_source or not table_destination or not isinstance(fields_mapped_df, pd.DataFrame):
        raise ValueError("Faltan parámetros obligatorios: 'table_source', 'table_destination' o 'fields_mapped_df'.")

    use_mapped_names = params.get("use_mapped_names", True)
    creation_date_field = params.get("creation_date_field", "")
    date_range = params.get("date_range", {})
    use_date_range = params.get("use_date_range", False)
    exclude_deleted_records_bool = params.get("exclude_deleted_records_bool", False)
    exclude_deleted_records_field_name = params.get("exclude_deleted_records_field_name", "")
    exclude_deleted_records_field_value = params.get("exclude_deleted_records_field_value", None)

    # Construir SELECT
    select_cols = []
    for _, row in fields_mapped_df.iterrows():
        orig = row["Campo Original"]
        mapped = row["Campo Formateado"]
        if use_mapped_names:
            select_cols.append(f"`{orig}` AS `{mapped}`")
        else:
            select_cols.append(f"`{orig}`")

    select_clause = ",\n  ".join(select_cols)

    # Construir WHERE
    where_filters = []

    # Rango de fechas
    if use_date_range and creation_date_field:
        date_from = date_range.get("from", "")
        date_to = date_range.get("to", "")
        if date_from and date_to:
            where_filters.append(f"`{creation_date_field}` BETWEEN '{date_from}' AND '{date_to}'")
        elif date_from:
            where_filters.append(f"`{creation_date_field}` >= '{date_from}'")
        elif date_to:
            where_filters.append(f"`{creation_date_field}` <= '{date_to}'")

    # Excluir registros borrados (si se ha definido el campo y un valor que indique "borrado")
    if exclude_deleted_records_bool and exclude_deleted_records_field_name and exclude_deleted_records_field_value is not None:
        # Se excluyen registros donde el campo sea igual al valor "borrado"
        # => solo se incluyen los que NO cumplan esa condición
        # y los que sean NULL (si queremos conservarlos).
        where_filters.append(
            f"(`{exclude_deleted_records_field_name}` IS NULL OR `{exclude_deleted_records_field_name}` != {exclude_deleted_records_field_value})"
        )

    where_clause = " AND ".join(where_filters) if where_filters else "TRUE"

    # Generar la sentencia final (vista o tabla, se asume vista)
    sql_script_str = f"""
CREATE OR REPLACE VIEW `{table_destination}` AS
SELECT
  {select_clause}
FROM `{table_source}`
WHERE {where_clause}
;
""".strip()

    return sql_script_str
    
#__________________________________________________________________________________________________________________________________________________________
# @title GBQ_execute_SQL()
#__________________________________________________________________________________________________________________________________________________________
    
def GBQ_execute_SQL(params: dict) -> None:
    """
    Ejecuta un script SQL en Google BigQuery y muestra un resumen detallado con estadísticas del proceso,
    incluyendo logs de la API y todos los detalles disponibles del trabajo.

    Args:
        params (dict):
            - GCP_project_id (str): ID del proyecto de GCP.
            - SQL_script (str): Script SQL a ejecutar.
            - json_keyfile (str, opcional): Ruta del archivo JSON de credenciales (solo requerido en entornos no GCP).
            - destination_table (str, opcional): Nombre de la tabla destino para mostrar detalles (conteo de filas, tamaño, etc.).
    
    Raises:
        ValueError: Si faltan los parámetros necesarios.
        google.api_core.exceptions.GoogleAPIError: Si ocurre un error durante la ejecución del script.
    """
    import os
    import time
    from google.cloud import bigquery
    from google.api_core.exceptions import GoogleAPIError
    from google.auth import default as gauth_default
    from google.oauth2 import service_account

    def _validar_parametros(params: dict) -> (str, str, str, str):
        """Valida la existencia de los parámetros obligatorios y retorna sus valores."""
        project_id = params.get('GCP_project_id')
        sql_script = params.get('SQL_script')
        if not project_id or not sql_script:
            raise ValueError("Faltan los parámetros 'GCP_project_id' o 'SQL_script'.")
        json_keyfile = params.get('json_keyfile')
        destination_table = params.get('destination_table')  # Opcional
        return project_id, sql_script, json_keyfile, destination_table

    def _autenticar(project_id: str, json_keyfile: str):
        """
        Realiza la autenticación en BigQuery.
        Si se detecta el entorno GCP se utiliza la autenticación automática; 
        de lo contrario se requiere el json_keyfile.
        """
        is_gcp = bool(os.environ.get("GOOGLE_CLOUD_PROJECT"))
        if is_gcp:
            print("[INFO]: Entorno detectado: GCP (autenticación automática)", flush=True)
            creds, _ = gauth_default()
        else:
            if not json_keyfile:
                raise ValueError("En entornos no GCP se debe proporcionar 'json_keyfile' en params.")
            print("[INFO]: Entorno local/Colab detectado (usando JSON de credenciales)", flush=True)
            creds = service_account.Credentials.from_service_account_file(json_keyfile)
        return creds

    def _inicializar_cliente(project_id: str, creds) -> bigquery.Client:
        """Inicializa el cliente de BigQuery usando las credenciales obtenidas."""
        print("[INFO]: Iniciando el cliente de BigQuery...", flush=True)
        client = bigquery.Client(project=project_id, credentials=creds)
        print(f"[INFO]: Cliente de BigQuery inicializado en el proyecto: {project_id}\n", flush=True)
        return client

    def _mostrar_resumen_script(sql_script: str) -> None:
        """Muestra un resumen del script SQL (primeras 5 líneas y la acción principal)."""
        action = sql_script.strip().split()[0]
        print(f"[INFO]: Acción detectada en el script SQL: {action}", flush=True)
        print("[INFO]: Resumen del script SQL:", flush=True)
        for line in sql_script.strip().split('\n')[:5]:
            print(line, flush=True)
        print("...", flush=True)

    def _ejecutar_query(client: bigquery.Client, sql_script: str, start_time: float):
        """Ejecuta el script SQL y retorna el objeto del trabajo (job) y el tiempo transcurrido."""
        print("[INFO]: Ejecutando el script SQL...", flush=True)
        query_job = client.query(sql_script)
        print("[INFO]: Tiempo de inicio: %s" % time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(start_time)), flush=True)
        query_job.result()  # Espera a que se complete la consulta
        elapsed_time = time.time() - start_time
        print("[SUCCESS]: Consulta SQL ejecutada exitosamente.\n", flush=True)
        return query_job, elapsed_time

    def _mostrar_detalles_trabajo(client: bigquery.Client, query_job, elapsed_time: float, destination_table: str) -> None:
        """
        Muestra un resumen detallado del trabajo, extrayendo todos los detalles disponibles:
          - ID del trabajo
          - Estado
          - Tiempos de creación, inicio y finalización (si están disponibles)
          - Bytes procesados y bytes facturados
          - Indicador de cache_hit
          - Número de filas afectadas o retornadas (según corresponda)
          - Detalles adicionales de la tabla destino (conteo y tamaño) si se proporciona destination_table.
        """
        print("[INFO]: Detalles del trabajo de BigQuery:", flush=True)
        print(f"- ID del trabajo: {query_job.job_id}", flush=True)
        print(f"- Estado: {query_job.state}", flush=True)
        print(f"- Tiempo de creación: {query_job.created}", flush=True)
        if hasattr(query_job, 'started'):
            print(f"- Tiempo de inicio: {query_job.started}", flush=True)
        if hasattr(query_job, 'ended'):
            print(f"- Tiempo de finalización: {query_job.ended}", flush=True)
        print(f"- Bytes procesados: {query_job.total_bytes_processed if query_job.total_bytes_processed is not None else 'N/A'}", flush=True)
        print(f"- Bytes facturados: {query_job.total_bytes_billed if query_job.total_bytes_billed is not None else 'N/A'}", flush=True)
        print(f"- Cache hit: {query_job.cache_hit}", flush=True)
        
        # Número de filas afectadas (para DML) o nulos en caso de SELECT
        if query_job.num_dml_affected_rows is not None:
            print(f"- Filas afectadas (DML): {query_job.num_dml_affected_rows}", flush=True)
        else:
            print("- No se reportan filas afectadas (consulta SELECT u otra operación)", flush=True)
        
        # Si se proporciona la tabla destino, se muestra información adicional
        if destination_table:
            try:
                count_query = f"SELECT COUNT(*) AS total_rows FROM `{destination_table}`"
                count_result = client.query(count_query).result()
                rows_in_table = [row['total_rows'] for row in count_result][0]
                print(f"- Filas en la tabla destino ('{destination_table}'): {rows_in_table}", flush=True)
                
                table_ref = client.get_table(destination_table)
                table_size_kb = table_ref.num_bytes / 1024  # Convertir a KB
                print(f"- Tamaño de la tabla destino: {table_size_kb:.2f} KB", flush=True)
            except Exception as e:
                print(f"[WARN]: No se pudo obtener información adicional de la tabla destino: {e}", flush=True)
        else:
            print("[INFO]: No se proporcionó 'destination_table', omitiendo detalles adicionales de la tabla destino.", flush=True)
        
        print(f"[INFO]: Tiempo total de ejecución: {elapsed_time:.2f} segundos\n", flush=True)

    # Inicio del proceso
    project_id, sql_script, json_keyfile, destination_table = _validar_parametros(params)
    creds = _autenticar(project_id, json_keyfile)
    client = _inicializar_cliente(project_id, creds)
    
    start_time = time.time()
    _mostrar_resumen_script(sql_script)
    
    try:
        query_job, elapsed_time = _ejecutar_query(client, sql_script, start_time)
        _mostrar_detalles_trabajo(client, query_job, elapsed_time, destination_table)
    except GoogleAPIError as e:
        print(f"[ERROR]: Ocurrió un error al ejecutar el script SQL: {str(e)}\n", flush=True)
        raise


#__________________________________________________________________________________________________________________________________________________________
# @title DF_to_GBQ()
#__________________________________________________________________________________________________________________________________________________________

def DF_to_GBQ(params: dict) -> None:
    """
    Carga un DataFrame en una tabla de Google BigQuery e imprime un informe detallado con
    los datos del job de carga (ID, estado, tiempos, bytes procesados, etc.).

    Parámetros en el diccionario `params`:
      - 'source_df' (pd.DataFrame, requerido): DataFrame a subir.
      - 'destination_table' (str, requerido): Tabla de destino en formato 'project_id.dataset_id.table_id'.
      - 'json_keyfile' (str, requerido): Ruta al archivo JSON de la cuenta de servicio.
      - 'if_exists' (str, opcional): Qué hacer si la tabla ya existe:
            - 'fail': Genera error si la tabla ya existe.
            - 'replace': Borra la tabla existente y crea una nueva.
            - 'append': Agrega datos a la tabla existente.
          Por defecto: 'append'.

    Returns:
      None

    Raises:
      RuntimeError: Si ocurre un error al subir los datos a BigQuery.
    """
    import re
    import time
    import pandas as pd
    from google.cloud import bigquery
    from google.oauth2 import service_account

    def _validar_parametros(params: dict) -> None:
        """Valida que existan los parámetros obligatorios."""
        required_params = ['source_df', 'destination_table', 'json_keyfile']
        missing = [p for p in required_params if p not in params]
        if missing:
            raise RuntimeError(f"❌ Faltan los siguientes parámetros obligatorios: {missing}")

    def _sanitizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitiza los nombres de columnas para que cumplan con las restricciones de BigQuery:
          - Solo caracteres alfanuméricos y guiones bajos.
          - Si empieza por dígito, se le antepone un guion bajo.
        """
        def sanitize(col: str) -> str:
            new_col = re.sub(r'[^0-9a-zA-Z_]', '_', col)
            if new_col and new_col[0].isdigit():
                new_col = '_' + new_col
            return new_col

        df.columns = [sanitize(col) for col in df.columns]
        return df

    def _autenticar_gcp(json_path: str, project_id: str) -> bigquery.Client:
        """
        Autentica en Google Cloud usando un archivo JSON de cuenta de servicio.
        Retorna un cliente de BigQuery autenticado.
        """
        print("🔑 Autenticando en Google Cloud con JSON de cuenta de servicio...", flush=True)
        credentials = service_account.Credentials.from_service_account_file(json_path)
        client = bigquery.Client(credentials=credentials, project=project_id)
        print("✅ Autenticación exitosa.\n", flush=True)
        return client

    def _cargar_dataframe(client: bigquery.Client, df: pd.DataFrame, destination_table: str, if_exists: str) -> None:
        """
        Carga el DataFrame a la tabla de BigQuery especificada, aplicando la política de escritura según 'if_exists'
        e imprime todos los detalles de la ejecución del job.
        """
        config_map = {
            'fail': 'WRITE_EMPTY',
            'replace': 'WRITE_TRUNCATE',
            'append': 'WRITE_APPEND'
        }
        job_config = bigquery.LoadJobConfig(write_disposition=config_map.get(if_exists, 'WRITE_APPEND'))
        print(f"📤 Subiendo datos a la tabla '{destination_table}'...", flush=True)
        start_time = time.time()
        job = client.load_table_from_dataframe(df, destination_table, job_config=job_config)
        job.result()  # Espera a que se complete la carga
        elapsed_time = time.time() - start_time
        print("✅ Datos cargados correctamente.", flush=True)
        print(f"📊 Filas insertadas: {df.shape[0]}, Columnas: {df.shape[1]}", flush=True)
        print(f"[INFO]: Tiempo total de ejecución: {elapsed_time:.2f} segundos\n", flush=True)
        _mostrar_detalles_job(client, job, elapsed_time, destination_table)

    def _mostrar_detalles_job(client: bigquery.Client, job, elapsed_time: float, destination_table: str) -> None:
        """
        Muestra un resumen detallado del job de carga, extrayendo los detalles que provee la API:
          - ID del job, estado, tiempos, bytes procesados, bytes facturados, cache_hit, etc.
          - Detalles adicionales de la tabla destino (conteo de filas y tamaño).
        """
        print("[INFO]: Detalles del job de carga en BigQuery:", flush=True)
        print(f"- ID del job: {job.job_id}", flush=True)
        print(f"- Estado: {job.state}", flush=True)
        print(f"- Tiempo de creación: {job.created}", flush=True)
        if hasattr(job, 'started'):
            print(f"- Tiempo de inicio: {job.started}", flush=True)
        if hasattr(job, 'ended'):
            print(f"- Tiempo de finalización: {job.ended}", flush=True)
        if hasattr(job, 'total_bytes_processed'):
            print(f"- Bytes procesados: {job.total_bytes_processed}", flush=True)
        else:
            print("- Bytes procesados: N/A", flush=True)
        if hasattr(job, 'total_bytes_billed'):
            print(f"- Bytes facturados: {job.total_bytes_billed}", flush=True)
        else:
            print("- Bytes facturados: N/A", flush=True)
        if hasattr(job, 'cache_hit'):
            print(f"- Cache hit: {job.cache_hit}", flush=True)
        else:
            print("- Cache hit: N/A", flush=True)
        if hasattr(job, 'num_dml_affected_rows'):
            value = job.num_dml_affected_rows if job.num_dml_affected_rows is not None else "N/A"
            print(f"- Filas afectadas (DML): {value}", flush=True)
        else:
            print("- Filas afectadas (DML): N/A", flush=True)
        
        # Mostrar información adicional de la tabla destino
        try:
            count_query = f"SELECT COUNT(*) AS total_rows FROM `{destination_table}`"
            count_result = client.query(count_query).result()
            rows_in_table = [row['total_rows'] for row in count_result][0]
            print(f"- Filas en la tabla destino ('{destination_table}'): {rows_in_table}", flush=True)
            
            table_ref = client.get_table(destination_table)
            table_size_kb = table_ref.num_bytes / 1024  # Convertir a KB
            print(f"- Tamaño de la tabla destino: {table_size_kb:.2f} KB", flush=True)
        except Exception as e:
            print(f"[WARN]: No se pudo obtener información adicional de la tabla destino: {e}", flush=True)
        
        print(f"[INFO]: Tiempo total de ejecución del job: {elapsed_time:.2f} segundos\n", flush=True)

    # Inicio del proceso
    print("\n📌 Iniciando carga del DataFrame a BigQuery...", flush=True)
    _validar_parametros(params)

    df = params['source_df']
    destination_table = params['destination_table']
    json_path = params['json_keyfile']
    if_exists = params.get('if_exists', 'append')

    # Sanitizar nombres de columnas
    df = _sanitizar_columnas(df)
    print("🔍 Nombres de columnas sanitizados:", df.columns.tolist(), flush=True)

    # Extraer project_id a partir de destination_table (formato: 'project_id.dataset_id.table_id')
    try:
        project_id = destination_table.split('.')[0]
    except IndexError:
        raise RuntimeError("❌ El formato de 'destination_table' debe ser 'project_id.dataset_id.table_id'.")

    # Autenticación y carga
    client = _autenticar_gcp(json_path, project_id)
    try:
        _cargar_dataframe(client, df, destination_table, if_exists)
    except Exception as e:
        error_message = f"❌ Error al subir datos a BigQuery: {e}"
        print(error_message, flush=True)
        raise RuntimeError(error_message)
