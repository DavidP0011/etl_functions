# ----------------------------------------------------------------------------
# box_office_mojo_to_GBQ()
# ----------------------------------------------------------------------------

def box_office_mojo_to_GBQ(params: dict) -> None:
    """
    Scrapea datos de Box Office Mojo, agrega el tconst desde IMDb Pro y carga la información en una tabla de BigQuery.

    Args:
        params (dict):
            - destination_table (str): Nombre completo de la tabla destino en BigQuery (formato "proyecto.dataset.tabla").
            - start_year (int): Año inicial del rango.
            - end_year (int): Año final del rango.
            - delete_previous_table (bool): Si es True, se elimina la tabla anterior antes de cargar los datos.
            - json_keyfile_GCP_secret_id (str): ID del secret para obtener credenciales desde Secret Manager en GCP.
            - json_keyfile_colab (str): Ruta al archivo JSON de credenciales para entornos no GCP.

    Returns:
        None

    Raises:
        ValueError: Si faltan parámetros obligatorios o credenciales.
    """
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd
    from google.cloud import bigquery, secretmanager
    import re
    import time
    from datetime import datetime
    import os, json
    from google.oauth2 import service_account
    # ────────────────────────────── Validación de Parámetros ──────────────────────────────
    required_keys = [
        "destination_table", "start_year", "end_year",
        "delete_previous_table", "json_keyfile_GCP_secret_id", "json_keyfile_colab"
    ]
    for key in required_keys:
        if key not in params:
            raise ValueError(f"[VALIDATION [ERROR ❌]] Falta '{key}' en params.")

    print("\n🔹🔹🔹 INICIO DEL PROCESO DE SCRAPING Y CARGA A BIGQUERY 🔹🔹🔹\n", flush=True)

    # ────────────────────────────── Autenticación ──────────────────────────────
    print("[AUTHENTICATION [INFO] 🔐] Iniciando autenticación...", flush=True)
    if os.environ.get("GOOGLE_CLOUD_PROJECT"):
        secret_id_str = params.get("json_keyfile_GCP_secret_id")
        if not secret_id_str:
            raise ValueError("[AUTHENTICATION [ERROR ❌]] En GCP se debe proporcionar 'json_keyfile_GCP_secret_id'.")
        print("[AUTHENTICATION [INFO] 🔐] Entorno GCP detectado. Obteniendo credenciales desde Secret Manager...", flush=True)
        project_env_str = os.environ.get("GOOGLE_CLOUD_PROJECT")
        client_sm = secretmanager.SecretManagerServiceClient()
        secret_name_str = f"projects/{project_env_str}/secrets/{secret_id_str}/versions/latest"
        response = client_sm.access_secret_version(name=secret_name_str)
        secret_string = response.payload.data.decode("UTF-8")
        secret_info = json.loads(secret_string)
        creds = service_account.Credentials.from_service_account_info(secret_info)
        print("[AUTHENTICATION [SUCCESS ✅]] Credenciales obtenidas desde Secret Manager.", flush=True)
    else:
        json_path_str = params.get("json_keyfile_colab")
        if not json_path_str:
            raise ValueError("[AUTHENTICATION [ERROR ❌]] En entornos no GCP se debe proporcionar 'json_keyfile_colab'.")
        print("[AUTHENTICATION [INFO] 🔐] Entorno local detectado. Usando credenciales desde archivo JSON...", flush=True)
        creds = service_account.Credentials.from_service_account_file(json_path_str)
        print("[AUTHENTICATION [SUCCESS ✅]] Credenciales cargadas desde archivo JSON.", flush=True)

    # Crear cliente de BigQuery utilizando el proyecto extraído de destination_table
    dest_table_str = params["destination_table"]
    project_id_str = dest_table_str.split(".")[0]
    client = bigquery.Client(project=project_id_str, credentials=creds)

    # ────────────────────────────── Función Interna: _scrape_box_office ──────────────────────────────
    def _scrape_box_office(year_int: int) -> pd.DataFrame:
        url_str = f"https://www.boxofficemojo.com/year/world/{year_int}/"
        print("\n🔹🔹🔹 EXTRACCIÓN DE DATOS ─ Año " + str(year_int) + " 🔹🔹🔹\n", flush=True)
        print(f"[EXTRACTION [START ⏳]] Accediendo a la URL: {url_str}", flush=True)
        try:
            response = requests.get(url_str)
            response.raise_for_status()
            print(f"[EXTRACTION [SUCCESS ✅]] Respuesta HTTP obtenida para el año {year_int}.", flush=True)
        except requests.exceptions.RequestException as e:
            print(f"[EXTRACTION [ERROR ❌]] Error al acceder a la página para el año {year_int}: {e}", flush=True)
            return pd.DataFrame()

        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table")
        if not table:
            print(f"[EXTRACTION [WARNING ⚠️]] No se encontró la tabla para el año {year_int}.", flush=True)
            return pd.DataFrame()

        headers = [header.text.strip() for header in table.find_all("th")]
        print(f"[EXTRACTION [INFO ℹ️]] Encabezados de la tabla para {year_int}: {headers}", flush=True)

        try:
            rank_idx = headers.index("Rank")
            title_idx = headers.index("Release Group")
            worldwide_gross_idx = headers.index("Worldwide")
            domestic_gross_idx = headers.index("Domestic")
            international_gross_idx = headers.index("Foreign")
        except ValueError as ve:
            print(f"[EXTRACTION [ERROR ❌]] Error al encontrar índices de columnas: {ve}", flush=True)
            return pd.DataFrame()

        rows = table.find_all("tr")[1:]
        total_rows_int = len(rows)
        print(f"[TRANSFORMATION [START ▶️]] Procesando {total_rows_int} filas para el año {year_int}...", flush=True)

        data_list = []
        for idx_int, row in enumerate(rows, start=1):
            cols = row.find_all("td")
            if len(cols) < max(rank_idx, title_idx, worldwide_gross_idx, domestic_gross_idx, international_gross_idx) + 1:
                print(f"[TRANSFORMATION [WARNING ⚠️]] Fila {idx_int} ignorada por tener columnas insuficientes.", flush=True)
                continue

            rank_text_str = cols[rank_idx].text.strip()
            if not rank_text_str.isdigit():
                print(f"[TRANSFORMATION [WARNING ⚠️]] Fila {idx_int} ignorada porque 'Rank' no es un número: '{rank_text_str}'", flush=True)
                continue

            try:
                rank_int = int(rank_text_str)
                title_str = cols[title_idx].text.strip()
                title_link_str = cols[title_idx].find("a")["href"]

                def _parse_gross(gross_str: str) -> int:
                    gross_str = gross_str.strip().replace("$", "").replace(",", "").replace("--", "").replace("-", "")
                    if gross_str in ["", "--", "-"]:
                        return None
                    match = re.match(r'^\d+$', gross_str)
                    return int(gross_str) if match else None

                domestic_text_str = cols[domestic_gross_idx].text.strip()
                foreign_text_str = cols[international_gross_idx].text.strip()
                print(f"[TRANSFORMATION [INFO ℹ️]] Fila {idx_int}: Domestic='{domestic_text_str}', Foreign='{foreign_text_str}'", flush=True)

                worldwide_gross_int = _parse_gross(cols[worldwide_gross_idx].text)
                domestic_gross_int = _parse_gross(domestic_text_str)
                international_gross_int = _parse_gross(foreign_text_str)

                data_list.append({
                    "Year": year_int,
                    "Rank": rank_int,
                    "Title": title_str,
                    "Worldwide_Gross": worldwide_gross_int,
                    "Domestic_Gross": domestic_gross_int,
                    "International_Gross": international_gross_int,
                    "Detail_Link": f"https://www.boxofficemojo.com{title_link_str}"
                })
            except Exception as e:
                print(f"[TRANSFORMATION [ERROR ❌]] Error procesando una fila en el índice {idx_int}: {e}", flush=True)
                continue

            time.sleep(0.1)  # Pausa de 100ms

        print(f"[TRANSFORMATION [SUCCESS ✅]] Procesadas {len(data_list)} filas válidas para el año {year_int}.", flush=True)
        return pd.DataFrame(data_list)

    # ────────────────────────────── Función Interna: _get_tconst ──────────────────────────────
    def _get_tconst(detail_link_str: str) -> str:
        print("\n🔹🔹🔹 OBTENCIÓN DE tconst ─ Detalle: " + detail_link_str + " 🔹🔹🔹\n", flush=True)
        print(f"[EXTRACTION [START ⏳]] Obteniendo tconst desde: {detail_link_str}", flush=True)
        try:
            response = requests.get(detail_link_str)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            imdb_link = soup.find("a", href=lambda href: href and "pro.imdb.com/title" in href)
            if imdb_link:
                tconst_match = re.search(r'/title/(tt\d+)/', imdb_link["href"])
                if tconst_match:
                    tconst_str = tconst_match.group(1)
                    print(f"[EXTRACTION [SUCCESS ✅]] tconst encontrado: {tconst_str}", flush=True)
                    return tconst_str
            print(f"[EXTRACTION [WARNING ⚠️]] tconst no encontrado en: {detail_link_str}", flush=True)
        except requests.exceptions.RequestException as e:
            print(f"[EXTRACTION [ERROR ❌]] Error al acceder al detalle: {detail_link_str} - {e}", flush=True)
        except Exception as e:
            print(f"[EXTRACTION [ERROR ❌]] Error al procesar el detalle: {detail_link_str} - {e}", flush=True)
        return None

    # ────────────────────────────── Extracción y Transformación de Datos ──────────────────────────────
    all_data_df = pd.DataFrame()
    print("\n🔹🔹🔹 INICIO DE EXTRACCIÓN DE DATOS POR AÑO 🔹🔹🔹\n", flush=True)
    for year_int in range(params["start_year"], params["end_year"] + 1):
        print("\n🔹🔹🔹 Año " + str(year_int) + " 🔹🔹🔹\n", flush=True)
        year_data_df = _scrape_box_office(year_int)
        if year_data_df.empty:
            print(f"[EXTRACTION [WARNING ⚠️]] No se encontraron datos para el año {year_int}.", flush=True)
            continue

        print("\n🔹🔹🔹 TRANSFORMACIÓN: OBTENCIÓN DE tconst ─ Títulos ─🔹🔹🔹\n", flush=True)
        year_data_df["tconst"] = year_data_df["Detail_Link"].apply(_get_tconst)

        tconst_found_int = year_data_df["tconst"].notnull().sum()
        tconst_total_int = len(year_data_df)
        print(f"[TRANSFORMATION [INFO ℹ️]] tconst encontrados: {tconst_found_int}/{tconst_total_int}", flush=True)

        all_data_df = pd.concat([all_data_df, year_data_df], ignore_index=True)

    if all_data_df.empty:
        print(f"[END [FAILED ❌]] No hay datos para subir a BigQuery.", flush=True)
        return

    # ────────────────────────────── Resumen y Validación de Datos ──────────────────────────────
    print("\n🔹🔹🔹 RESUMEN Y VALIDACIÓN DE DATOS 🔹🔹🔹\n", flush=True)
    schema_list = [
        bigquery.SchemaField("Year", "INTEGER"),
        bigquery.SchemaField("Rank", "INTEGER"),
        bigquery.SchemaField("Title", "STRING"),
        bigquery.SchemaField("Worldwide_Gross", "INTEGER"),
        bigquery.SchemaField("Domestic_Gross", "INTEGER"),
        bigquery.SchemaField("International_Gross", "INTEGER"),
        bigquery.SchemaField("Detail_Link", "STRING"),
        bigquery.SchemaField("tconst", "STRING"),
    ]

    expected_types = {
        "Year": int,
        "Rank": int,
        "Title": str,
        "Worldwide_Gross": pd.Int64Dtype(),
        "Domestic_Gross": pd.Int64Dtype(),
        "International_Gross": pd.Int64Dtype(),
        "Detail_Link": str,
        "tconst": str,
    }

    for column_str, dtype in expected_types.items():
        if column_str in all_data_df.columns:
            all_data_df[column_str] = all_data_df[column_str].astype(dtype)
        else:
            all_data_df[column_str] = None

    print(f"[METRICS [INFO ℹ️]] Vista previa de los datos recopilados:", flush=True)
    print(all_data_df.head(), flush=True)

    print(f"\n[METRICS [INFO ℹ️]] Resumen de valores nulos:", flush=True)
    print(all_data_df.isnull().sum(), flush=True)

    # ────────────────────────────── Carga a BigQuery ──────────────────────────────
    print("\n🔹🔹🔹 CARGA DE DATOS A BIGQUERY 🔹🔹🔹\n", flush=True)
    table_id_str = params["destination_table"]
    print(f"[LOAD [START ▶️]] Cargando datos a la tabla de BigQuery: {table_id_str}...", flush=True)

    job_config = bigquery.LoadJobConfig(
        schema=schema_list,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if params.get("delete_previous_table", False)
        else bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    try:
        job = client.load_table_from_dataframe(all_data_df, table_id_str, job_config=job_config)
        job.result()  # Esperar a que finalice la carga
        print(f"[LOAD [SUCCESS ✅]] Datos subidos exitosamente!", flush=True)
        print(f"[METRICS [INFO ℹ️]] Total de registros cargados: {len(all_data_df)}", flush=True)
    except Exception as e:
        print(f"[LOAD [ERROR ❌]] Error al subir datos a BigQuery: {e}", flush=True)

    print("\n🔹🔹🔹 FIN DEL PROCESO 🔹🔹🔹\n", flush=True)
    print(f"[END [FINISHED 🏁]] Proceso finalizado exitosamente.", flush=True)
