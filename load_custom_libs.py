# @title load_custom_libs()

def load_custom_libs(config_list: list) -> None:
    """
    Carga dinámicamente uno o varios módulos a partir de una lista de diccionarios de configuración.

    Cada diccionario debe incluir:
      - module_host: "GD" para rutas locales o "github" para archivos en GitHub.
      - module_path: Ruta local o URL al archivo .py.
      - selected_functions_list: Lista de nombres de funciones/clases a importar.
          Si está vacío se importan todos los objetos definidos en el módulo.

    Para módulos alojados en GitHub, la URL se transforma a formato raw y se descarga en un archivo temporal.
    La fecha de última modificación mostrada corresponde a la fecha del último commit en GitHub, convertida a la hora de Madrid.
    """
    import os
    import sys
    import importlib
    import inspect
    import datetime
    from zoneinfo import ZoneInfo  # Python 3.9+
    import tempfile
    import requests
    from urllib.parse import urlparse

    # ────────────────────────────── Subfunciones Auxiliares ──────────────────────────────
    def _imprimir_encabezado(mensaje: str) -> None:
        print(f"\n🔹🔹🔹 {mensaje} 🔹🔹🔹\n", flush=True)

    def _download_module_from_github(module_path: str) -> tuple:
        """
        Descarga el módulo desde GitHub forzando la actualización y obtiene la fecha
        del último commit mediante la API de GitHub.
        Retorna una tupla: (ruta_temporal, commit_date) donde commit_date es un objeto datetime en hora de Madrid.
        """
        headers = {"Cache-Control": "no-cache", "Pragma": "no-cache"}
        # Si se recibe una URL estándar de GitHub, se convierte a raw
        if "github.com" in module_path and "raw.githubusercontent.com" not in module_path:
            raw_url = module_path.replace("github.com", "raw.githubusercontent.com").replace("/blob", "")
        else:
            raw_url = module_path

        try:
            print(f"[EXTRACTION [START ▶️]] Descargando módulo desde GitHub: {raw_url}", flush=True)
            response = requests.get(raw_url, headers=headers)
            if response.status_code != 200:
                error_details = response.text[:200].strip()
                print(f"[EXTRACTION [ERROR ❌]] No se pudo descargar el archivo desde {raw_url}. Código de estado: {response.status_code}. Detalles: {error_details}", flush=True)
                return "", None

            # Obtener la fecha del último commit usando la API de GitHub
            commit_date = None
            if "raw.githubusercontent.com" in module_path:
                parsed = urlparse(module_path)
                parts = parsed.path.split('/')
                # Se espera: /owner/repo/branch/path/to/file.py
                if len(parts) >= 5:
                    owner = parts[1]
                    repo = parts[2]
                    branch = parts[3]
                    file_path_in_repo = "/".join(parts[4:])
                    api_url = f"https://api.github.com/repos/{owner}/{repo}/commits?path={file_path_in_repo}&sha={branch}&per_page=1"
                    api_response = requests.get(api_url, headers=headers)
                    if api_response.status_code == 200:
                        commit_info = api_response.json()
                        if isinstance(commit_info, list) and len(commit_info) > 0:
                            commit_date_str = commit_info[0]["commit"]["committer"]["date"]
                            commit_date = datetime.datetime.fromisoformat(commit_date_str.replace("Z", "+00:00"))
                            # Convertir a la zona horaria de Madrid
                            commit_date = commit_date.astimezone(ZoneInfo("Europe/Madrid"))
                    else:
                        print(f"[EXTRACTION [WARNING ⚠️]] No se pudo obtener la fecha del último commit. Código: {api_response.status_code}", flush=True)
            elif "github.com" in module_path:
                # URL estándar: /owner/repo/blob/branch/path/to/file.py
                parsed = urlparse(module_path)
                parts = parsed.path.split('/')
                if len(parts) >= 6 and parts[3] == "blob":
                    owner = parts[1]
                    repo = parts[2]
                    branch = parts[4]
                    file_path_in_repo = "/".join(parts[5:])
                    api_url = f"https://api.github.com/repos/{owner}/{repo}/commits?path={file_path_in_repo}&sha={branch}&per_page=1"
                    api_response = requests.get(api_url, headers=headers)
                    if api_response.status_code == 200:
                        commit_info = api_response.json()
                        if isinstance(commit_info, list) and len(commit_info) > 0:
                            commit_date_str = commit_info[0]["commit"]["committer"]["date"]
                            commit_date = datetime.datetime.fromisoformat(commit_date_str.replace("Z", "+00:00"))
                            # Convertir a la zona horaria de Madrid
                            commit_date = commit_date.astimezone(ZoneInfo("Europe/Madrid"))
                    else:
                        print(f"[EXTRACTION [WARNING ⚠️]] No se pudo obtener la fecha del último commit. Código: {api_response.status_code}", flush=True)

            # Guardar el archivo descargado en un directorio temporal
            parsed_url = urlparse(raw_url)
            base_file_name = os.path.basename(parsed_url.path)
            if not base_file_name.endswith(".py"):
                base_file_name += ".py"
            temp_dir = tempfile.gettempdir()
            temp_file_path = os.path.join(temp_dir, base_file_name)
            counter = 1
            original_file_name = base_file_name.rsplit(".", 1)[0]
            extension = ".py"
            while os.path.exists(temp_file_path):
                temp_file_path = os.path.join(temp_dir, f"{original_file_name}_{counter}{extension}")
                counter += 1
            with open(temp_file_path, "wb") as f:
                f.write(response.content)
            print(f"[EXTRACTION [SUCCESS ✅]] Archivo descargado y guardado en: {temp_file_path}", flush=True)
            return temp_file_path, commit_date
        except Exception as e:
            print(f"[EXTRACTION [ERROR ❌]] Error al descargar el archivo desde GitHub: {e}", flush=True)
            return "", None

    def _get_defined_objects(module, selected_functions_list: list) -> dict:
        all_objects = inspect.getmembers(module, lambda obj: inspect.isfunction(obj) or inspect.isclass(obj))
        defined_objects = {name: obj for name, obj in all_objects if getattr(obj, "__module__", "") == module.__name__}
        if selected_functions_list:
            return {name: obj for name, obj in defined_objects.items() if name in selected_functions_list}
        return defined_objects

    def _get_module_mod_date(module_path: str) -> datetime.datetime:
        # Para módulos locales se usa la fecha de modificación del archivo
        mod_timestamp = os.path.getmtime(module_path)
        mod_date = datetime.datetime.fromtimestamp(mod_timestamp, tz=ZoneInfo("Europe/Madrid"))
        return mod_date

    def _import_module(module_path: str):
        module_dir, module_file = os.path.split(module_path)
        module_name, _ = os.path.splitext(module_file)
        if module_dir not in sys.path:
            sys.path.insert(0, module_dir)
            print(f"[TRANSFORMATION [INFO ℹ️]] Directorio agregado al sys.path: {module_dir}", flush=True)
        if module_name in sys.modules:
            del sys.modules[module_name]
            print(f"[TRANSFORMATION [INFO ℹ️]] Eliminada versión previa del módulo: {module_name}", flush=True)
        try:
            print(f"[LOAD [START ▶️]] Importando módulo: {module_name}", flush=True)
            module = importlib.import_module(module_name)
            module = importlib.reload(module)
            print(f"[LOAD [SUCCESS ✅]] Módulo '{module_name}' importado correctamente.", flush=True)
            return module, module_name
        except Exception as e:
            print(f"[LOAD [ERROR ❌]] Error al importar el módulo '{module_name}': {e}", flush=True)
            return None, module_name

    def _print_module_report(module_name: str, module_path: str, mod_date: datetime.datetime, selected_objects: dict) -> None:
        print("\n[METRICS [INFO 📊]] Informe de carga del módulo:", flush=True)
        print(f"  - Módulo: {module_name}", flush=True)
        print(f"  - Ruta: {module_path}", flush=True)
        print(f"  - Fecha de última modificación (último commit en GitHub o mod. local): {mod_date}", flush=True)
        if not selected_objects:
            print("  - [WARNING ⚠️] No se encontraron objetos para importar.", flush=True)
        else:
            print("  - Objetos importados:", flush=True)
            for name, obj in selected_objects.items():
                obj_type = type(obj).__name__
                doc = inspect.getdoc(obj) or "Sin documentación"
                first_line = doc.split("\n")[0]
                print(f"      • {name} ({obj_type}): {first_line}", flush=True)
        print(f"\n[END [FINISHED ✅]] Módulo '{module_name}' actualizado e importado en globals().\n", flush=True)

    # ────────────────────────────── Proceso Principal ──────────────────────────────
    for config in config_list:
        # Obtener el nombre original del módulo a partir de la ruta en el config
        original_module_name = os.path.basename(config.get("module_path", ""))
        _imprimir_encabezado(f"[START ▶️] Iniciando carga de módulo {original_module_name}")

        module_host = config.get("module_host")
        module_path = config.get("module_path")
        selected_functions_list = config.get("selected_functions_list", [])
        github_commit_date = None

        if module_host == "github":
            temp_module_path, github_commit_date = _download_module_from_github(module_path)
            if not temp_module_path:
                continue
            module_path = temp_module_path

        if not os.path.exists(module_path):
            print(f"[VALIDATION [ERROR ❌]] La ruta del módulo no existe: {module_path}", flush=True)
            continue

        importlib.invalidate_caches()
        # Si se obtuvo la fecha del último commit desde GitHub, se utiliza; de lo contrario, se toma la fecha de modificación local
        if module_host == "github" and github_commit_date is not None:
            mod_date = github_commit_date
        else:
            mod_date = _get_module_mod_date(module_path)

        module, module_name = _import_module(module_path)
        if module is None:
            continue

        selected_objects = _get_defined_objects(module, selected_functions_list)
        globals().update(selected_objects)
        _print_module_report(module_name, module_path, mod_date, selected_objects)
