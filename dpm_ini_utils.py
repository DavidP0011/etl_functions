
# __________________________________________________________________________________________________________________________________________________________
# environment_identification
# __________________________________________________________________________________________________________________________________________________________
def ini_install_libraries(params: dict) -> None:
    """
    Verifica e instala una librería de Python o un paquete del sistema.

    Args:
        params (dict):
            - name (str): Nombre descriptivo para mensajes.
            - is_system (bool, opcional): Indica si es un paquete del sistema.
            - import_name (str): Nombre del módulo a importar (para librerías Python).
            - pip_name (str, opcional): Nombre del paquete para instalar vía pip (si difiere del módulo).
            - version (str, opcional): Versión específica a instalar.
            - install_cmd (str, opcional): Comando de instalación personalizado.
            - check_cmd (str, opcional): Comando para verificar la instalación (para paquetes del sistema).
            - install_cmds (list, opcional): Lista de comandos para instalar paquetes del sistema.

    Returns:
        None

    Raises:
        ValueError: Si falta 'name' o 'import_name' en params.
    """
    from IPython import get_ipython
    import os
    import importlib
    

    # ────────────────────────────── Validación de Parámetros ──────────────────────────────
    name_str = params.get("name")
    is_system_bool = params.get("is_system", False)
    import_name_str = params.get("import_name")

    if not name_str or not import_name_str:
        raise ValueError("[VALIDATION [ERROR ❌]] 'name' e 'import_name' son obligatorios en params.")

    print(f"\n[START ▶️] Verificando instalación de {name_str}...", flush=True)

    # ────────────────────────────── Verificación de Paquetes del Sistema ──────────────────────────────
    if is_system_bool:
        check_cmd_str = params.get("check_cmd")
        if not check_cmd_str:
            print(f"[VALIDATION [WARNING ⚠️]] No se especificó 'check_cmd' para {name_str}.", flush=True)
            return

        if os.system(check_cmd_str) != 0:
            print(f"[INSTALLATION [INFO ℹ️]] {name_str} no está instalado. Procediendo con la instalación...", flush=True)
            for cmd_str in params.get("install_cmds", []):
                print(f"[INSTALLATION [COMMAND ▶️]] Ejecutando: {cmd_str}", flush=True)
                os.system(cmd_str)
        else:
            print(f"[INSTALLATION [SUCCESS ✅]] {name_str} ya está instalado.", flush=True)
        return

    # ────────────────────────────── Verificación de Librerías de Python ──────────────────────────────
    try:
        importlib.import_module(import_name_str)
        print(f"[INSTALLATION [SUCCESS ✅]] {name_str} ya está instalado.", flush=True)
    except ImportError:
        print(f"[INSTALLATION [INFO ℹ️]] {name_str} no está instalado. Procediendo con la instalación...", flush=True)

        install_cmd_str = params.get("install_cmd")
        pip_name_str = params.get("pip_name", import_name_str)
        version_str = params.get("version")
        version_spec_str = f"=={version_str}" if version_str else ""

        if install_cmd_str:
            print(f"[INSTALLATION [COMMAND ▶️]] Ejecutando comando personalizado: {install_cmd_str}", flush=True)
            os.system(install_cmd_str)
        else:
            install_cmd_pip_str = f"pip install --upgrade {pip_name_str}{version_spec_str}"
            print(f"[INSTALLATION [COMMAND ▶️]] Ejecutando: {install_cmd_pip_str}", flush=True)
            os.system(install_cmd_pip_str)

    print(f"[END [FINISHED ✅]] Proceso de instalación finalizado para {name_str}.\n", flush=True)


# __________________________________________________________________________________________________________________________________________________________
# environment_identification
# __________________________________________________________________________________________________________________________________________________________

def ini_environment_identification() -> str:
    """
    Detecta el entorno de ejecución original basado en variables de entorno y módulos disponibles.

    La función utiliza la siguiente lógica:
      - Si la variable de entorno 'VERTEX_PRODUCT' tiene el valor 'COLAB_ENTERPRISE', se asume que se está ejecutando en Colab Enterprise y se devuelve ese valor original.
      - Si la variable de entorno 'GOOGLE_CLOUD_PROJECT' existe, se asume que se está ejecutando en GCP y se devuelve su valor original.
      - Si se puede importar el módulo 'google.colab', se asume que se está ejecutando en Colab (estándar) y se devuelve 'COLAB'.
      - Si ninguna de las condiciones anteriores se cumple, se asume que el entorno es Local y se devuelve 'LOCAL'.

    Returns:
        str: Cadena que representa el entorno de ejecución original. Los posibles valores son:
             - 'COLAB_ENTERPRISE'
             - El valor de la variable 'GOOGLE_CLOUD_PROJECT' (ej.: 'mi-proyecto')
             - 'COLAB'
             - 'LOCAL'
    """
    import os

    # ────────────────────────────── DETECCIÓN DEL ENTORNO ──────────────────────────────
    # Verificar si se está en Colab Enterprise / VERTEX_PRODUCT
    if os.environ.get('VERTEX_PRODUCT') == 'COLAB_ENTERPRISE':
        return os.environ.get('VERTEX_PRODUCT')
    
    # Verificar si se está en un entorno GCP (Google Cloud Platform)
    if os.environ.get('GOOGLE_CLOUD_PROJECT'):
        return os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    # Verificar si se está en Colab estándar
    try:
        import google.colab  # type: ignore
        return 'COLAB'
    except ImportError:
        pass

    # Por defecto, se asume que se está en un entorno local
    return 'LOCAL'








# __________________________________________________________________________________________________________________________________________________________
# environment_identification
# __________________________________________________________________________________________________________________________________________________________
def ini_google_drive_instalation(params: dict) -> None:
    """
    Monta Google Drive en función del entorno de ejecución especificado en params.

    Args:
        params (dict):
            - entorno_identificado_str (str): Valor que indica el entorno de ejecución.
              Los posibles valores pueden ser:
                * 'VERTEX_PRODUCT'
                * 'COLAB'
                * Cualquier otro valor que indique un entorno diferente (por ejemplo, el nombre de un proyecto GCP o 'LOCAL').

    Returns:
        None

    Raises:
        ValueError: Si falta la key 'entorno_identificado_str' en params.
    """
    entorno_identificado_str = params.get('entorno_identificado_str')
    if not entorno_identificado_str:
        raise ValueError("[VALIDATION [ERROR ❌]] Falta la key 'entorno_identificado_str' en params.")

    # Montar Google Drive si el entorno es Colab (estándar o Enterprise)
    if entorno_identificado_str in ['COLAB']:
        try:
            from google.colab import drive
            drive.mount('/content/drive')
            print("[INFO ℹ️] Google Drive montado correctamente.", flush=True)
        except ImportError as e:
            print(f"[ERROR ❌] No se pudo importar google.colab para montar Google Drive: {e}", flush=True)
    else:
        print(f"[INFO ℹ️] El entorno '{entorno_identificado_str}' no requiere montaje de Google Drive.", flush=True)













# __________________________________________________________________________________________________________________________________________________________
# load_custom_libs
# __________________________________________________________________________________________________________________________________________________________
def ini_load_dpm_libs(config_list: list) -> None:
    """
    Carga dinámicamente uno o varios módulos a partir de una lista de diccionarios de configuración.

    Cada diccionario debe incluir:
      - module_host: "GD" para rutas locales o "github" para archivos en GitHub.
      - module_path: Ruta local o URL al archivo .py.
      - selected_functions_list: Lista de nombres de funciones/clases a importar.
          Si está vacío se importan todos los objetos definidos en el módulo.

    Para módulos alojados en GitHub, la URL se transforma a formato raw y se descarga en un archivo temporal.
    La fecha de última modificación mostrada corresponde a la fecha del último commit en GitHub,
    convertida a la hora de Madrid.
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
    import builtins  # Se actualizarán los builtins para que las funciones sean globales

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
                            commit_date = commit_date.astimezone(ZoneInfo("Europe/Madrid"))
                    else:
                        print(f"[EXTRACTION [WARNING ⚠️]] No se pudo obtener la fecha del último commit. Código: {api_response.status_code}", flush=True)
            elif "github.com" in module_path:
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
        print(f"\n[END [FINISHED ✅]] Módulo '{module_name}' actualizado e importado en los builtins.\n", flush=True)

    # ────────────────────────────── Proceso Principal ──────────────────────────────
    for config in config_list:
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
        if module_host == "github" and github_commit_date is not None:
            mod_date = github_commit_date
        else:
            mod_date = _get_module_mod_date(module_path)

        module, module_name = _import_module(module_path)
        if module is None:
            continue

        selected_objects = _get_defined_objects(module, selected_functions_list)
        # Actualizamos los builtins para que los objetos sean accesibles globalmente en Colab
        import builtins
        builtins.__dict__.update(selected_objects)
        _print_module_report(module_name, module_path, mod_date, selected_objects)
