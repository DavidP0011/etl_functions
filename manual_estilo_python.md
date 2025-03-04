# Manual de Estilo para Creación de Funciones en Python (Actualizado)

Este manual establece una guía detallada para escribir funciones en Python de forma **clara, modular, trazable y consistente**. La idea es que, al seguir estas directrices, se garantice la facilidad de mantenimiento, la reutilización del código y una depuración eficaz.

---

## 1. Nomenclatura: De lo General a lo Específico

### 1.1. Nombres de Funciones
- **Formato:** Utiliza *snake_case* (todo en minúsculas y separado por guiones bajos).
- **Estructura sugerida:**  
  **verbo** + **objeto/ámbito** + **detalle (opcional)** + **resultado** + **tipo de dato**  
  *Ejemplos:*  
  - `SQL_generate_report_str()`  
  - `API_fetch_data_dic()`  
  - `leads_calculate_conversion_float()`
- **Siglas reconocidas:** Escribe las siglas (por ejemplo, SQL, API, JSON) en **MAYÚSCULAS**.

### 1.2. Nombres de Variables
- Sigue la misma estructura que en las funciones, incluyendo un **sufijo de tipo**:
  - `_str`, `_int`, `_float`, `_bool`, `_dic`, `_list`, `_df`, `_set`, `_tuple`.
- *Ejemplos:*  
  - `campaign_cost_total_float`  
  - `SQL_query_str`  
  - `user_data_dic`

---

## 2. Estructura de Funciones

### 2.1. Definición y Argumentos
- **Uso de un único argumento:**  
  Todas las funciones deberán recibir un único parámetro, generalmente un diccionario `params: dict`.  
- Si se requieren varios parámetros, agrúpalos en un diccionario para mantener la uniformidad y facilitar la validación.
- **Docstrings completos:**  
  Cada función debe incluir un docstring que contenga:
  - Una breve descripción de la función.
  - Detalle de cada parámetro en `params` (nombre, tipo y descripción).
  - Tipo y descripción del valor de retorno.
  - Posibles excepciones (`Raises`).

### 2.2. Ejemplo Base
```python
# @title SQL_generate_report_str()
def SQL_generate_report_str(params: dict) -> str:
    """
    Genera un reporte SQL en forma de cadena.

    Args:
        params (dict):
            - query (str): Consulta SQL.
            - format (str, opcional): Formato de salida (default: 'csv').

    Returns:
        str: Reporte generado en formato cadena.

    Raises:
        ValueError: Si falta el parámetro 'query'.
    """
    query_str = params.get('query')
    if not query_str:
        raise ValueError("[VALIDATION [ERROR ❌]] Falta 'query' en params.")
    
    return f"Reporte para: {query_str}"
```

---

## 3. Validación de Parámetros

- **Inmediatamente al inicio de la función**, valida que se hayan proporcionado todos los parámetros obligatorios.  
- Utiliza mensajes de error claros y utiliza el prefijo `[VALIDATION [ERROR ❌]]` para indicar problemas en la validación.
- Ejemplo:
  ```python
  if leads_total_int == 0:
      raise ValueError("[VALIDATION [ERROR ❌]] Leads no puede ser cero.")
  ```

---

## 4. Estilo de Código y Formato

### 4.1. Formato y Espaciado
- **Indentación:** Utiliza 4 espacios por nivel.
- **Espaciado entre funciones:** Deja 2 líneas en blanco entre funciones.
- **Líneas en blanco:** Usa una línea en blanco antes del `return` si hay lógica previa para mejorar la legibilidad.

### 4.2. Importaciones
- **Importaciones globales:** Coloca las importaciones generales (librerías estándar o de terceros que se usan en varias funciones) al inicio del módulo.
- **Importaciones locales:** Importa dentro de la función aquellas librerías cuyo uso es específico para esa función. Esto ayuda a reducir dependencias globales y mejora la encapsulación.

---

## 5. Seguimiento y Mensajes de Log

### 5.1. Mensajes Estructurados
Utiliza `print(..., flush=True)` para asegurar que los mensajes se impriman inmediatamente. Se deben usar mensajes con prefijos estandarizados para cada fase del proceso:

- **Inicio y finalización:**
  - `[START ▶️]` → Inicio del proceso o tarea.
  - `[END [FINISHED 🏁]]` → Finalización exitosa.
  - `[END [FAILED ❌]]` → Finalización con errores.

- **Autenticación:**
  - `[AUTHENTICATION [INFOℹ️]]` → Información sobre el proceso de autenticación.
  - `[AUTHENTICATION [SUCCESS ✅]]` → Autenticación completada con éxito.
  - `[AUTHENTICATION [ERROR ❌]]` → Error durante la autenticación.

- **Extracción:**
  - `[EXTRACTION [START ⏳]]` → Inicio de la extracción de datos.
  - `[EXTRACTION [SUCCESS ✅]]` → Extracción completada correctamente.
  - `[EXTRACTION [WARNING ⚠️]]` → Advertencias durante la extracción.
  - `[EXTRACTION [ERROR ❌]]` → Error durante la extracción.

- **Transformación:**
  - `[TRANSFORMATION [START ▶️]]` → Inicio de la transformación de datos.
  - `[TRANSFORMATION [SUCCESS ✅]]` → Transformación realizada correctamente.
  - `[TRANSFORMATION [WARNING ⚠️]]` → Advertencia durante la transformación.
  - `[TRANSFORMATION [ERROR ❌]]` → Error durante la transformación.

- **Carga (Load):**
  - `[LOAD [START ▶️]]` → Inicio de la carga de datos.
  - `[LOAD [SUCCESS ✅]]` → Carga completada correctamente.
  - `[LOAD [WARNING ⚠️]]` → Advertencia durante la carga.
  - `[LOAD [ERROR ❌]]` → Error en la carga.

- **Métricas y Reporte:**
  - `[METRICS [INFO ℹ️]]` → Información y estadísticas del proceso.

### 5.2. Organización Visual
- **Separadores y Encabezados:**  
  Separa cada grupo de acciones o secciones con una línea en blanco y un encabezado visual (por ejemplo, usando "🔹🔹🔹") para agrupar fases o bloques de código dentro de la función.

---

## 6. Manejo de Errores

- **Uso de bloques `try-except`:**  
  Envuelve operaciones sensibles (como descarga de archivos, importación dinámica, lectura de datos) en bloques `try-except` para capturar y registrar errores.
- **Mensajes de Error Claros:**  
  Acompaña los errores con mensajes que incluyan el prefijo correspondiente, p.ej., `[EXTRACTION [ERROR ❌]]`, y proporciona detalles útiles (por ejemplo, el código de estado HTTP o parte del contenido de la respuesta).

---

## 7. Funciones Auxiliares (Subfunciones)

- **Definición interna:**  
  Siempre que la funcionalidad sea específica de una función "madre", define las funciones auxiliares (subfunciones) dentro de ella.  
- **Nomenclatura interna:**  
  Utiliza un guión bajo inicial (`_`) para indicar que son funciones internas y no deben formar parte de la API pública.
- **Ejemplo:**
  ```python
  def load_custom_libs(config_list: list) -> None:
      def _download_module_from_github(module_path: str) -> str:
          # Lógica para descargar el módulo...
          return temp_file_path
      # Resto de la función...
  ```

---

## 8. Orden y Organización del Código

- **Orden Alfabético:**  
  Dentro del repositorio, las funciones deben ordenarse alfabéticamente por su nombre. Esto facilita la búsqueda y el mantenimiento.
- **Agrupación Lógica:**  
  Las funciones relacionadas o que comparten un ámbito similar deben agruparse en módulos o secciones dentro del archivo.

---

## 9. Consideraciones Adicionales

- **Actualización y Recarga de Módulos:**  
  En funciones que realizan importaciones dinámicas (por ejemplo, `load_custom_libs`), se debe eliminar la versión previa del módulo y usar `importlib.invalidate_caches()` para asegurar la carga de la versión actual.
- **Gestión de Archivos Temporales:**  
  Cuando se descargue un archivo o módulo de forma dinámica (por ejemplo, desde GitHub), utiliza la librería `tempfile` para guardar el archivo y asegúrate de gestionar su eliminación o mantenimiento según sea necesario.
- **Uso de Zonas Horarias:**  
  Para mostrar fechas (por ejemplo, la fecha de última modificación de un módulo), utiliza el módulo `zoneinfo` (o alternativas como `pytz`) para ajustar la zona horaria según corresponda.

---

## 10. Ejemplo Completo de Función con Buenas Prácticas

```python
# @title load_custom_libs()
def load_custom_libs(config_list: list) -> None:
    """
    Carga dinámicamente uno o varios módulos a partir de una lista de diccionarios de configuración.

    Cada diccionario debe incluir:
      - module_host (str): "GD" para rutas locales o "github" para archivos en GitHub.
      - module_path (str): Ruta local o URL al archivo .py.
      - selected_functions_list (list, opcional): Lista de nombres de funciones/clases a importar.
        Si está vacío se importan todos los objetos definidos en el módulo.

    Para módulos alojados en GitHub, la URL se transforma a formato raw y se descarga en un archivo temporal.
    La fecha de última modificación se muestra ajustada a la zona horaria de Madrid.

    Retorna:
        None

    Raises:
        ValueError: Si faltan parámetros obligatorios.
    """
    import os
    import sys
    import importlib
    import inspect
    import datetime
    from zoneinfo import ZoneInfo
    import tempfile
    import requests

    # ────────────────────────────── Subfunciones Auxiliares ──────────────────────────────
    def _imprimir_encabezado(mensaje: str) -> None:
        print(f"\n🔹🔹🔹 {mensaje} 🔹🔹🔹\n", flush=True)

    def _download_module_from_github(module_path: str) -> str:
        if "github.com" in module_path:
            raw_url = module_path.replace("github.com", "raw.githubusercontent.com").replace("/blob", "")
        else:
            raw_url = module_path
        try:
            print(f"[EXTRACTION [START ▶️]] Descargando módulo desde GitHub: {raw_url}", flush=True)
            response = requests.get(raw_url)
            if response.status_code != 200:
                error_details = response.text[:200].strip()
                print(f"[EXTRACTION [ERROR ❌]] No se pudo descargar el archivo desde {raw_url}. Código de estado: {response.status_code}. Detalles: {error_details}", flush=True)
                return ""
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".py")
            temp_file.write(response.content)
            temp_file.close()
            print(f"[EXTRACTION [SUCCESS ✅]] Archivo descargado y guardado en: {temp_file.name}", flush=True)
            return temp_file.name
        except Exception as e:
            print(f"[EXTRACTION [ERROR ❌]] Error al descargar el archivo desde GitHub: {e}", flush=True)
            return ""

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
        print("\n[METRICS [INFO ℹ️]] Informe de carga del módulo:", flush=True)
        print(f"  - Módulo: {module_name}", flush=True)
        print(f"  - Ruta: {module_path}", flush=True)
        print(f"  - Fecha de última modificación: {mod_date}", flush=True)
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
    _imprimir_encabezado("[START ▶️] Iniciando carga de módulos personalizados")
    for config in config_list:
        module_host = config.get("module_host")
        module_path = config.get("module_path")
        selected_functions_list = config.get("selected_functions_list", [])

        if module_host == "github":
            temp_module_path = _download_module_from_github(module_path)
            if not temp_module_path:
                continue
            module_path = temp_module_path

        if not os.path.exists(module_path):
            print(f"[VALIDATION [ERROR ❌]] La ruta del módulo no existe: {module_path}", flush=True)
            continue

        importlib.invalidate_caches()
        mod_date = _get_module_mod_date(module_path)
        module, module_name = _import_module(module_path)
        if module is None:
            continue

        selected_objects = _get_defined_objects(module, selected_functions_list)
        globals().update(selected_objects)
        _print_module_report(module_name, module_path, mod_date, selected_objects)
```
