# Manual de Estilo para Funciones DPM en Python

Este manual ofrece una guía para escribir funciones en Python de forma **clara, modular, trazable y consistente**. Su objetivo es facilitar el mantenimiento, la reutilización y la depuración del código.

---

## 1. Introducción y Objetivos

- **Propósito:** Establecer directrices para la creación de funciones que sean fáciles de mantener, reutilizar y depurar.
- **Enfoque:** Código limpio, validaciones tempranas, mensajes de log estandarizados y organización modular.

---

## 2. Nomenclatura y Convenciones

### 2.1. Nombres de Funciones

- **Formato:** Usa *snake_case* (minúsculas y guiones bajos).
- **Estructura Sugerida:**  
  **verbo** + **objeto/ámbito** + **detalle (opcional)** + **resultado** + **tipo de dato**  
  *Ejemplos:*  
  - `SQL_generate_report_str()`  
  - `API_fetch_data_dic()`  
  - `leads_calculate_conversion_float()`
- **Siglas:** Se escriben en **MAYÚSCULAS** (ej.: SQL, API, JSON).

### 2.2. Nombres de Variables

- Aplica el mismo esquema que para funciones, añadiendo un **sufijo de tipo**:  
  `_str`, `_int`, `_float`, `_bool`, `_dic`, `_list`, `_df`, `_set`, `_tuple`.
- *Ejemplos:*  
  - `campaign_cost_total_float`  
  - `SQL_query_str`  
  - `user_data_dic`

---

## 3. Estructura y Documentación de Funciones

### 3.1. Definición y Parámetros

- **Argumento Único:**  
  Toda función debe recibir un único parámetro, usualmente un diccionario (`config: dict`), para agrupar los valores de entrada.
- **Agrupación de Parámetros:**  
  Si se requieren múltiples entradas, se deben consolidar en el diccionario para mantener la uniformidad.

### 3.2. Docstrings Completos

Cada función debe incluir un docstring que contenga:
- **Descripción breve** de la función.
- **Detalle de parámetros:** nombre, tipo y descripción.
- **Valor de retorno:** tipo y descripción.
- **Excepciones:** Detalle de posibles errores (`Raises`).

#### Ejemplo Base
```python
def SQL_generate_report_str(config: dict) -> str:
    """
    Genera un reporte SQL en forma de cadena.

    Args:
        config (dict):
            - query (str): Consulta SQL.
            - format (str, opcional): Formato de salida (default: 'csv').

    Returns:
        str: Reporte generado en formato cadena.

    Raises:
        ValueError: Si falta el parámetro 'query'.
    """
    query_str = config.get('query')
    if not query_str:
        raise ValueError("[VALIDATION [ERROR ❌]] Falta 'query' en config.")
    
    return f"Reporte para: {query_str}"
```

---

## 4. Validación de Parámetros

- **Validación Inicial:**  
  Realiza la verificación de todos los parámetros obligatorios al inicio de la función.
- **Mensajes de Error:**  
  Usa mensajes claros con el prefijo `[VALIDATION [ERROR ❌]]`.

*Ejemplo:*
```python
if leads_total_int == 0:
    raise ValueError("[VALIDATION [ERROR ❌]] Leads no puede ser cero.")
```

---

## 5. Estilo de Código y Formato

### 5.1. Formato y Espaciado

- **Indentación:** 4 espacios por nivel.
- **Espaciado entre funciones:** 2 líneas en blanco.
- **Líneas en Blanco:** Coloca una línea en blanco antes del `return` si hay lógica previa para mejorar la legibilidad.

### 5.2. Importaciones

- **Globales:** Colócalas al inicio del módulo.
- **Locales:** Importa dentro de la función solo si es necesario para reducir dependencias globales.

---

## 6. Seguimiento y Mensajes de Log usando únciamente print()

### 6.1. Mensajes Inmediatos

Utiliza `print(..., flush=True)` para asegurar que los mensajes se impriman sin retrasos.

### 6.2. Componentes del Mensaje

- **CABECERA:** Elemento opcional para resaltar el inicio de una sección clave del log. Mejora la legibilidad agrupando eventos relacionados.

Ejemplos:

```plaintext
🔹🔹🔹 [START ▶️] Carga de Datos 🔹🔹🔹
🔹🔹🔹 [PROCESSING 🔄] Transformando Datos 🔹🔹🔹
```

- **DESCRIPTOR:** Término en mayúsculas que describe claramente la acción o fase actual del proceso. Ejemplos habituales:

  - `FILE SEARCH`
  - `METADATA EXTRACTION`
  - `VALIDATION`
  - `PROCESSING`
  - `BACKUP`

- **STATE:** Estado específico del proceso con etiquetas estándar adaptadas al contexto:

  - **START ▶️:** Indica el inicio del proceso.
  - **INFO ℹ️:** Información intermedia relevante o puntual.
  - **SUCCESS ✅ / FINISHED ✅:** Finalización exitosa.
  - **WARNING ⚠️:** Situaciones que requieren atención sin interrumpir el flujo.
  - **ERROR ❌ / FAILED ❌:** Errores críticos que afectan la ejecución.

### Aplicación Dinámica

- Comienza cada proceso con `[<DESCRIPTOR> START ▶️]`.
- Usa `[<DESCRIPTOR> INFO ℹ️]` para información adicional relevante.
- Finaliza operaciones con `[<DESCRIPTOR> SUCCESS ✅]` o `[<DESCRIPTOR> FINISHED ✅]`.
- En errores críticos usa `[<DESCRIPTOR> ERROR ❌]` o `[<DESCRIPTOR> FAILED ❌]`.
- Usa `[<DESCRIPTOR> WARNING ⚠️]` para alertas sobre posibles problemas.
- Indica progreso mediante barras, porcentajes o contadores parciales/totales.
- Cierra cada proceso con métricas y estadísticas relevantes.

Aplicación Dinámica

### Ejemplo

```plaintext
🔹🔹🔹 [START ▶️] Proceso de Búsqueda de Archivos 🔹🔹🔹
[FILE SEARCH START ▶️] Iniciando búsqueda en la ruta especificada. (Progreso: 0%)
[PROGRESS] [█_________] 10.75% completado (50/465)
[LOCATED FILE INFO ℹ️] Archivo encontrado: video.mp4 (Ruta: /ruta/del/archivo). (Progreso: 25%)
[PROGRESS] [████______] 50.32% completado (234/465)
[LOCATED FILE INFO ℹ️] Archivo encontrado: audio.mp3 (Ruta: /ruta/del/archivo). (Progreso: 50%)
[PROGRESS] [███████___] 75.48% completado (351/465)
[LOCATED FILE INFO ℹ️] Archivo encontrado: imagen.jpg (Ruta: /ruta/del/archivo). (Progreso: 75%)
[FILE SEARCH SUCCESS ✅] Búsqueda finalizada. Archivos encontrados: 465. Tiempo empleado: 1m 23s. Tamaño total: 3.5 GB. (Progreso: 100%)

🔹🔹🔹 [METRICS 📊] Resumen de Ejecución 🔹🔹🔹
[METRICS INFO ℹ️] Resumen de ejecución:
  - Archivos procesados: 465
  - Tamaño total: 3.5 GB
  - Tiempo total de búsqueda: 1m 23s
  - Tiempo de procesamiento de metadatos: 45s
  - Advertencias detectadas: 1
[END FINISHED ✅] Tiempo total de ejecución: 2m 08s
```

---

## 7. Manejo de Errores

- **Bloques try-except:**  
  Envuelve operaciones críticas (descarga, importación dinámica, lectura de datos) para capturar y registrar errores.
- **Mensajes Claros:**  
  Acompaña los errores con el prefijo correspondiente (por ejemplo, `[EXTRACTION [ERROR ❌]]`) y detalles útiles.

---

## 8. Funciones Auxiliares (Subfunciones)

- **Definición Interna:**  
  Si la funcionalidad es específica de una función “madre”, define subfunciones dentro de ella.
- **Nomenclatura:**  
  Usa un guión bajo inicial (`_`) para indicar que son internas y no forman parte de la API pública.

*Ejemplo:*
```python
def load_custom_libs(config_list: list) -> None:
    def _download_module_from_github(module_path: str) -> str:
        # Lógica para descargar el módulo...
        return temp_file_path
    # Resto de la función...
```

---

## 9. Organización y Orden del Código

- **Orden Alfabético:**  
  Las funciones deben ordenarse alfabéticamente para facilitar la búsqueda y mantenimiento.
- **Agrupación Lógica:**  
  Organiza funciones relacionadas en módulos o secciones dentro del archivo.

---

## 10. Consideraciones Adicionales

- **Recarga de Módulos:**  
  En importaciones dinámicas, elimina versiones previas y usa `importlib.invalidate_caches()` para cargar la versión actual.
- **Gestión de Archivos Temporales:**  
  Utiliza la librería `tempfile` para manejar archivos descargados y asegúrate de su correcta eliminación o mantenimiento.
- **Zonas Horarias:**  
  Emplea `zoneinfo` (o `pytz`) para ajustar fechas según la zona horaria requerida (ej.: "Europe/Madrid").

---

## 11. Autenticación Dinámica con `ini_environment_identificated`

Esta estrategia permite que las funciones seleccionen automáticamente el método de acceso a las credenciales según el entorno:

- **Entorno LOCAL:**  
  - Valor: `"LOCAL"`  
  - Método: Usa `json_keyfile_local`.

- **Entorno GOOGLE COLAB:**  
  - Valor: `"COLAB"`  
  - Método: Usa `json_keyfile_colab`.

- **Entornos GCP (incluyendo COLAB_ENTERPRISE):**  
  - Valor: `"COLAB_ENTERPRISE"` o un `project_id` (por ejemplo, `"mi-proyecto"`)  
  - Método: Utiliza Secret Manager mediante `json_keyfile_GCP_secret_id`.  
    - En `"COLAB_ENTERPRISE"`, el `project_id` se extrae de la variable `GOOGLE_CLOUD_PROJECT`.

### Ejemplo de Configuración
```python
config = {
    "ini_environment_identificated": "COLAB",  # Opciones: "LOCAL", "COLAB", "COLAB_ENTERPRISE" o un project_id
    "json_keyfile_local": r"D://ruta/a/credenciales.json",    # Usado para "LOCAL"
    "json_keyfile_colab": "/ruta/a/credenciales.json",          # Usado para "COLAB"
    "json_keyfile_GCP_secret_id": "mi-secret-id",              # Usado para entornos GCP
    # Otros parámetros...
}
```

---

## 12. Ejemplo Completo de Función con Buenas Prácticas

Este ejemplo ilustra el proceso completo, desde la descarga (si es necesario) hasta la importación y reporte del módulo, incluyendo validaciones, mensajes de log y manejo de errores.

```python
# @title load_custom_libs()
def load_custom_libs(config_list: list) -> None:
    """
    Carga dinámicamente uno o varios módulos a partir de una lista de diccionarios de configuración.

    Cada diccionario debe incluir:
      - module_host (str): "GD" para rutas locales o "github" para archivos en GitHub.
      - module_path (str): Ruta local o URL al archivo .py.
      - selected_functions_list (list, opcional): Lista de nombres de funciones/clases a importar.
        Si está vacío, se importan todos los objetos definidos en el módulo.

    Para módulos alojados en GitHub, la URL se transforma a formato raw y se descarga en un archivo temporal.
    La fecha de última modificación se muestra ajustada a la zona horaria de Madrid.

    Returns:
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
