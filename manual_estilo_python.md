# Manual de Estilo para Funciones DPM en Python

Este manual ofrece una guía para escribir funciones en Python de forma **clara, modular, trazable y consistente**. Su objetivo es facilitar el mantenimiento, la reutilización y la depuración del código.

---

## 1. Introducción y Objetivos

- **Propósito:** Establecer directrices para la creación de funciones que sean fáciles de mantener, reutilizar y depurar.
- **Enfoque:** Código limpio, validaciones tempranas, mensajes de log estandarizados y organización modular.
- **Eficiencia:** Revisa la función e indica en tus respuestas si hay alguna funcionalidad sin uso o redundante, o keys de config que no tengan uso en la función.

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
  - **PROCESSING 🔄:** Actualmente procesando.
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
# @title function_example_str()
def function_example_str(config: dict) -> str:
    """
    Procesa un archivo de texto y genera un resumen en forma de cadena.

    Args:
        config (dict):
            - file_path (str): Ruta del archivo a procesar.
            - uppercase (bool, opcional): Si es True, convierte las líneas procesadas a mayúsculas (default: False).

    Returns:
        str: Resumen del procesamiento que incluye la ruta del archivo, total de líneas leídas y procesadas.

    Raises:
        ValueError: Si falta el parámetro 'file_path' en config o si ocurre un error al abrir el archivo.
    """
    # Validación inicial de parámetros
    file_path_str = config.get('file_path')
    if not file_path_str:
        raise ValueError("[VALIDATION [ERROR ❌]] Falta 'file_path' en config.")

    print("🔹🔹🔹 [START ▶️] Proceso de lectura y procesamiento de archivo 🔹🔹🔹", flush=True)

    # Subfunción para leer el archivo
    def _read_file(path: str) -> list:
        try:
            with open(path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
            print(f"[FILE READ SUCCESS ✅] Archivo '{path}' leído correctamente. Total líneas: {len(lines)}", flush=True)
            return lines
        except Exception as e:
            raise ValueError(f"[FILE PROCESS ERROR ❌] No se pudo abrir el archivo: {e}")

    # Subfunción para procesar las líneas del archivo
    def _process_file(lines: list, config: dict) -> (list, int):
        # Función auxiliar interna para limpiar cada línea
        def _clean_line_str(line: str) -> str:
            return line.strip()

        processed_lines_list = []
        total_lines_int = len(lines)
        for idx, line in enumerate(lines):
            cleaned_line_str = _clean_line_str(line)
            if cleaned_line_str:  # Solo considerar líneas no vacías
                processed_lines_list.append(cleaned_line_str)
            # Informar progreso cada 10% o cada cierto número de líneas
            if total_lines_int > 0 and (idx + 1) % max(1, total_lines_int // 10) == 0:
                progreso_int = int(((idx + 1) / total_lines_int) * 100)
                print(f"[PROCESSING 🔄] Progreso: {progreso_int}% completado", flush=True)

        # Aplicar transformación opcional: conversión a mayúsculas
        if config.get("uppercase", False):
            processed_lines_list = [line.upper() for line in processed_lines_list]
            print("[PROCESSING INFO ℹ️] Conversión a mayúsculas aplicada a las líneas procesadas.", flush=True)

        return processed_lines_list, total_lines_int

    # Llamada a la subfunción para leer el archivo
    lines_list = _read_file(file_path_str)
    # Llamada a la subfunción para procesar las líneas del archivo
    processed_lines_list, total_lines_int = _process_file(lines_list, config)

    # Bloque try/except para volcar el resultado con información detallada del proceso
    try:
        summary_str = (
            f"Resumen del archivo:\n"
            f"- Ruta: {file_path_str}\n"
            f"- Total líneas leídas: {total_lines_int}\n"
            f"- Líneas procesadas (no vacías): {len(processed_lines_list)}\n"
        )
        print("🔹🔹🔹 [FILE PROCESS FINISHED ✅] Procesamiento completado. 🔹🔹🔹", flush=True)
    except Exception as e:
        print(f"[SUMMARY ERROR ❌] Ocurrió un error al generar el resumen: {e}", flush=True)
        raise e

    return summary_str


```
