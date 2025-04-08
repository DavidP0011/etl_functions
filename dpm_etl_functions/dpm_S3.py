# __________________________________________________________________________________________________________________________________________________________
# S3_folder_and_files_list
# __________________________________________________________________________________________________________________________________________________________
def S3_folder_and_files_list(params: dict) -> dict:
    """
    Lista todos los archivos y subcarpetas a partir de una carpeta específica en un bucket de s3.

    Args:
        params (dict): Diccionario con las claves:
            - S3_bucket_name (str): Nombre del bucket de s3.
            - S3_folder_path (str): Ruta de la carpeta en el bucket de s3.

    Returns:
        dict: Diccionario con la estructura de carpetas y archivos:
            {
                'folders': {
                    'subcarpeta1/': {
                        'files': [lista de archivos en subcarpeta1],
                        'folders': {estructura recursiva de subcarpetas}
                    }
                },
                'files': [lista de archivos en la carpeta actual]
            }

    Raises:
        ValueError: Si falta algún parámetro en params.
        Exception: Si ocurre un error al listar los objetos.
    """
    import boto3
    import logging

    # Configurar el registro
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Validar parámetros
    S3_bucket_name = params.get('S3_bucket_name')
    S3_folder_path = params.get('S3_folder_path')

    if not S3_bucket_name or not S3_folder_path:
        raise ValueError("Faltan parámetros requeridos: 'S3_bucket_name' o 'S3_folder_path'.")

    try:
        # Cliente de s3
        s3_client = boto3.client('s3')

        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=S3_bucket_name, Prefix=S3_folder_path)

        structure = {'folders': {}, 'files': []}

        # Procesar los objetos obtenidos
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('/'):
                    # Es una carpeta
                    relative_path = key[len(S3_folder_path):]
                    if '/' not in relative_path.strip('/'):
                        structure['folders'][key] = {
                            'files': [],
                            'folders': {}
                        }
                else:
                    # Es un archivo
                    relative_path = key[len(S3_folder_path):]
                    if '/' not in relative_path:
                        structure['files'].append(key)

        return structure

    except Exception as e:
        logger.error(f"Error al listar objetos en s3: {e}")
        raise
