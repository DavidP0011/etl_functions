!pip install PyPDF2

from PyPDF2 import PdfReader, PdfWriter

def intercalar_pdfs(impares_path, pares_path, output_path):
    # Cargar los PDFs
    reader_impares = PdfReader(impares_path)
    reader_pares = PdfReader(pares_path)
    
    # Crear un objeto PdfWriter para el resultado
    writer = PdfWriter()
    
    # Número de páginas en impares (se espera que coincida con pares)
    num_impares = len(reader_impares.pages)
    num_pares = len(reader_pares.pages)
    
    if num_impares != num_pares:
        print("Error: El número de páginas en 'impares.pdf' y 'pares.pdf' no coincide.")
        return
    
    # Extraer y revertir las páginas de 'pares.pdf'
    pares_paginas = list(reader_pares.pages)[::-1]
    
    # Intercalar páginas: una de impares, una de pares
    for i in range(num_impares):
        writer.add_page(reader_impares.pages[i])
        writer.add_page(pares_paginas[i])
    
    # Escribir el PDF resultante
    with open(output_path, "wb") as f:
        writer.write(f)
    
    print(f"PDF intercalado guardado en: {output_path}")

# Ejemplo de uso:
# Suponiendo que tienes "impares.pdf" y "pares.pdf" en el directorio actual
intercalar_pdfs("impares.pdf", "pares.pdf", "documento_completo.pdf")
