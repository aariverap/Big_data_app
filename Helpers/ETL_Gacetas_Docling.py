"""
Pipeline de extracción y procesamiento de Gacetas del Congreso
Extrae metadatos del nombre del archivo y contenido del PDF usando Docling
"""

from pathlib import Path
import json
import re
from typing import Dict, Optional
from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import PdfFormatOption


def configurar_converter():
    """
    Configura el DocumentConverter con opciones para OCR y tablas
    """
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = True
    pipeline_options.do_table_structure = True
    
    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options
            )
        }
    )
    
    return converter


def extraer_metadatos_nombre_archivo(nombre_archivo: str) -> Optional[Dict]:
    """
    Extrae metadatos del nombre del archivo de gaceta
    
    Formato esperado: "001_Camara_Gaceta1405_2025.pdf"
    
    Args:
        nombre_archivo: Nombre del archivo (con o sin extensión)
    
    Returns:
        Diccionario con id, corporacion, numeroGaceta, año
        None si el formato no es válido
    """
    # Remover extensión si existe
    nombre_sin_ext = Path(nombre_archivo).stem
    
    # Patrón: ID_Corporacion_GacetaNUMERO_AÑO
    # Ejemplo: 001_Camara_Gaceta1405_2025
    patron = r'^(\d+)_([^_]+)_Gaceta(\d+)_(\d{4})$'
    
    match = re.match(patron, nombre_sin_ext)
    
    if match:
        id_gaceta, corporacion, numero_gaceta, año = match.groups()
        return {
            "id": id_gaceta,
            "corporacion": corporacion,
            "numeroGaceta": numero_gaceta,
            "año": año
        }
    else:
        print(f"  ⚠ Advertencia: El nombre '{nombre_archivo}' no sigue el formato esperado")
        print(f"     Formato esperado: ID_Corporacion_GacetaNUMERO_AÑO.pdf")
        return None


def extraer_texto_pdf(pdf_path: str, converter: DocumentConverter) -> str:
    """
    Extrae el texto completo de un PDF usando Docling
    
    Args:
        pdf_path: Ruta al archivo PDF
        converter: Instancia de DocumentConverter configurada
    
    Returns:
        Texto completo extraído en formato markdown
    """
    result = converter.convert(pdf_path)
    texto_completo = result.document.export_to_markdown()
    return texto_completo


def procesar_gaceta(pdf_path: Path, converter: DocumentConverter) -> Optional[Dict]:
    """
    Procesa una gaceta individual: extrae metadatos del nombre y texto del PDF
    
    Args:
        pdf_path: Path al archivo PDF
        converter: Instancia de DocumentConverter
    
    Returns:
        Diccionario con toda la información de la gaceta
        None si hay error en el formato del nombre
    """
    # Extraer metadatos del nombre
    metadatos = extraer_metadatos_nombre_archivo(pdf_path.name)
    
    if metadatos is None:
        return None
    
    # Extraer texto del PDF
    print(f"  → Extrayendo texto del PDF...")
    texto_completo = extraer_texto_pdf(str(pdf_path), converter)
    
    # Combinar metadatos y texto
    gaceta_completa = {
        **metadatos,  # id, corporacion, numeroGaceta, año
        "texto_completo": texto_completo
    }
    
    return gaceta_completa


def procesar_carpeta_gacetas(carpeta_entrada: str = "camara", carpeta_salida: str = None):
    """
    Procesa todas las gacetas en una carpeta y exporta cada una a JSON
    
    Args:
        carpeta_entrada: Ruta a la carpeta con los PDFs (default: 'camara')
        carpeta_salida: Ruta donde guardar los JSON (opcional, por defecto mismo directorio)
    """
    carpeta_entrada = Path(carpeta_entrada)
    
    # Validar que existe la carpeta
    if not carpeta_entrada.exists():
        print(f"❌ Error: La carpeta '{carpeta_entrada}' no existe")
        return
    
    # Configurar carpeta de salida
    if carpeta_salida is None:
        carpeta_salida = carpeta_entrada
    else:
        carpeta_salida = Path(carpeta_salida)
        carpeta_salida.mkdir(parents=True, exist_ok=True)
    
    # Obtener todos los archivos PDF
    archivos_pdf = list(carpeta_entrada.glob("*.pdf"))
    
    if not archivos_pdf:
        print(f"❌ No se encontraron archivos PDF en '{carpeta_entrada}'")
        return
    
    print(f"\n{'='*70}")
    print(f"Pipeline de Procesamiento de Gacetas del Congreso")
    print(f"{'='*70}")
    print(f"📁 Carpeta de entrada: {carpeta_entrada}")
    print(f"📄 Archivos encontrados: {len(archivos_pdf)}")
    print(f"{'='*70}\n")
    
    # Configurar el converter una sola vez
    converter = configurar_converter()
    
    # Procesar cada PDF
    resultados = []
    exitosos = 0
    con_errores = 0
    
    for idx, pdf_path in enumerate(archivos_pdf, 1):
        print(f"[{idx}/{len(archivos_pdf)}] Procesando: {pdf_path.name}")
        
        try:
            # Procesar gaceta
            gaceta_data = procesar_gaceta(pdf_path, converter)
            
            if gaceta_data is None:
                con_errores += 1
                resultados.append({
                    "archivo": pdf_path.name,
                    "estado": "error",
                    "mensaje": "Formato de nombre incorrecto"
                })
                continue
            
            # Guardar JSON individual
            json_filename = f"{pdf_path.stem}.json"
            json_path = carpeta_salida / json_filename
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(gaceta_data, f, ensure_ascii=False, indent=2)
            
            exitosos += 1
            print(f"  ✓ Exportado a: {json_filename}")
            print(f"    - ID: {gaceta_data['id']}")
            print(f"    - Corporación: {gaceta_data['corporacion']}")
            print(f"    - Número de Gaceta: {gaceta_data['numeroGaceta']}")
            print(f"    - Año: {gaceta_data['año']}")
            print(f"    - Caracteres extraídos: {len(gaceta_data['texto_completo'])}")
            
            resultados.append({
                "archivo": pdf_path.name,
                "json_generado": json_filename,
                "estado": "exitoso",
                **{k: v for k, v in gaceta_data.items() if k != 'texto_completo'}
            })
            
        except Exception as e:
            con_errores += 1
            print(f"  ✗ Error procesando {pdf_path.name}: {str(e)}")
            resultados.append({
                "archivo": pdf_path.name,
                "estado": "error",
                "mensaje": str(e)
            })
        
        print()
    
    # Guardar resumen general
    resumen_path = carpeta_salida / "resumen_procesamiento_gacetas.json"
    resumen = {
        "total_archivos": len(archivos_pdf),
        "exitosos": exitosos,
        "con_errores": con_errores,
        "carpeta_origen": str(carpeta_entrada),
        "carpeta_destino": str(carpeta_salida),
        "detalles": resultados
    }
    
    with open(resumen_path, 'w', encoding='utf-8') as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)
    
    # Imprimir resumen final
    print(f"{'='*70}")
    print(f"📊 RESUMEN DEL PROCESAMIENTO")
    print(f"{'='*70}")
    print(f"✓ Exitosos: {exitosos}/{len(archivos_pdf)}")
    print(f"✗ Con errores: {con_errores}/{len(archivos_pdf)}")
    print(f"📋 Resumen guardado en: {resumen_path.name}")
    print(f"{'='*70}\n")


def procesar_gaceta_individual(pdf_path: str, json_path: str = None):
    """
    Procesa una única gaceta y la exporta a JSON
    
    Args:
        pdf_path: Ruta al archivo PDF
        json_path: Ruta del archivo JSON de salida (opcional)
    """
    pdf_path = Path(pdf_path)
    
    if not pdf_path.exists():
        print(f"❌ Error: El archivo '{pdf_path}' no existe")
        return
    
    if json_path is None:
        json_path = pdf_path.parent / f"{pdf_path.stem}.json"
    else:
        json_path = Path(json_path)
    
    print(f"Procesando gaceta: {pdf_path.name}\n")
    
    # Configurar y procesar
    converter = configurar_converter()
    gaceta_data = procesar_gaceta(pdf_path, converter)
    
    if gaceta_data is None:
        print("❌ No se pudo procesar la gaceta debido a formato incorrecto")
        return
    
    # Exportar a JSON
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(gaceta_data, f, ensure_ascii=False, indent=2)
    
    print(f"\n✓ Gaceta procesada exitosamente")
    print(f"📄 Archivo JSON: {json_path}")
    print(f"\nDatos extraídos:")
    print(f"  - ID: {gaceta_data['id']}")
    print(f"  - Corporación: {gaceta_data['corporacion']}")
    print(f"  - Número de Gaceta: {gaceta_data['numeroGaceta']}")
    print(f"  - Año: {gaceta_data['año']}")
    print(f"  - Caracteres de texto: {len(gaceta_data['texto_completo'])}")


if __name__ == "__main__":
    # EJEMPLO 1: Procesar todos los PDFs de la carpeta 'camara'
    procesar_carpeta_gacetas(
        carpeta_entrada="senado",
        carpeta_salida="senado_json"  # Opcional: carpeta diferente para los JSON
    )
    
    # EJEMPLO 2: Procesar una gaceta individual
    # procesar_gaceta_individual("camara/001_Camara_Gaceta1405_2025.pdf")
    
    # EJEMPLO 3: Procesar en la misma carpeta (sin carpeta de salida separada)
    # procesar_carpeta_gacetas("camara")