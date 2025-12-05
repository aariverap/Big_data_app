from Helpers import ElasticSearch
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()

ELASTIC_CLOUD_URL = os.getenv('ELASTIC_CLOUD_URL', '')
ELASTIC_API_KEY = os.getenv('ELASTIC_API_KEY', '')
ELASTIC_INDEX_DEFAULT = os.getenv('ELASTIC_INDEX_DEFAULT', 'index_gacetas')

# Conectar
elastic = ElasticSearch(ELASTIC_CLOUD_URL, ELASTIC_API_KEY)

print("="*60)
print("CARGANDO GACETAS A ELASTICSEARCH")
print("="*60)

# Datos de las gacetas (las 2 que tienes en el notebook)
gacetas = [
    {
        "id": "002",
        "corporacion": "Camara",
        "numeroGaceta": 1540,
        "año": "2025",
        "texto_completo": """AÑO XXXIV - Nº 1540

DIRECTORES:

## REPÚBLICA   DE   COLOMBIA

## G a c e t a   d e l   C o n g

## SENADO Y CÁMARA

(Artículo 36,  Ley 5ª de 1992)

IMPRENTA   NACIONAL   DE   COLOMBIA www.imprenta.gov.co

Bogotá, D. C., miércoles, 27 de agosto de 2025

## DIEGO ALEJANDRO GONZÁLEZ GONZÁLEZ

SECRETARIO  GENERAL  DEL  SENADO

www.secretariasenado.gov.co

I S S N  0 1 2 3  -  9 0 6 6

EDICIÓN  DE  16  PÁGINAS

JAIME LUIS LACOUTURE PEÑALOZA

SECRETARIO  GENERAL  DE  LA  CÁMARA

www.camara.gov.co

RAMA  LEGISLATIVA  DEL  PODER  PÚBLICO

## C Á M A R A   D E   R E P R E S E N T A N T E S

## I N F O R M E S   D E   C O N C I L I A C I Ó N

## INFORME DE CONCILIACIÓN DEL PROYECTO DE LEY NÚMERO 251 DE 2024 CÁMARA, 369 DE 2024 SENADO

por medio de la cual se rinde honores a la memoria y obra del expresidente José María Rojas Garrido en el bicentenario de su natalicio."""
    },
    {
        "id": "003",
        "corporacion": "Camara",
        "numeroGaceta": 1403,
        "año": "2025",
        "texto_completo": """AÑO XXXIV - Nº 1403

DIRECTORES:

## REPÚBLICA   DE   COLOMBIA

## G a c e t a   d e l   C o n g

## SENADO Y CÁMARA

(Artículo 36,  Ley 5ª de 1992)

IMPRENTA   NACIONAL   DE   COLOMBIA www.imprenta.gov.co

Bogotá, D. C., jueves, 14 de agosto de 2025

## DIEGO ALEJANDRO GONZÁLEZ GONZÁLEZ

SECRETARIO  GENERAL  DEL  SENADO

www.secretariasenado.gov.co

I S S N  0 1 2 3  -  9 0 6 6

EDICIÓN  DE  23  PÁGINAS

JAIME LUIS LACOUTURE PEÑALOZA

SECRETARIO  GENERAL  DE  LA  CÁMARA

www.camara.gov.co

RAMA  LEGISLATIVA  DEL  PODER  PÚBLICO

## C Á M A R A   D E   R E P R E S E N T A N T E S P R O Y E C T O S   D E   L E Y   E S TAT U TA R I A

## PROYECTO DE LEY ESTATUTARIA NÚMERO 101 DE 2025 CÁMARA

por medio de la cual se adoptan y fortalecen medidas de protección para víctimas de violencias basadas en género."""
    }
]

print(f"\n📝 Preparando {len(gacetas)} gacetas para indexar...")

# Indexar documentos
resultado = elastic.indexar_bulk(ELASTIC_INDEX_DEFAULT, gacetas)

print("\n" + "="*60)
if resultado['success']:
    print("✅ INDEXACIÓN COMPLETADA CON ÉXITO")
    print(f"   📊 Documentos indexados: {resultado['indexados']}")
    print(f"   ❌ Errores: {resultado['fallidos']}")
    
    if resultado['fallidos'] > 0:
        print("\n⚠️  Detalles de errores:")
        for error in resultado.get('errores', []):
            print(f"   - {error}")
else:
    print("❌ ERROR EN LA INDEXACIÓN")
    print(f"   Error: {resultado.get('error', 'Desconocido')}")

print("="*60)

# Refrescar el índice para que los documentos sean inmediatamente buscables
print("\n🔄 Refrescando índice...")
elastic.client.indices.refresh(index=ELASTIC_INDEX_DEFAULT)
print("✅ Índice refrescado")

# Verificar que se cargaron los documentos
print("\n🔍 Verificando documentos cargados...")
try:
    resultado_busqueda = elastic.buscar(
        index=ELASTIC_INDEX_DEFAULT,
        query={"match_all": {}},
        size=10
    )
    
    if resultado_busqueda['success']:
        print(f"✅ Total de documentos en el índice: {resultado_busqueda['total']}")
        print("\n📄 Documentos encontrados:")
        for i, doc in enumerate(resultado_busqueda['resultados'], 1):
            source = doc['_source']
            print(f"\n   {i}. Gaceta {source['numeroGaceta']} - {source['corporacion']}")
            print(f"      Año: {source['año']}")
            print(f"      ID: {source['id']}")
            texto_preview = source['texto_completo'][:100].replace('\n', ' ')
            print(f"      Texto: {texto_preview}...")
    else:
        print(f"❌ Error al verificar: {resultado_busqueda.get('error', 'Desconocido')}")
        
except Exception as e:
    print(f"❌ Error en la verificación: {e}")

# Cerrar conexión
elastic.close()
print("\n✅ Conexión cerrada")
print("="*60)