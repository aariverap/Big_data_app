from elasticsearch import Elasticsearch
import traceback

class ElasticSearch:
    def __init__(self, cloud_url, api_key):
        """Inicializar conexión a Elasticsearch"""
        try:
            print(f"🔍 Intentando conectar a: {cloud_url[:50]}...")
            self.es = Elasticsearch(
                cloud_url,
                api_key=api_key,
                verify_certs=True,
                request_timeout=30
            )
            print("✅ Elasticsearch inicializado correctamente")
        except Exception as e:
            print(f"❌ Error al conectar con Elasticsearch: {e}")
            traceback.print_exc()
            self.es = None
    
    def test_connection(self):
        """Verificar conexión"""
        try:
            if self.es and self.es.ping():
                print("✅ Ping a Elasticsearch exitoso")
                return True
            print("❌ Ping a Elasticsearch falló")
            return False
        except Exception as e:
            print(f"❌ Error en test_connection: {e}")
            traceback.print_exc()
            return False
    
    def listar_indices(self):
        """Listar todos los índices con debugging mejorado"""
        print("\n" + "="*60)
        print("🔍 INICIANDO listar_indices()")
        print("="*60)
        
        try:
            # Verificar cliente
            if not self.es:
                print("❌ self.es es None - Cliente no inicializado")
                return {
                    'success': False,
                    'error': 'Cliente de Elasticsearch no inicializado',
                    'indices': []
                }
            
            print("✅ Cliente de Elasticsearch existe")
            
            # Verificar conexión
            try:
                if not self.es.ping():
                    print("❌ Ping falló - Elasticsearch no responde")
                    return {
                        'success': False,
                        'error': 'Elasticsearch no responde al ping',
                        'indices': []
                    }
                print("✅ Ping exitoso")
            except Exception as e:
                print(f"❌ Error en ping: {e}")
                return {
                    'success': False,
                    'error': f'Error de conexión: {str(e)}',
                    'indices': []
                }
            
            # Método 1: cat.indices
            print("\n📋 Intentando método cat.indices...")
            try:
                indices_info = self.es.cat.indices(format='json')
                print(f"✅ cat.indices devolvió {len(indices_info)} índices totales")
                
                # Formatear y filtrar
                indices = []
                for idx in indices_info:
                    nombre_indice = idx.get('index', '')
                    print(f"   - Procesando: {nombre_indice}")
                    
                    # Filtrar índices del sistema
                    if not nombre_indice.startswith('.'):
                        indices.append({
                            'nombre': nombre_indice,
                            'salud': idx.get('health', 'unknown'),
                            'estado': idx.get('status', 'unknown'),
                            'documentos': idx.get('docs.count', '0'),
                            'tamaño': idx.get('store.size', '0b')
                        })
                
                print(f"✅ Devolviendo {len(indices)} índices de usuario")
                
                if len(indices) == 0:
                    print("⚠️  No se encontraron índices de usuario (todos son del sistema)")
                    return {
                        'success': True,
                        'total': 0,
                        'indices': [],
                        'mensaje': 'No hay índices creados aún. Los índices del sistema están ocultos.'
                    }
                
                return {
                    'success': True,
                    'total': len(indices),
                    'indices': indices
                }
                
            except Exception as e1:
                print(f"⚠️  cat.indices falló: {type(e1).__name__} - {str(e1)}")
                traceback.print_exc()
                
                # Método 2: Fallback con indices.get_alias
                print("\n📋 Intentando método alternativo indices.get_alias...")
                try:
                    indices_dict = self.es.indices.get_alias(index="*")
                    print(f"✅ get_alias devolvió {len(indices_dict)} índices")
                    
                    indices = []
                    for nombre_indice in indices_dict.keys():
                        print(f"   - Procesando: {nombre_indice}")
                        
                        if not nombre_indice.startswith('.'):
                            # Intentar obtener stats
                            try:
                                stats = self.es.indices.stats(index=nombre_indice)
                                doc_count = stats['indices'][nombre_indice]['total']['docs']['count']
                                size = stats['indices'][nombre_indice]['total']['store']['size_in_bytes']
                                size_str = f"{size / (1024**2):.2f}mb" if size > 0 else "0b"
                            except:
                                doc_count = 0
                                size_str = "unknown"
                            
                            indices.append({
                                'nombre': nombre_indice,
                                'salud': 'unknown',
                                'estado': 'open',
                                'documentos': str(doc_count),
                                'tamaño': size_str
                            })
                    
                    print(f"✅ Método alternativo devolvió {len(indices)} índices")
                    
                    if len(indices) == 0:
                        return {
                            'success': True,
                            'total': 0,
                            'indices': [],
                            'mensaje': 'No hay índices creados aún'
                        }
                    
                    return {
                        'success': True,
                        'total': len(indices),
                        'indices': indices
                    }
                    
                except Exception as e2:
                    print(f"❌ Método alternativo también falló: {type(e2).__name__} - {str(e2)}")
                    traceback.print_exc()
                    raise e2
            
        except Exception as e:
            print(f"\n❌ ERROR FATAL en listar_indices:")
            print(f"   Tipo: {type(e).__name__}")
            print(f"   Mensaje: {str(e)}")
            traceback.print_exc()
            
            return {
                'success': False,
                'error': f'{type(e).__name__}: {str(e)}',
                'indices': []
            }
        finally:
            print("="*60)
            print("FIN listar_indices()")
            print("="*60 + "\n")
    
    def buscar(self, index, query, aggs=None, size=10):
        """Realizar búsqueda en Elasticsearch"""
        try:
            if not self.es:
                return {
                    'success': False,
                    'error': 'Cliente de Elasticsearch no inicializado',
                    'total': 0,
                    'resultados': [],
                    'aggs': {}
                }
            
            # Construir el cuerpo de la búsqueda
            body = {"size": size}
            
            # Agregar query
            if query and 'query' in query:
                body['query'] = query['query']
            else:
                body['query'] = {"match_all": {}}
            
            # Agregar agregaciones si existen
            if aggs:
                body["aggs"] = aggs
            
            print(f"🔍 Ejecutando búsqueda en '{index}'")
            
            # Ejecutar búsqueda
            result = self.es.search(index=index, body=body)
            
            # Formatear resultados
            resultados = []
            for hit in result['hits']['hits']:
                resultados.append({
                    '_id': hit['_id'],
                    '_score': hit['_score'],
                    '_source': hit['_source']
                })
            
            response = {
                'success': True,
                'total': result['hits']['total']['value'],
                'resultados': resultados
            }
            
            # Agregar agregaciones si existen
            if 'aggregations' in result:
                response['aggs'] = result['aggregations']
            
            print(f"✅ Búsqueda completada: {response['total']} resultados")
            return response
            
        except Exception as e:
            print(f"❌ Error en buscar: {e}")
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e),
                'total': 0,
                'resultados': [],
                'aggs': {}
            }
    
    def ejecutar_query(self, query_json):
        """Ejecutar una query personalizada"""
        try:
            if not self.es:
                return {'success': False, 'error': 'Cliente no inicializado'}
            
            import json
            query = json.loads(query_json) if isinstance(query_json, str) else query_json
            
            index = query.pop('index', '_all')
            result = self.es.search(index=index, body=query)
            
            return {'success': True, 'result': result}
        except Exception as e:
            print(f"❌ Error en ejecutar_query: {e}")
            return {'success': False, 'error': str(e)}
    
    def indexar_bulk(self, index, documentos):
        """Indexar múltiples documentos"""
        try:
            if not self.es:
                return {
                    'success': False,
                    'error': 'Cliente no inicializado',
                    'indexados': 0,
                    'fallidos': len(documentos)
                }
            
            from elasticsearch.helpers import bulk
            
            actions = [
                {"_index": index, "_source": doc}
                for doc in documentos
            ]
            
            print(f"📤 Indexando {len(actions)} documentos en '{index}'")
            
            success, failed = bulk(self.es, actions, raise_on_error=False)
            
            print(f"✅ Indexados: {success}, Fallidos: {len(failed) if failed else 0}")
            
            return {
                'success': True,
                'indexados': success,
                'fallidos': len(failed) if failed else 0
            }
        except Exception as e:
            print(f"❌ Error en indexar_bulk: {e}")
            return {
                'success': False,
                'error': str(e),
                'indexados': 0,
                'fallidos': len(documentos)
            }
    
    def crear_indice(self, nombre_indice, mapping=None):
        """Crear un nuevo índice"""
        try:
            if not self.es:
                return {'success': False, 'error': 'Cliente no inicializado'}
            
            if self.es.indices.exists(index=nombre_indice):
                return {'success': False, 'error': f'El índice {nombre_indice} ya existe'}
            
            body = {}
            if mapping:
                body['mappings'] = mapping
            
            self.es.indices.create(index=nombre_indice, body=body)
            print(f"✅ Índice '{nombre_indice}' creado")
            
            return {'success': True, 'mensaje': f'Índice {nombre_indice} creado correctamente'}
        except Exception as e:
            print(f"❌ Error en crear_indice: {e}")
            return {'success': False, 'error': str(e)}
    
    def eliminar_indice(self, nombre_indice):
        """Eliminar un índice"""
        try:
            if not self.es:
                return {'success': False, 'error': 'Cliente no inicializado'}
            
            if not self.es.indices.exists(index=nombre_indice):
                return {'success': False, 'error': f'El índice {nombre_indice} no existe'}
            
            self.es.indices.delete(index=nombre_indice)
            print(f"✅ Índice '{nombre_indice}' eliminado")
            
            return {'success': True, 'mensaje': f'Índice {nombre_indice} eliminado correctamente'}
        except Exception as e:
            print(f"❌ Error en eliminar_indice: {e}")
            return {'success': False, 'error': str(e)}