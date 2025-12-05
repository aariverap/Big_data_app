"""
Selenium scraper para Imprenta: descarga 50 filas por página 
navega páginas y guarda PDFs en carpetas por corporación (Senado / Cámara).

"""

import os
import time
import glob
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

BASE_URL = "https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml"  # URL de la Imprenta
OUTPUT_DIR = os.path.abspath("pdfs")
SENADO_DIR = os.path.join(OUTPUT_DIR, "Senado_de_la_Republica")
CAMARA_DIR = os.path.join(OUTPUT_DIR, "Camara_de_Representantes")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(SENADO_DIR, exist_ok=True)
os.makedirs(CAMARA_DIR, exist_ok=True)

def setup_driver():
    """Configura y retorna el driver de Chrome."""
    options = webdriver.ChromeOptions()
    options.headless = False
    
    # Configurar carpeta de descarga
    prefs = {
        "download.default_directory": OUTPUT_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
        "safebrowsing.disable_download_protection": True,
        "profile.default_content_settings.popups": 0,
        "profile.default_content_setting_values.automatic_downloads": 1,
    }
    options.add_experimental_option("prefs", prefs)
    
    # Argumentos adicionales
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("useAutomationExtension", False)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver
    except Exception as e:
        print(f"Error al inicializar el driver: {e}")
        return None

def wait_for_table_rows(driver, min_rows=1, timeout=15):
    """Espera hasta que la tabla tenga al menos min_rows filas."""
    end = time.time() + timeout
    while time.time() < end:
        try:
            rows = driver.find_elements(By.CSS_SELECTOR, "tbody[id*='dataTableResumen_data'] tr")
            if len(rows) >= min_rows:
                return rows
        except:
            pass
        time.sleep(0.3)
    
    try:
        return driver.find_elements(By.CSS_SELECTOR, "tbody[id*='dataTableResumen_data'] tr")
    except:
        return []

def change_page_size_to_50(driver, wait):
    """Intenta seleccionar '50' en el select de filas por página."""
    try:
        select_element = wait.until(EC.presence_of_element_located((By.ID, "formResumen:dataTableResumen_rppDD")))
        sel = Select(select_element)
        sel.select_by_value("50")
        time.sleep(2)  # Esperar a que se actualice la tabla
        rows = wait_for_table_rows(driver, min_rows=1, timeout=8)
        print(f"[info] filas actualmente en pantalla: {len(rows)}")
        return True
    except Exception as e:
        print(f"[warning] no se pudo seleccionar 50 con el control: {e}")
        return False

def get_total_pages(driver):
    """Obtiene el número total de páginas."""
    try:
        pages = driver.find_elements(By.CSS_SELECTOR, ".ui-paginator-pages .ui-paginator-page")
        if pages:
            return int(pages[-1].text.strip())
    except:
        pass
    return 1

def get_current_page(driver):
    """Obtiene la página actual."""
    try:
        active = driver.find_element(By.CSS_SELECTOR, ".ui-paginator-pages .ui-paginator-page.ui-state-active")
        return int(active.text.strip())
    except:
        return 1

def go_to_page(driver, target):
    """Navega hasta la página target."""
    for attempt in range(50):  # Reducido de 200 a 50
        cur = get_current_page(driver)
        if cur == target:
            return True
            
        # Intentar click en número visible
        try:
            pages = driver.find_elements(By.CSS_SELECTOR, ".ui-paginator-pages .ui-paginator-page")
            clicked = False
            for p in pages:
                if p.text.strip() == str(target):
                    driver.execute_script("arguments[0].click();", p)
                    time.sleep(1)
                    clicked = True
                    break
            if clicked:
                continue
        except:
            pass
            
        # Si no está visible, usar next o prev
        try:
            if cur < target:
                nxt = driver.find_element(By.CSS_SELECTOR, ".ui-paginator-next")
                driver.execute_script("arguments[0].click();", nxt)
            else:
                prev = driver.find_element(By.CSS_SELECTOR, ".ui-paginator-prev")
                driver.execute_script("arguments[0].click();", prev)
            time.sleep(1)
        except:
            time.sleep(0.5)
            
    print(f"[warning] No pude navegar a la página {target}")
    return False

def wait_for_complete_download(timeout=60):
    """Espera a que se complete la descarga."""
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            # Buscar archivos de descarga temporal
            temp_files = (glob.glob(os.path.join(OUTPUT_DIR, "*.crdownload")) + 
                         glob.glob(os.path.join(OUTPUT_DIR, "*.part")) + 
                         glob.glob(os.path.join(OUTPUT_DIR, "*.tmp")))
            
            # Si no hay archivos temporales, buscar PDFs recientes
            if not temp_files:
                pdf_files = glob.glob(os.path.join(OUTPUT_DIR, "*.pdf"))
                if pdf_files:
                    # Encontrar el archivo más reciente
                    newest_pdf = max(pdf_files, key=os.path.getctime)
                    
                    # Verificar que tenga un tamaño razonable
                    if os.path.getsize(newest_pdf) > 1024:
                        time.sleep(1)  # Esperar un poco más
                        return os.path.basename(newest_pdf)
        except Exception as e:
            print(f"[debug] Error en wait_for_complete_download: {e}")
        
        time.sleep(1)
    
    return None

def is_pdf_valid(filepath):
    """Verifica si un archivo PDF es válido."""
    try:
        if not os.path.exists(filepath):
            return False
        if os.path.getsize(filepath) < 100:  # Muy pequeño para ser un PDF válido
            return False
        with open(filepath, 'rb') as f:
            header = f.read(10)
            return header.startswith(b'%PDF-')
    except:
        return False

# Contador global para ID único
ID_COUNTER = 1

def generate_unique_id():
    """Genera un ID único secuencial de máximo 3 cifras (001-999)."""
    global ID_COUNTER
    if ID_COUNTER > 999:
        ID_COUNTER = 1  # Reiniciar si supera 999
    current_id = f"{ID_COUNTER:03d}"
    ID_COUNTER += 1
    return current_id

def extract_year_from_row(row):
    """Extrae el año de la fila de la tabla."""
    try:
        cols = row.find_elements(By.TAG_NAME, "td")
        # Buscar en diferentes columnas posibles donde podría estar la fecha
        for col in cols:
            text = col.text.strip()
            # Buscar patrones de año (YYYY o DD/MM/YYYY o similares)
            import re
            year_match = re.search(r'(20\d{2}|19\d{2})', text)
            if year_match:
                return year_match.group(1)
        # Si no encuentra año, usar el año actual
        return str(time.strftime("%Y"))
    except:
        return str(time.strftime("%Y"))

def download_rows_on_current_page(driver):
    """Descarga las filas de la página actual."""
    rows = wait_for_table_rows(driver, min_rows=1, timeout=10)
    print(f"[info] filas en esta vista: {len(rows)}")
    
    for idx, row in enumerate(rows):
        try:
            # Obtener número de gaceta
            try:
                numero = row.find_element(By.CSS_SELECTOR, "label").text.strip()
            except:
                numero = f"sin_numero_{idx+1}"
            
            # Obtener corporación
            try:
                cols = row.find_elements(By.TAG_NAME, "td")
                corporacion = cols[1].text.strip() if len(cols) >= 2 else "Desconocida"
            except:
                corporacion = "Desconocida"
            
            # Extraer año de la fila
            year = extract_year_from_row(row)
            
            # Generar ID único
            unique_id = generate_unique_id()
            
            # Determinar directorio y tipo de entidad ANTES de usarlo
            if "Senado" in corporacion:
                target_dir = SENADO_DIR
                entity_type = "Senado"
            elif "Cámara" in corporacion or "Camara" in corporacion:
                target_dir = CAMARA_DIR
                entity_type = "Camara"
            else:
                target_dir = OUTPUT_DIR
                entity_type = "Desconocida"
            
            # Buscar botón de descarga
            try:
                btn = row.find_element(By.CSS_SELECTOR, "button[title='Descargar Pdf']")
            except:
                print(f"[warn] no hay botón para gaceta {numero}")
                continue

            print(f"[info] Descargando gaceta {numero} ({entity_type}, {year})...")
            
            # Limpiar archivos temporales anteriores
            for temp_pattern in ["*.crdownload", "*.part", "*.tmp"]:
                for temp_file in glob.glob(os.path.join(OUTPUT_DIR, temp_pattern)):
                    try:
                        os.remove(temp_file)
                    except:
                        pass
            
            # Click en el botón
            driver.execute_script("arguments[0].click();", btn)
            
            # Esperar descarga
            new_file = wait_for_complete_download(timeout=60)
            
            if not new_file:
                print(f"[error] timeout descargando gaceta {numero}")
                continue
                
            src_path = os.path.join(OUTPUT_DIR, new_file)
            
            # Verificar PDF
            if not is_pdf_valid(src_path):
                print(f"[error] archivo PDF inválido para gaceta {numero}")
                try:
                    os.remove(src_path)
                except:
                    pass
                continue
            
            # Crear nombre final con el formato: ID_ENTIDAD_NUMERO_AÑO.pdf
            final_name = f"{unique_id}_{entity_type}_Gaceta{numero}_{year}.pdf"
            dest_path = os.path.join(target_dir, final_name)
            
            # Si existe, agregar timestamp al final
            if os.path.exists(dest_path):
                timestamp = int(time.time())
                base_name = f"{unique_id}_{entity_type}_Gaceta{numero}_{year}"
                final_name = f"{base_name}_{timestamp}.pdf"
                dest_path = os.path.join(target_dir, final_name)
            
            # Mover archivo
            try:
                os.replace(src_path, dest_path)
                print(f"✅ Guardado: {final_name}")
                print(f"   Ubicación: {dest_path}")
            except Exception as e:
                print(f"⚠️ Error moviendo archivo: {e}")

            time.sleep(2)  # Pausa entre descargas
            
        except Exception as e:
            print(f"[error] Error procesando fila {idx+1}: {e}")
            continue

def main():
    """Función principal."""
    global ID_COUNTER
    driver = None
    
    try:
        print("Iniciando scraper...")
        print(f"[info] ID iniciará desde: {ID_COUNTER:03d}")
        
        # Configurar driver
        driver = setup_driver()
        if not driver:
            print("❌ No se pudo inicializar el driver")
            return
        
        wait = WebDriverWait(driver, 20)
        
        # Ir a la página 
        if BASE_URL == "https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml":
            print("❌ ERROR: Debes cambiar BASE_URL por la URL correcta del sitio web")
            return
            
        print(f"Navegando a: {BASE_URL}")
        driver.get(BASE_URL)
        
        # Esperar a que cargue la tabla
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tbody[id*='dataTableResumen_data'] tr")))
        
        # Intentar cambiar a 50 filas por página
        change_page_size_to_50(driver, wait)
        
        # Obtener total de páginas
        total_pages = get_total_pages(driver)
        print(f"[info] total de páginas detectadas: {total_pages}")
        
        # Procesar páginas (limitado a 2 para pruebas)
        start_page = 1
        end_page = min(total_pages, 2)  # Cambiar por total_pages cuando esté funcionando
        
        print(f"[info] Procesando páginas {start_page} a {end_page}")
        
        for p in range(start_page, end_page + 1):
            print(f"\n=== Procesando página {p} / {end_page} ===")
            if go_to_page(driver, p):
                download_rows_on_current_page(driver)
            else:
                print(f"[error] No se pudo navegar a página {p}")
        
        print(f"\n✅ Proceso completado.")
        print(f"[info] Último ID usado: {ID_COUNTER-1:03d}")
        print(f"Archivos guardados en:")
        print(f"  - Senado: {SENADO_DIR}")
        print(f"  - Cámara: {CAMARA_DIR}")
        
    except Exception as e:
        print(f"❌ Error durante la ejecución: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if driver:
            print("Cerrando navegador...")
            driver.quit()

if __name__ == "__main__":

    main()


