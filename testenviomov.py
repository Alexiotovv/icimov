import os
import time
import json
import requests
from datetime import datetime
import logging
import sys

# Importar dbfread
from dbfread import DBF

# Configuración
API_URL = "http://localhost:8000/api/movimientos"
RUTA_DBF = "C:/Users/Equipo/Desktop/ICI_Archivos/6. db_Productos"
INTERVALO_SEGUNDOS = 300  # 5 minutos
LOG_FILE = "envio_movimientos.log"
LOG_LEVEL = "INFO"

# Configurar logging - sin emojis
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

class EnvioMovimientos:
    def __init__(self, ruta_dbf, api_url):
        self.ruta_dbf = ruta_dbf
        self.api_url = api_url
        self.ultimo_envio = None
        
        # Verificar que la ruta existe
        if not os.path.exists(ruta_dbf):
            logging.error(f"La ruta {ruta_dbf} no existe")
            sys.exit(1)
    
    def leer_dbf_movimientos(self, archivo):
        """Leer archivo DBF de movimientos usando dbfread"""
        movimientos = []
        try:
            logging.info(f"Intentando leer {archivo}")
            # Usar dbfread
            table = DBF(archivo, encoding='latin-1', ignore_missing_memofile=True)
            
            # Obtener nombres de campos para debug
            campos = [field.name for field in table.fields]
            logging.info(f"Campos encontrados en {os.path.basename(archivo)}: {campos}")
            
            contador = 0
            for record in table:
                contador += 1
                movimiento = {
                    'movcoditip': self.limpiar_valor(record.get('movcoditip')),
                    'movnumero': self.limpiar_valor(record.get('movnumero')),
                    'almcodiorg': self.limpiar_valor(record.get('almcodiorg')),
                    'almorgvir': self.limpiar_valor(record.get('almorgvir')),
                    'almcodidst': self.limpiar_valor(record.get('almcodidst')),
                    'almdstvir': self.limpiar_valor(record.get('almdstvir')),
                    'movtipodci': self.limpiar_valor(record.get('movtipodci')),
                    'movnumedci': self.limpiar_valor(record.get('movnumedci')),
                    'movtipodco': self.limpiar_valor(record.get('movtipodco')),
                    'movnumedco': self.limpiar_valor(record.get('movnumedco')),
                    'cctcodigo': self.limpiar_valor(record.get('cctcodigo')),
                    'movtot': self.limpiar_valor(record.get('movtot'), es_numero=True),
                    'prvnumeruc': self.limpiar_valor(record.get('prvnumeruc')),
                    'prvdescrip': self.limpiar_valor(record.get('prvdescrip')),
                    'movrefe': self.limpiar_valor(record.get('movrefe')),
                    'movfechult': self.formatear_fecha(record.get('movfechult')),
                    'movsitua': self.limpiar_valor(record.get('movsitua')),
                    'tip_comp': self.limpiar_valor(record.get('tip_comp')),
                    'tip_proc': self.limpiar_valor(record.get('tip_proc')),
                    'num_proc': self.limpiar_valor(record.get('num_proc')),
                    'movfecanul': self.limpiar_valor(record.get('movfecanul')),
                }
                # Solo agregar si tiene movnumero
                if movimiento['movnumero'] and str(movimiento['movnumero']).strip():
                    movimientos.append(movimiento)
                
                # Debug: mostrar primeros 5 registros
                if contador <= 5:
                    logging.debug(f"Registro {contador}: movnumero={movimiento['movnumero']}, movtot={movimiento['movtot']}")
            
            logging.info(f"Leidos {len(movimientos)} movimientos de {os.path.basename(archivo)} (total registros: {contador})")
            
        except Exception as e:
            logging.error(f"Error al leer {archivo}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
        
        return movimientos
    
    def leer_dbf_detalles(self, archivo):
        """Leer archivo DBF de detalles usando dbfread"""
        detalles = []
        try:
            logging.info(f"Intentando leer {archivo}")
            # Usar dbfread
            table = DBF(archivo, encoding='latin-1', ignore_missing_memofile=True)
            
            # Obtener nombres de campos para debug
            campos = [field.name for field in table.fields]
            logging.info(f"Campos encontrados en {os.path.basename(archivo)}: {campos}")
            
            contador = 0
            for record in table:
                contador += 1
                detalle = {
                    'movcoditip': self.limpiar_valor(record.get('movcoditip')),
                    'movnumero': self.limpiar_valor(record.get('movnumero')),
                    'medcod': self.limpiar_valor(record.get('medcod')),
                    'medlote': self.limpiar_valor(record.get('medlote')),
                    'medfechvto': self.formatear_fecha(record.get('medfechvto')),
                    'movcantid': self.limpiar_valor(record.get('movcantid'), es_numero=True),
                    'movprecio': self.limpiar_valor(record.get('movprecio'), es_numero=True),
                    'movtotal': self.limpiar_valor(record.get('movtotal'), es_numero=True),
                    'movfechult': self.formatear_fecha(record.get('movfechult')),
                    'movsitua': self.limpiar_valor(record.get('movsitua')),
                }
                # Solo agregar si tiene movnumero y medcod
                if detalle['movnumero'] and str(detalle['movnumero']).strip() and detalle['medcod']:
                    detalles.append(detalle)
                
                # Debug: mostrar primeros 5 registros
                if contador <= 5:
                    logging.debug(f"Registro {contador}: movnumero={detalle['movnumero']}, medcod={detalle['medcod']}, cant={detalle['movcantid']}")
            
            logging.info(f"Leidos {len(detalles)} detalles de {os.path.basename(archivo)} (total registros: {contador})")
            
        except Exception as e:
            logging.error(f"Error al leer {archivo}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
        
        return detalles
    
    def limpiar_valor(self, valor, es_numero=False):
        """Limpiar y formatear valores"""
        if valor is None:
            return None
        
        # Si es bytes, decodificar
        if isinstance(valor, bytes):
            try:
                valor = valor.decode('latin-1').strip()
            except:
                valor = str(valor).strip()
        
        # Convertir a string y limpiar
        if not isinstance(valor, (int, float)):
            valor = str(valor).strip()
            if valor == '' or valor == 'None' or valor == 'nan':
                return None
        
        # Convertir a número si es necesario
        if es_numero and valor is not None:
            try:
                return float(valor)
            except (ValueError, TypeError):
                return None
        
        return valor
    
    def formatear_fecha(self, fecha):
        """Formatear fecha para enviar a la API"""
        if fecha is None:
            return None
        
        # Si es datetime o date object
        if hasattr(fecha, 'isoformat'):
            return fecha.isoformat()
        
        # Si es string, intentar parsear
        if isinstance(fecha, str):
            # Limpiar espacios
            fecha = fecha.strip()
            if not fecha:
                return None
            
            # Intentar diferentes formatos comunes
            formatos = [
                '%Y-%m-%d',  # 2024-01-31
                '%Y%m%d',    # 20240131
                '%d/%m/%Y',  # 31/01/2024
                '%d/%m/%Y %H:%M:%S',  # 31/01/2024 14:30:00
                '%d/%m/%Y %I:%M:%S %p',  # 31/01/2024 02:30:00 PM
                '%Y-%m-%d %H:%M:%S',  # 2024-01-31 14:30:00
            ]
            
            for fmt in formatos:
                try:
                    fecha_obj = datetime.strptime(fecha, fmt)
                    return fecha_obj.isoformat()
                except ValueError:
                    continue
        
        return str(fecha) if fecha else None
    
    def enviar_datos_completo(self, movimientos, detalles):
        """Enviar ambos tipos de datos en una sola peticion"""
        try:
            payload = {}
            if movimientos:
                payload['movimientos'] = movimientos
            if detalles:
                payload['detalles'] = detalles
            
            if not payload:
                return True
            
            url = f"{self.api_url}/store-completo"
            logging.info(f"Enviando datos a {url}")
            logging.info(f"Movimientos: {len(movimientos)}, Detalles: {len(detalles)}")
            
            response = requests.post(url, json=payload, timeout=30)
            
            if response.status_code == 200:
                resultado = response.json()
                logging.info(f"Envio exitoso: {resultado.get('message', 'OK')}")
                if 'data' in resultado:
                    data = resultado['data']
                    if 'movimientos' in data:
                        mov_data = data['movimientos']['data']
                        logging.info(f"  Movimientos - Guardados: {mov_data.get('guardados', 0)}, Actualizados: {mov_data.get('actualizados', 0)}")
                    if 'detalles' in data:
                        det_data = data['detalles']['data']
                        logging.info(f"  Detalles - Guardados: {det_data.get('guardados', 0)}, Actualizados: {det_data.get('actualizados', 0)}")
                return True
            else:
                logging.error(f"Error {response.status_code}: {response.text}")
                return False
                
        except Exception as e:
            logging.error(f"Error en envio: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return False
    
    def procesar_archivos(self):
        """Procesar archivos DBF en la ruta especificada"""
        movimientos = []
        detalles = []
        
        archivos_encontrados = False
        
        # Listar archivos en la ruta
        for archivo in os.listdir(self.ruta_dbf):
            archivo_lower = archivo.lower()
            
            if archivo_lower == 'tmovim.dbf':
                archivos_encontrados = True
                ruta = os.path.join(self.ruta_dbf, archivo)
                movimientos = self.leer_dbf_movimientos(ruta)
            elif archivo_lower == 'tmovimdet.dbf':
                archivos_encontrados = True
                ruta = os.path.join(self.ruta_dbf, archivo)
                detalles = self.leer_dbf_detalles(ruta)
        
        if not archivos_encontrados:
            logging.warning("No se encontraron archivos tmovim.dbf o tmovimdet.dbf")
            return
        
        if movimientos or detalles:
            logging.info(f"Procesando {len(movimientos)} movimientos y {len(detalles)} detalles")
            
            # Enviar datos completos
            self.enviar_datos_completo(movimientos, detalles)
            self.ultimo_envio = datetime.now()
        else:
            logging.info("No hay datos nuevos para enviar")
    
    def iniciar_monitoreo(self, intervalo):
        """Iniciar monitoreo continuo"""
        logging.info("Iniciando monitoreo de archivos DBF")
        logging.info(f"Ruta: {self.ruta_dbf}")
        logging.info(f"Intervalo: {intervalo} segundos ({intervalo/60:.1f} minutos)")
        logging.info(f"API URL: {self.api_url}")
        logging.info("-" * 50)
        
        contador = 0
        while True:
            try:
                contador += 1
                logging.info(f"Ciclo #{contador} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                self.procesar_archivos()
                
                logging.info(f"Esperando {intervalo} segundos...")
                time.sleep(intervalo)
                
            except KeyboardInterrupt:
                logging.info("Monitoreo detenido por el usuario")
                break
            except Exception as e:
                logging.error(f"Error en ciclo: {str(e)}")
                import traceback
                logging.error(traceback.format_exc())
                time.sleep(60)

def test_single_read():
    """Funcion para probar la lectura de archivos"""
    print("=" * 60)
    print("PRUEBA DE LECTURA DE ARCHIVOS DBF")
    print("=" * 60)
    
    monitor = EnvioMovimientos(RUTA_DBF, API_URL)
    
    # Probar lectura de movimientos
    print("\n1. Leyendo tmovim.dbf...")
    mov_path = os.path.join(RUTA_DBF, "tmovim.dbf")
    if os.path.exists(mov_path):
        movimientos = monitor.leer_dbf_movimientos(mov_path)
        print(f"Total movimientos: {len(movimientos)}")
        if movimientos:
            print("Primer movimiento:", movimientos[0])
    else:
        print(f"Archivo no encontrado: {mov_path}")
    
    # Probar lectura de detalles
    print("\n2. Leyendo tmovimdet.dbf...")
    det_path = os.path.join(RUTA_DBF, "tmovimdet.dbf")
    if os.path.exists(det_path):
        detalles = monitor.leer_dbf_detalles(det_path)
        print(f"Total detalles: {len(detalles)}")
        if detalles:
            print("Primer detalle:", detalles[0])
    else:
        print(f"Archivo no encontrado: {det_path}")

if __name__ == "__main__":
    # Verificar dependencias
    try:
        from dbfread import DBF
    except ImportError:
        logging.error("Falta la libreria 'dbfread'. Instalala con: pip install dbfread")
        sys.exit(1)
    
    try:
        import requests
    except ImportError:
        logging.error("Falta la libreria 'requests'. Instalala con: pip install requests")
        sys.exit(1)
    
    # Si se pasa el argumento 'test', solo probar lectura
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        test_single_read()
    else:
        # Crear y ejecutar monitor
        monitor = EnvioMovimientos(RUTA_DBF, API_URL)
        monitor.iniciar_monitoreo(INTERVALO_SEGUNDOS)