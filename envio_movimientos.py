import os
import time
import json
import requests
from datetime import datetime
import logging
import sys
from dbfread import DBF

class EnvioMovimientos:
    def __init__(self, ruta_dbf, api_url, intervalo_segundos=300, log_level="INFO", timeout_segundos=30):
        self.ruta_dbf = ruta_dbf
        self.api_url = api_url
        self.intervalo_segundos = intervalo_segundos
        self.timeout_segundos = timeout_segundos
        self.ultimo_envio = None
        self.control_file = os.path.join(os.path.dirname(__file__), "ultimo_envio.txt")
        
        # Configurar logging
        self.log_file = "envio_movimientos.log"
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        if not os.path.exists(ruta_dbf):
            logging.error(f"La ruta {ruta_dbf} no existe")
            raise Exception(f"La ruta {ruta_dbf} no existe")
    
    def leer_ultimo_registro(self):
        """Leer el último movnumero enviado desde el archivo de control"""
        try:
            if os.path.exists(self.control_file):
                with open(self.control_file, 'r', encoding='utf-8') as f:
                    ultimo = f.read().strip()
                    if ultimo:
                        logging.info(f"Último registro enviado: {ultimo}")
                        return ultimo
            return None
        except Exception as e:
            logging.error(f"Error al leer archivo de control: {e}")
            return None
    
    def guardar_ultimo_registro(self, movnumero):
        """Guardar el último movnumero enviado en el archivo de control"""
        try:
            with open(self.control_file, 'w', encoding='utf-8') as f:
                f.write(str(movnumero))
            logging.info(f"Guardado último registro: {movnumero}")
        except Exception as e:
            logging.error(f"Error al guardar archivo de control: {e}")
    
    def leer_dbf_movimientos(self, archivo):
        """Leer archivo DBF de movimientos y devolver diccionario por movnumero"""
        movimientos_dict = {}
        try:
            logging.info(f"Leyendo {archivo}")
            table = DBF(archivo, encoding='latin-1', ignore_missing_memofile=True)
            
            contador = 0
            for record in table:
                contador += 1
                movnumero = record.get('MOVNUMERO')
                if movnumero and str(movnumero).strip():
                    movimiento = {
                        'movcoditip': self.limpiar_valor(record.get('MOVCODITIP')),
                        'movnumero': self.limpiar_valor(movnumero),
                        'almcodiorg': self.limpiar_valor(record.get('ALMCODIORG')),
                        'almorgvir': self.limpiar_valor(record.get('ALMORGVIR')),
                        'almcodidst': self.limpiar_valor(record.get('ALMCODIDST')),
                        'almdstvir': self.limpiar_valor(record.get('ALMDSTVIR')),
                        'movtipodci': self.limpiar_valor(record.get('MOVTIPODCI')),
                        'movnumedci': self.limpiar_valor(record.get('MOVNUMEDCI')),
                        'movtipodco': self.limpiar_valor(record.get('MOVTIPODCO')),
                        'movnumedco': self.limpiar_valor(record.get('MOVNUMEDCO')),
                        'cctcodigo': self.limpiar_valor(record.get('CCTCODIGO')),
                        'movtot': self.limpiar_valor(record.get('MOVTOT'), es_numero=True),
                        'prvnumeruc': self.limpiar_valor(record.get('PRVNUMERUC')),
                        'prvdescrip': self.limpiar_valor(record.get('PRVDESCRIP')),
                        'movrefe': self.limpiar_valor(record.get('MOVREFE')),
                        'movfechult': self.formatear_fecha(record.get('MOVFECHULT')),
                        'movsitua': self.limpiar_valor(record.get('MOVSITUA')),
                        'tip_comp': self.limpiar_valor(record.get('TIP_COMP')),
                        'tip_proc': self.limpiar_valor(record.get('TIP_PROC')),
                        'num_proc': self.limpiar_valor(record.get('NUM_PROC')),
                        'movfecanul': self.limpiar_valor(record.get('MOVFECANUL')),
                    }
                    movimientos_dict[str(movnumero).strip()] = movimiento
            
            logging.info(f"Total movimientos leídos: {contador}")
            
        except Exception as e:
            logging.error(f"Error al leer {archivo}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
        
        return movimientos_dict
    
    def leer_dbf_detalles_por_movimiento(self, archivo):
        """Leer archivo DBF de detalles y agrupar por movnumero"""
        detalles_dict = {}
        try:
            logging.info(f"Leyendo {archivo}")
            table = DBF(archivo, encoding='latin-1', ignore_missing_memofile=True)
            
            contador = 0
            for record in table:
                contador += 1
                movnumero = record.get('MOVNUMERO')
                medcod = record.get('MEDCOD')
                
                if movnumero and str(movnumero).strip() and medcod:
                    mov_num = str(movnumero).strip()
                    detalle = {
                        'movcoditip': self.limpiar_valor(record.get('MOVCODITIP')),
                        'movnumero': mov_num,
                        'medcod': self.limpiar_valor(medcod),
                        'medlote': self.limpiar_valor(record.get('MEDLOTE')),
                        'medfechvto': self.formatear_fecha(record.get('MEDFECHVTO')),
                        'movcantid': self.limpiar_valor(record.get('MOVCANTID'), es_numero=True),
                        'movprecio': self.limpiar_valor(record.get('MOVPRECIO'), es_numero=True),
                        'movtotal': self.limpiar_valor(record.get('MOVTOTAL'), es_numero=True),
                        'movfechult': self.formatear_fecha(record.get('MOVFECHULT')),
                        'movsitua': self.limpiar_valor(record.get('MOVSITUA')),
                    }
                    
                    if mov_num not in detalles_dict:
                        detalles_dict[mov_num] = []
                    detalles_dict[mov_num].append(detalle)
            
            logging.info(f"Total detalles leídos: {contador}")
            
        except Exception as e:
            logging.error(f"Error al leer {archivo}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
        
        return detalles_dict
    
    def limpiar_valor(self, valor, es_numero=False):
        """Limpiar y formatear valores"""
        if valor is None:
            return None
        
        if isinstance(valor, bytes):
            try:
                valor = valor.decode('latin-1').strip()
            except:
                valor = str(valor).strip()
        
        if not isinstance(valor, (int, float)):
            valor = str(valor).strip()
            if valor == '' or valor == 'None' or valor == 'nan':
                return None
        
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
        
        if hasattr(fecha, 'isoformat'):
            return fecha.isoformat()
        
        if isinstance(fecha, str):
            fecha = fecha.strip()
            if not fecha:
                return None
            
            formatos = [
                '%Y-%m-%d',
                '%Y%m%d',
                '%d/%m/%Y',
                '%d/%m/%Y %H:%M:%S',
                '%d/%m/%Y %I:%M:%S %p',
                '%Y-%m-%d %H:%M:%S',
            ]
            
            for fmt in formatos:
                try:
                    fecha_obj = datetime.strptime(fecha, fmt)
                    return fecha_obj.isoformat()
                except ValueError:
                    continue
        
        return str(fecha) if fecha else None
    
    def enviar_movimiento_con_detalles(self, movimiento, detalles):
        """Enviar un movimiento y sus detalles a la API"""
        try:
            logging.info(f"Enviando movimiento: {movimiento['movnumero']} con {len(detalles)} detalles")
            
            # Enviar movimiento
            url_mov = f"{self.api_url}/store"
            payload_mov = {'movimientos': [movimiento]}
            response_mov = requests.post(url_mov, json=payload_mov, timeout=self.timeout_segundos)
            
            if response_mov.status_code != 200:
                logging.error(f"Error al enviar movimiento {movimiento['movnumero']}: {response_mov.status_code}")
                return False
            
            # Enviar detalles si existen
            if detalles:
                url_det = f"{self.api_url}/store-detalles"
                payload_det = {'detalles': detalles}
                response_det = requests.post(url_det, json=payload_det, timeout=self.timeout_segundos)
                
                if response_det.status_code != 200:
                    logging.error(f"Error al enviar detalles para {movimiento['movnumero']}: {response_det.status_code}")
                    return False
            
            return True
            
        except Exception as e:
            logging.error(f"Error en envío: {str(e)}")
            return False
    
    def procesar_archivos(self):
        """Procesar archivos DBF y enviar registro por registro"""
        # Obtener último registro enviado
        ultimo_enviado = self.leer_ultimo_registro()
        
        # Leer movimientos y detalles
        mov_path = os.path.join(self.ruta_dbf, "tmovim.dbf")
        det_path = os.path.join(self.ruta_dbf, "tmovimdet.dbf")
        
        if not os.path.exists(mov_path):
            logging.error(f"No se encuentra el archivo: {mov_path}")
            return
        
        movimientos = self.leer_dbf_movimientos(mov_path)
        detalles = self.leer_dbf_detalles_por_movimiento(det_path) if os.path.exists(det_path) else {}
        
        # Ordenar movimientos por movnumero
        movimientos_ordenados = sorted(movimientos.items(), key=lambda x: x[0])
        
        # Encontrar desde dónde empezar
        empezar = False
        enviados = 0
        fallidos = 0
        
        logging.info(f"Procesando {len(movimientos_ordenados)} movimientos totales")
        
        for mov_num, movimiento in movimientos_ordenados:
            # Si hay un último registro guardado, saltar hasta encontrarlo
            if ultimo_enviado and not empezar:
                if mov_num == ultimo_enviado:
                    empezar = True
                    logging.info(f"Reanudando desde: {mov_num}")
                continue
            
            empezar = True  # Después de encontrar el último, procesar todos
            
            # Obtener detalles para este movimiento
            detalles_mov = detalles.get(mov_num, [])
            
            # Enviar movimiento con sus detalles
            logging.info(f"Enviando registro {mov_num} ({len(detalles_mov)} detalles)")
            
            if self.enviar_movimiento_con_detalles(movimiento, detalles_mov):
                self.guardar_ultimo_registro(mov_num)
                enviados += 1
                logging.info(f"✅ Registro {mov_num} enviado correctamente")
            else:
                fallidos += 1
                logging.error(f"❌ Error al enviar registro {mov_num}")
                break  # Detener en caso de error para no perder el orden
        
        logging.info(f"Resumen - Enviados: {enviados}, Fallidos: {fallidos}")
        return enviados, fallidos
    
    def iniciar_monitoreo(self, detener_evento=None):
        """Iniciar monitoreo continuo"""
        logging.info("=" * 60)
        logging.info("INICIANDO MONITOREO DE ARCHIVOS DBF")
        logging.info("=" * 60)
        logging.info(f"Ruta: {self.ruta_dbf}")
        logging.info(f"Intervalo: {self.intervalo_segundos} segundos ({self.intervalo_segundos/60:.1f} minutos)")
        logging.info(f"API URL: {self.api_url}")
        logging.info(f"Archivo control: {self.control_file}")
        logging.info("=" * 60)
        
        contador = 0
        while True:
            # Verificar si se debe detener
            if detener_evento and detener_evento.is_set():
                logging.info("\n⏹️ Monitoreo detenido")
                break
            
            try:
                contador += 1
                logging.info(f"\n{'='*60}")
                logging.info(f"Ciclo #{contador} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                logging.info(f"{'='*60}")
                
                self.procesar_archivos()
                
                # Esperar con verificación de detención
                for _ in range(self.intervalo_segundos):
                    if detener_evento and detener_evento.is_set():
                        break
                    time.sleep(1)
                
            except Exception as e:
                logging.error(f"Error en ciclo: {str(e)}")
                import traceback
                logging.error(traceback.format_exc())
                time.sleep(60)

                




