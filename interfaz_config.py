import os
import json
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from envio_movimientos import EnvioMovimientos

class ConfiguracionApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Configuración de Envío de Movimientos")
        self.root.geometry("600x500")
        
        self.monitor_thread = None
        self.detener_evento = None
        self.monitor_activo = False
        
        # Archivo de configuración
        self.config_file = "parameters.txt"
        
        # Cargar configuración existente o crear por defecto
        self.config = self.cargar_configuracion()
        
        # Crear interfaz
        self.crear_interfaz()
        
        # Cargar valores actuales
        self.cargar_valores_interfaz()
    
    def cargar_configuracion(self):
        """Cargar configuración desde parameters.txt"""
        config_default = {
            "ruta_dbf": "C:/Users/Equipo/Desktop/ICI_Archivos/6. db_Productos",
            "api_url": "http://localhost:8000/api/movimientos",
            "intervalo_segundos": 300,
            "log_level": "INFO",
            "timeout_segundos": 30
        }
        
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    # Asegurar que todas las claves existan
                    for key in config_default:
                        if key not in config:
                            config[key] = config_default[key]
                    return config
            except Exception as e:
                messagebox.showerror("Error", f"Error al cargar configuración: {e}")
                return config_default
        else:
            # Guardar configuración por defecto
            self.guardar_configuracion(config_default)
            return config_default
    
    def guardar_configuracion(self, config=None):
        """Guardar configuración en parameters.txt"""
        if config is None:
            config = self.config
        
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
            return True
        except Exception as e:
            messagebox.showerror("Error", f"Error al guardar configuración: {e}")
            return False
    
    def crear_interfaz(self):
        """Crear los elementos de la interfaz"""
        # Frame principal con scroll
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configurar grid
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        
        # Título
        titulo = ttk.Label(main_frame, text="Configuración del Sistema de Envío", 
                          font=('Arial', 14, 'bold'))
        titulo.grid(row=0, column=0, columnspan=3, pady=10)
        
        # Ruta DBF
        ttk.Label(main_frame, text="Ruta DBF:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.ruta_dbf_var = tk.StringVar()
        self.ruta_dbf_entry = ttk.Entry(main_frame, textvariable=self.ruta_dbf_var, width=50)
        self.ruta_dbf_entry.grid(row=1, column=1, sticky=(tk.W, tk.E), pady=5, padx=5)
        ttk.Button(main_frame, text="Examinar", command=self.seleccionar_ruta).grid(row=1, column=2, pady=5)
        
        # API URL
        ttk.Label(main_frame, text="API URL:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.api_url_var = tk.StringVar()
        self.api_url_entry = ttk.Entry(main_frame, textvariable=self.api_url_var, width=50)
        self.api_url_entry.grid(row=2, column=1, sticky=(tk.W, tk.E), pady=5, padx=5)
        
        # Intervalo (minutos)
        ttk.Label(main_frame, text="Intervalo (minutos):").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.intervalo_var = tk.StringVar()
        self.intervalo_spinbox = ttk.Spinbox(main_frame, from_=1, to=60, textvariable=self.intervalo_var, width=20)
        self.intervalo_spinbox.grid(row=3, column=1, sticky=tk.W, pady=5, padx=5)
        
        # Log Level
        ttk.Label(main_frame, text="Nivel de Log:").grid(row=4, column=0, sticky=tk.W, pady=5)
        self.log_level_var = tk.StringVar()
        self.log_level_combo = ttk.Combobox(main_frame, textvariable=self.log_level_var, 
                                            values=["DEBUG", "INFO", "WARNING", "ERROR"], width=20)
        self.log_level_combo.grid(row=4, column=1, sticky=tk.W, pady=5, padx=5)
        
        # Timeout
        ttk.Label(main_frame, text="Timeout (segundos):").grid(row=5, column=0, sticky=tk.W, pady=5)
        self.timeout_var = tk.StringVar()
        self.timeout_spinbox = ttk.Spinbox(main_frame, from_=10, to=120, textvariable=self.timeout_var, width=20)
        self.timeout_spinbox.grid(row=5, column=1, sticky=tk.W, pady=5, padx=5)
        
        # Separador
        ttk.Separator(main_frame, orient='horizontal').grid(row=6, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=10)
        
        # Botones de acción
        btn_frame = ttk.Frame(main_frame)
        btn_frame.grid(row=7, column=0, columnspan=3, pady=10)
        
        self.btn_guardar = ttk.Button(btn_frame, text="Guardar Configuración", command=self.guardar_configuracion_interfaz)
        self.btn_guardar.pack(side=tk.LEFT, padx=5)
        
        self.btn_iniciar = ttk.Button(btn_frame, text="Iniciar Monitoreo", command=self.iniciar_monitoreo)
        self.btn_iniciar.pack(side=tk.LEFT, padx=5)
        
        self.btn_detener = ttk.Button(btn_frame, text="Detener Monitoreo", command=self.detener_monitoreo, state=tk.DISABLED)
        self.btn_detener.pack(side=tk.LEFT, padx=5)
        
        self.btn_estado = ttk.Button(btn_frame, text="Ver Estado", command=self.ver_estado)
        self.btn_estado.pack(side=tk.LEFT, padx=5)
        
        self.btn_reiniciar_control = ttk.Button(btn_frame, text="Reiniciar Control", command=self.reiniciar_control)
        self.btn_reiniciar_control.pack(side=tk.LEFT, padx=5)
        
        # Área de log
        ttk.Label(main_frame, text="Log de Actividad:", font=('Arial', 10, 'bold')).grid(row=8, column=0, sticky=tk.W, pady=5)
        
        # Frame para el texto de log con scroll
        log_frame = ttk.Frame(main_frame)
        log_frame.grid(row=9, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        
        self.log_text = tk.Text(log_frame, height=15, width=70, wrap=tk.WORD)
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.log_text.config(yscrollcommand=scrollbar.set)
        
        # Configurar peso para expansión
        main_frame.rowconfigure(9, weight=1)
        self.root.rowconfigure(0, weight=1)
    
    def cargar_valores_interfaz(self):
        """Cargar valores desde self.config a la interfaz"""
        self.ruta_dbf_var.set(self.config.get("ruta_dbf", ""))
        self.api_url_var.set(self.config.get("api_url", ""))
        
        intervalo_min = self.config.get("intervalo_segundos", 300) / 60
        self.intervalo_var.set(str(int(intervalo_min)))
        
        self.log_level_var.set(self.config.get("log_level", "INFO"))
        self.timeout_var.set(str(self.config.get("timeout_segundos", 30)))
    
    def seleccionar_ruta(self):
        """Abrir diálogo para seleccionar carpeta"""
        ruta = filedialog.askdirectory(title="Seleccionar carpeta de archivos DBF")
        if ruta:
            self.ruta_dbf_var.set(ruta)
    
    def guardar_configuracion_interfaz(self):
        """Guardar configuración actual de la interfaz"""
        try:
            self.config["ruta_dbf"] = self.ruta_dbf_var.get()
            self.config["api_url"] = self.api_url_var.get()
            self.config["intervalo_segundos"] = int(float(self.intervalo_var.get()) * 60)
            self.config["log_level"] = self.log_level_var.get()
            self.config["timeout_segundos"] = int(self.timeout_var.get())
            
            if self.guardar_configuracion():
                messagebox.showinfo("Éxito", "Configuración guardada correctamente")
        except ValueError as e:
            messagebox.showerror("Error", f"Error en los valores ingresados: {e}")
    
    def escribir_log(self, mensaje):
        """Escribir mensaje en el área de log"""
        self.log_text.insert(tk.END, f"{mensaje}\n")
        self.log_text.see(tk.END)
        self.root.update_idletasks()
    
    def iniciar_monitoreo(self):
        """Iniciar el monitoreo en un hilo separado"""
        if self.monitor_activo:
            messagebox.showwarning("Advertencia", "El monitoreo ya está activo")
            return
        
        # Guardar configuración actual antes de iniciar
        self.guardar_configuracion_interfaz()
        
        try:
            # Crear instancia del monitor
            self.monitor = EnvioMovimientos(
                ruta_dbf=self.config["ruta_dbf"],
                api_url=self.config["api_url"],
                intervalo_segundos=self.config["intervalo_segundos"],
                log_level=self.config["log_level"],
                timeout_segundos=self.config["timeout_segundos"]
            )
            
            # Redirigir logs a la interfaz
            self.redirigir_logs()
            
            # Crear evento de detención
            self.detener_evento = threading.Event()
            
            # Iniciar hilo de monitoreo
            self.monitor_thread = threading.Thread(
                target=self.monitor.iniciar_monitoreo,
                args=(self.detener_evento,),
                daemon=True
            )
            self.monitor_thread.start()
            
            self.monitor_activo = True
            self.btn_iniciar.config(state=tk.DISABLED)
            self.btn_detener.config(state=tk.NORMAL)
            self.escribir_log("=== MONITOREO INICIADO ===")
            
        except Exception as e:
            messagebox.showerror("Error", f"Error al iniciar monitoreo: {e}")
    
    def detener_monitoreo(self):
        """Detener el monitoreo"""
        if self.monitor_activo:
            self.detener_evento.set()
            self.monitor_activo = False
            self.btn_iniciar.config(state=tk.NORMAL)
            self.btn_detener.config(state=tk.DISABLED)
            self.escribir_log("=== MONITOREO DETENIDO ===")
    
    def ver_estado(self):
        """Ver el último registro enviado"""
        control_file = os.path.join(os.path.dirname(__file__), "ultimo_envio.txt")
        if os.path.exists(control_file):
            with open(control_file, 'r', encoding='utf-8') as f:
                ultimo = f.read().strip()
                messagebox.showinfo("Estado", f"Último registro enviado: {ultimo}")
        else:
            messagebox.showinfo("Estado", "No hay registro de envíos previos")
    
    def reiniciar_control(self):
        """Reiniciar el archivo de control"""
        if messagebox.askyesno("Confirmar", "¿Está seguro de reiniciar el control? Se enviarán todos los registros nuevamente."):
            control_file = os.path.join(os.path.dirname(__file__), "ultimo_envio.txt")
            if os.path.exists(control_file):
                os.remove(control_file)
                messagebox.showinfo("Éxito", "Archivo de control eliminado. Se enviarán todos los registros desde el inicio.")
            else:
                messagebox.showinfo("Info", "No existe archivo de control para reiniciar.")
    
    def redirigir_logs(self):
        """Redirigir logs a la interfaz"""
        import logging
        
        class TextHandler(logging.Handler):
            def __init__(self, text_widget, app):
                super().__init__()
                self.text_widget = text_widget
                self.app = app
            
            def emit(self, record):
                msg = self.format(record)
                self.app.root.after(0, lambda: self.app.escribir_log(msg))
        
        # Agregar handler para la interfaz
        handler = TextHandler(self.log_text, self)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(handler)

def main():
    root = tk.Tk()
    app = ConfiguracionApp(root)
    
    # Manejar cierre de ventana
    def on_closing():
        if app.monitor_activo:
            if messagebox.askokcancel("Salir", "¿Detener monitoreo y salir?"):
                app.detener_monitoreo()
                root.destroy()
        else:
            root.destroy()
    
    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()

if __name__ == "__main__":
    main()