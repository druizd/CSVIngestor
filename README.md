# CSV to SQL Processor (Windows Service)

Demonio (Windows Service Nativo) desarrollado en Go para el procesamiento asíncrono y continuo de archivos CSV. Escanea un directorio de entrada, parsea contenidos de archivos bajo patrones estrictos usando manejo nativo de memoria en CPU (`strings.Builder`) y genera archivos SQL de inserción correspondientes. 

## Características

- **Servicio Nativo de Windows**: Utiliza la arquitectura base para entablar cliente/servidor con el administrador local usando la API nativa de GO (`golang.org/x/sys/windows/svc`). Permite autoarranque con el sistema.
- **Rendimiento Industrial**: Código construido con Cero-alocaciones intermitentes en memoria (zero-allocations), sin dependencias genéricas o interpolación tardía mediante Reflection.
- **Bloqueos Exclusivos (OS Locks)**: Aplica bloqueos rígidos dictaminados a nivel de kernel en Windows (`syscall.O_EXCL`) para evitar lecturas de copias truncadas.
- **Tiempos de Gracia y Apagado**: Las capturas se manejan directo a nivel del *Service Control Manager (SCM)*, ofreciendo apagado limpio que finaliza todo archivo en proceso antes de bajar las memorias, evitando base de datos corruptas (`Graceful Shutdown`).

---

## Interfaz de Auditoría (API y Métricas)

La aplicación levanta silenciosamente un pequeño servidor interno asíncrono para otorgarte estadísticas y auditoría remota. Todas las rutas entregan un formato **JSON plano**, por lo que podrás visualizar estas métricas cómoda y orgánicamente en cualquier navegador web o consumirlas desde cualquier nube o script (ej. desde Linux mediante `CURL` o `wget`) usando la sintaxis:

**URL de Acceso Base:** `http://<IP-DE-ESTE-SERVIDOR>:<API_PORT>`

### 1. `GET /health`
Avisa de forma simple si el servicio está andando al 100% como demonio sin bloqueos fantasma en el Loop del hilo principal.
```json
{
  "status": "UP",
  "uptime": "56m12s"
}
```

### 2. `GET /metrics`
Ofrece las mecánicas del sistema recuperadas de la caché atómica, demostrando la fluidez sin comprometer megabytes ni causar pausas de lectura dentro de tu disco virtual.
```json
{
  "archivos_fallidos": 0,
  "archivos_procesados": 125,
  "promedio_proceso_ms": 11,
  "tiempo_maximo_ms": 42
}
```
*(Nota: El "tiempo_maximo_ms" captura el archivo que ha costado más milisegundos a la máquina procesar desde que se encendió el servicio).*

---

## Configuración 

El archivo `config.json` se encuentra siempre alojado junto al ejecutable (posee un anclador inteligente forzado que evita rutear a la carpeta System32 nativa del O.S.).

```json
{
  "input_dir": "./input",
  "sql_log_dir": "./sqllog",
  "csv_log_dir": "./csvlog",
  "logs_dir": "./logs",
  "max_agents": 2,
  "max_files_per_agent": 50,
  "delay_before_read_ms": 200,
  "api_port": 8080
}
```

---

## Operación y Comandos

Debes lanzar este bloque de instrucciones situando tu consola CMD o PowerShell en la carpeta raíz del proceso en estricto **Modo Administrador**:

- **Instalar permanente:** `.\csvprocessor.exe install`
- **Iniciar demonio:** `.\csvprocessor.exe start`
- **Detener demonio:** `.\csvprocessor.exe stop`
- **Eliminar servicio:** `.\csvprocessor.exe remove`
- **Ejecución de Debug Local:** `.\csvprocessor.exe debug` *(Levanta una CLI amigable a prueba de bloqueos cortable con CTRL-C).*

## Manual de Refactorización / Actualización

Dado que es un binario inyectado nativamente sobre las reglas de Windows Service, el Sistema Operativo del Servidor denegará estrictamente el pegado, reemplazo y reescritura del archivo `csvprocessor.exe` mientras siga vivo en los componentes locales del Kernel ("El archivo está en uso").

Para reemplazar tu versión, o aplicar en base las nuevas mecánicas de variables en `config.json`, tu flujo obligatorio debe ser:
1. Frenar la ejecución segura del motor: `.\csvprocessor.exe stop`
2. El archivo será destrabado del O.S. -> *Sobre-escriba o cambie lo necesario*.
3. Reiniciar el motor con la configuración tomada: `.\csvprocessor.exe start`
