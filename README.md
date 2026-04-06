# CSV to SQL Processor (Windows Service)

Demonio (Windows Service Nativo) desarrollado en Go para el procesamiento asíncrono y continuo de archivos CSV. Escanea un directorio de entrada, parsea contenidos de archivos bajo patrones estrictos usando manejo nativo de memoria en CPU (`strings.Builder`) y genera archivos SQL de inserción **compatibles con TimescaleDB hypertables**.

## Características

- **Servicio Nativo de Windows**: Utiliza la arquitectura base para entablar cliente/servidor con el administrador local usando la API nativa de GO (`golang.org/x/sys/windows/svc`). Permite autoarranque con el sistema.
- **Rendimiento Industrial**: Código construido con Cero-alocaciones intermitentes en memoria (zero-allocations), sin dependencias genéricas o interpolación tardía mediante Reflection.
- **Bloqueos Exclusivos (OS Locks)**: Aplica bloqueos rígidos dictaminados a nivel de kernel en Windows (`syscall.O_EXCL`) para evitar lecturas de copias truncadas.
- **Tiempos de Gracia y Apagado**: Las capturas se manejan directo a nivel del *Service Control Manager (SCM)*, ofreciendo apagado limpio que finaliza todo archivo en proceso antes de bajar las memorias, evitando base de datos corruptas (`Graceful Shutdown`).

---

## Interfaz de Auditoría (API y Métricas)

La aplicación levanta silenciosamente un pequeño servidor interno asíncrono para otorgarte estadísticas y auditoría remota. Todas las rutas entregan un formato **JSON plano**, por lo que podrás visualizar estas métricas cómoda y orgánicamente en cualquier navegador web o consumirlas desde cualquier nube o script (ej. desde Linux mediante `CURL` o `wget`) usando la sintaxis:

**URL de Acceso Base:** `http://<IP-DE-ESTE-SERVIDOR>:<API_PORT>`

**Puntos Finales (Endpoints):**
Tanto **GET `/health`** como **GET `/metrics`** despachan el mismo paquete JSON global a tiempo real. Puedes usar cualquiera.
  ```json
  {
    "status": "UP",
    "uptime": "1h20m5s",
    "archivos_fallidos": 0,
    "archivos_procesados": 1500,
    "promedio_proceso_ms": 11,
    "tiempo_maximo_ms": 34
  }
  ```
*(Nota: El tiempo de uptime se muestra redondeado a segundos fijos, mientras que los promedios de proceso se entregan en Milisegundos para mayor detalle).*

---

## Configuración 

El archivo `config.json` se encuentra siempre alojado junto al ejecutable (posee un anclador inteligente forzado que evita rutear a la carpeta System32 nativa del O.S.).

```json
{
  "input_dir":           "./input",
  "sql_log_dir":         "./sqllog",
  "csv_log_dir":         "./csvlog",
  "logs_dir":            "./logs",
  "max_agents":          2,
  "max_files_per_agent": 50,
  "delay_before_read_ms": 200,
  "api_port":            8080
}
```

---

## SQL Generado (Formato TimescaleDB)

Cada archivo `.sql` producido es **idempotente** y compatible con TimescaleDB. Incluye:

```sql
-- 1. Crea la tabla si no existe
CREATE TABLE IF NOT EXISTS log_records (
    primary_id     VARCHAR(50)  NOT NULL,
    reception_date DATE         NOT NULL,
    record_name    VARCHAR(50)  NOT NULL,
    record_ts      TIMESTAMPTZ  NOT NULL,  -- fecha+hora combinadas para la hypertable
    record_value   FLOAT
);

-- 2. Convierte en hypertable (no falla si ya lo es)
SELECT create_hypertable('log_records', 'record_ts', if_not_exists => TRUE);

-- 3. Inserciones de datos
INSERT INTO log_records (primary_id, reception_date, record_name, record_ts, record_value)
VALUES ('151.20.35.10', '2024-03-26', 'MiIndicador', '2024-03-26 17:00', 3.14);
```

> **Nota**: `record_date` y `record_time` del CSV se combinan en una sola columna `record_ts TIMESTAMPTZ`, requerida por TimescaleDB para la partición temporal.

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
