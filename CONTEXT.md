# csvprocessor — Contexto del Repositorio

## ¿Qué es?

Servicio **Windows** (ejecutable nativo `.exe`) responsable de la **primera etapa del pipeline**:  
Convierte archivos `.csv` de logs de trading en sentencias SQL compatibles con TimescaleDB y los publica en RabbitMQ para su ejecución remota.

Corre como un **Servicio Nativo de Windows** (`services.msc`) con arranque automático, arquitectura de Worker Pool y una API HTTP interna para monitoreo remoto.

## Posición en el sistema

```
[Fuente de CSVs] ──.csv─→ [csvprocessor (Win)] ──SQL─→ [RabbitMQ (nube)] ──→ [csvconsumer (Linux)] ──→ [TimescaleDB]
                              ↑
                     Escaneo cada 1s del InputDir
```

## Stack técnico

| Componente | Detalle |
|------------|---------|
| Lenguaje | Go (Windows-only: `GOOS=windows`) |
| Base de datos destino | TimescaleDB — genera SQL con `create_hypertable()` |
| Windows Service | `golang.org/x/sys/windows/svc` |
| Logging | Sistema propio (`internal/logger`) con rotación de archivos |
| API interna | `net/http` estándar de Go |

## Estructura del proyecto

```
csvprocessor/
├── cmd/
│   └── csvprocessor/
│       └── main.go          # Punto de entrada: lógica de servicio Windows, comandos CLI
├── internal/
│   ├── api/                 # Servidor HTTP de métricas y health check
│   ├── config/              # Carga y validación de config.json
│   ├── logger/              # Logger con rotación de archivos de log
│   ├── processor/           # Lógica de conversión CSV → SQL (TimescaleDB hypertable)
│   └── worker/              # Worker Pool con goroutines y bloqueo de archivos OS
├── config.json              # Configuración de producción
├── winres/                  # Recursos del ejecutable Windows (icono, versión, etc.)
└── csvprocessor.exe         # Binario compilado listo para despliegue
```

## SQL generado (compatible con TimescaleDB)

Cada archivo `.sql` producido es **completamente idempotente** (puede ejecutarse múltiples veces sin error) e incluye las siguientes secciones en orden:

### 1. Tabla principal

```sql
CREATE TABLE IF NOT EXISTS log_records (
    primary_id     VARCHAR(50)  NOT NULL,   -- IP / identificador del equipo
    reception_date DATE         NOT NULL,   -- Fecha del archivo CSV
    record_name    VARCHAR(50)  NOT NULL,   -- Nombre de la variable
    record_ts      TIMESTAMPTZ  NOT NULL,   -- Timestamp combinado (col. de partición)
    record_value   FLOAT
);
```

> `record_date` (DATE) y `record_time` (TIME) del CSV se **fusionan** en `record_ts TIMESTAMPTZ`.

### 2. Hypertable con partición dual

```sql
SELECT create_hypertable(
    'log_records', 'record_ts',
    partitioning_column => 'primary_id',
    number_partitions   => 16,   -- distribución para 500+ equipos
    if_not_exists       => TRUE
);
```

### 3. Índices de rendimiento

```sql
-- Consultas por equipo + tiempo → O(log n)
CREATE INDEX IF NOT EXISTS idx_log_records_device_time
    ON log_records (primary_id, record_ts DESC);

-- Previene duplicados (idempotencia al reprocesar el mismo CSV)
CREATE UNIQUE INDEX IF NOT EXISTS uq_log_records_device_name_ts
    ON log_records (primary_id, record_name, record_ts);
```

### 4. Compresión automática (chunks > 7 días)

```sql
ALTER TABLE log_records SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'primary_id, record_name'
);
SELECT add_compression_policy('log_records', INTERVAL '7 days', if_not_exists => TRUE);
```

### 5. Continuous Aggregates (5 vistas pre-calculadas)

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS log_daily   WITH (timescaledb.continuous) AS ...;  -- bucket 1 día
CREATE MATERIALIZED VIEW IF NOT EXISTS log_weekly  WITH (timescaledb.continuous) AS ...;  -- bucket 1 semana
CREATE MATERIALIZED VIEW IF NOT EXISTS log_monthly WITH (timescaledb.continuous) AS ...;  -- bucket 1 mes
CREATE MATERIALIZED VIEW IF NOT EXISTS log_semester WITH (timescaledb.continuous) AS ...; -- bucket 6 meses
CREATE MATERIALIZED VIEW IF NOT EXISTS log_yearly  WITH (timescaledb.continuous) AS ...;  -- bucket 1 año
```

Cada vista expone: `primary_id`, `record_name`, `bucket`, `avg_value`, `min_value`, `max_value`, `sample_count`.

### 6. Datos (INSERT)

```sql
INSERT INTO log_records (primary_id, reception_date, record_name, record_ts, record_value)
VALUES ('151.20.35.10', '2024-03-26', 'CPU_Load', '2024-03-26 17:00:17', 45.2)
ON CONFLICT (primary_id, record_name, record_ts) DO NOTHING;  -- idempotente
```

## Comandos CLI

```powershell
# Ejecutar como Administrador:
csvprocessor.exe install   # Instala en services.msc con arranque automático
csvprocessor.exe start     # Inicia el servicio
csvprocessor.exe stop      # Detiene el servicio
csvprocessor.exe remove    # Desinstala el servicio
csvprocessor.exe debug     # Ejecuta en consola interactiva (para pruebas con Ctrl+C)
```

## Configuración (`config.json`)

```json
{
  "input_dir":    "ruta donde caen los CSV",
  "done_dir":     "ruta para CSV procesados exitosamente",
  "error_dir":    "ruta para CSV con errores",
  "logs_dir":     "ruta donde se escriben los logs del servicio",
  "worker_count": 2,
  "api_port":     8080
}
```

## API de Métricas

Cuando `api_port > 0`, expone un servidor HTTP (por defecto en el puerto 8080) para monitoreo remoto del estado del servicio, archivos procesados y errores activos.

## Módulos internos clave

| Paquete | Responsabilidad |
|---------|-----------------|
| `internal/config` | Deserializa `config.json` y aplica defaults |
| `internal/logger` | Logger de archivos con niveles Info/Error/Event |
| `internal/processor` | Parsea CSV y genera SQL `INSERT` para TimescaleDB hypertable |
| `internal/worker` | Worker Pool: toma archivos del channel, llama al processor, mueve archivos |
| `internal/api` | Servidor HTTP con endpoints de métricas y health |

## Notas de despliegue

- Requiere ejecución como **Administrador** para instalar/desinstalar el servicio
- El `.exe` se registra en el SCM (Service Control Manager) de Windows apuntando a su ruta absoluta
- Integra con el **Event Viewer de Windows** (`eventlog`) para logs persistentes del sistema
- El binario `csvprocessor.exe` y `config.json` deben estar en el **mismo directorio**

## Repositorios relacionados

| Repositorio | Relación |
|-------------|----------|
| `csvshipper-win` | Servicio hermano: toma los SQL generados por este y los envía a RabbitMQ |
| `csvconsumer` | Ejecuta en Linux los SQL enviados a RabbitMQ |
| `db-infra` | Provee la base de datos TimescaleDB destino |
