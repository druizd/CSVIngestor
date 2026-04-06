package processor

import (
	"bufio"
	"csvprocessor/internal/config"
	"csvprocessor/internal/logger"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

var fileNameRegex = regexp.MustCompile(`^log \((.*?)(?:--.*?)\)\s+(\d{4})_(\d{2})_(\d{2})_.*\.csv$`)

// openLocked implements an exclusive file lock for Windows
func openLocked(path string) (*os.File, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	h, err := syscall.CreateFile(p, syscall.GENERIC_READ, 0, nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(h), path), nil
}

func ProcessFile(cfg *config.Config, filePath string) error {
	stat, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("error al leer estadisticas del archivo: %w", err)
	}

	// Wait 200ms from the last modification time
	timeSinceLastMod := time.Since(stat.ModTime())
	targetWait := time.Duration(cfg.DelayBeforeReadMs) * time.Millisecond
	if timeSinceLastMod < targetWait {
		sleepDuration := targetWait - timeSinceLastMod
		logger.Info("Esperando %v antes de procesar %s", sleepDuration, filepath.Base(filePath))
		time.Sleep(sleepDuration)
	}

	// Extract info from filename
	baseName := filepath.Base(filePath)
	matches := fileNameRegex.FindStringSubmatch(baseName)
	if len(matches) < 5 {
		return fmt.Errorf("el nombre del archivo no cumple el formato esperado: %s", baseName)
	}
	primaryIDFile := matches[1]
	receptionDate := fmt.Sprintf("%s-%s-%s", matches[2], matches[3], matches[4])

	// Open with exclusive lock
	file, err := openLocked(filePath)
	if err != nil {
		return fmt.Errorf("no se pudo abrir el archivo de forma exclusiva (quizas esta en uso): %w", err)
	}
	defer file.Close()

	// Read contents
	scanner := bufio.NewScanner(file)
	lineCount := 0

	var sqlBuffer strings.Builder
	// Pre-solicitamos memoria a la RAM para evitar fragmentaciones (ej: 2MB).
	sqlBuffer.Grow(2 * 1024 * 1024)

	sqlBuffer.WriteString("CREATE TABLE IF NOT EXISTS log_records (\n")
	sqlBuffer.WriteString("    primary_id     VARCHAR(50)  NOT NULL,\n")
	sqlBuffer.WriteString("    reception_date DATE         NOT NULL,\n")
	sqlBuffer.WriteString("    record_name    VARCHAR(50)  NOT NULL,\n")
	sqlBuffer.WriteString("    record_ts      TIMESTAMPTZ  NOT NULL,\n")
	sqlBuffer.WriteString("    record_value   FLOAT\n")
	sqlBuffer.WriteString(");\n\n")
	// Hypertable con doble partición: tiempo (record_ts) + espacio (primary_id).
	// number_partitions => 16 distribuye bien 500+ equipos en chunks manejables.
	// if_not_exists => TRUE lo hace idempotente.
	sqlBuffer.WriteString("SELECT create_hypertable('log_records', 'record_ts',\n")
	sqlBuffer.WriteString("    partitioning_column => 'primary_id',\n")
	sqlBuffer.WriteString("    number_partitions   => 16,\n")
	sqlBuffer.WriteString("    if_not_exists       => TRUE\n")
	sqlBuffer.WriteString(");\n\n")
	// Índice compuesto: queries por equipo + rango de tiempo son O(log n) en vez de full scan.
	sqlBuffer.WriteString("CREATE INDEX IF NOT EXISTS idx_log_records_device_time\n")
	sqlBuffer.WriteString("    ON log_records (primary_id, record_ts DESC);\n\n")
	// Índice único: evita duplicados de (equipo + variable + timestamp).
	// Si el mismo archivo se procesa dos veces, el INSERT ignorará la fila existente.
	sqlBuffer.WriteString("CREATE UNIQUE INDEX IF NOT EXISTS uq_log_records_device_name_ts\n")
	sqlBuffer.WriteString("    ON log_records (primary_id, record_name, record_ts);\n\n")
	// Política de compresión: comprime chunks con más de 7 días.
	// segmentby agrupa por equipo+variable dentro de cada chunk comprimido.
	sqlBuffer.WriteString("ALTER TABLE log_records SET (\n")
	sqlBuffer.WriteString("    timescaledb.compress,\n")
	sqlBuffer.WriteString("    timescaledb.compress_segmentby = 'primary_id, record_name'\n")
	sqlBuffer.WriteString(");\n")
	sqlBuffer.WriteString("SELECT add_compression_policy('log_records', INTERVAL '7 days', if_not_exists => TRUE);\n\n")
	// Continuous Aggregates: vistas pre-calculadas para consultas por día/semana/mes/semestre/año.
	// TimescaleDB las actualiza en background, las queries son instantáneas.
	sqlBuffer.WriteString("CREATE MATERIALIZED VIEW IF NOT EXISTS log_daily\n")
	sqlBuffer.WriteString("WITH (timescaledb.continuous) AS\n")
	sqlBuffer.WriteString("SELECT\n")
	sqlBuffer.WriteString("    primary_id,\n")
	sqlBuffer.WriteString("    record_name,\n")
	sqlBuffer.WriteString("    time_bucket('1 day', record_ts) AS bucket,\n")
	sqlBuffer.WriteString("    AVG(record_value)  AS avg_value,\n")
	sqlBuffer.WriteString("    MIN(record_value)  AS min_value,\n")
	sqlBuffer.WriteString("    MAX(record_value)  AS max_value,\n")
	sqlBuffer.WriteString("    COUNT(*)           AS sample_count\n")
	sqlBuffer.WriteString("FROM log_records\n")
	sqlBuffer.WriteString("GROUP BY primary_id, record_name, bucket\n")
	sqlBuffer.WriteString("WITH NO DATA;\n\n")
	sqlBuffer.WriteString("CREATE MATERIALIZED VIEW IF NOT EXISTS log_weekly\n")
	sqlBuffer.WriteString("WITH (timescaledb.continuous) AS\n")
	sqlBuffer.WriteString("SELECT\n")
	sqlBuffer.WriteString("    primary_id,\n")
	sqlBuffer.WriteString("    record_name,\n")
	sqlBuffer.WriteString("    time_bucket('1 week', record_ts) AS bucket,\n")
	sqlBuffer.WriteString("    AVG(record_value)  AS avg_value,\n")
	sqlBuffer.WriteString("    MIN(record_value)  AS min_value,\n")
	sqlBuffer.WriteString("    MAX(record_value)  AS max_value,\n")
	sqlBuffer.WriteString("    COUNT(*)           AS sample_count\n")
	sqlBuffer.WriteString("FROM log_records\n")
	sqlBuffer.WriteString("GROUP BY primary_id, record_name, bucket\n")
	sqlBuffer.WriteString("WITH NO DATA;\n\n")
	sqlBuffer.WriteString("CREATE MATERIALIZED VIEW IF NOT EXISTS log_monthly\n")
	sqlBuffer.WriteString("WITH (timescaledb.continuous) AS\n")
	sqlBuffer.WriteString("SELECT\n")
	sqlBuffer.WriteString("    primary_id,\n")
	sqlBuffer.WriteString("    record_name,\n")
	sqlBuffer.WriteString("    time_bucket('1 month', record_ts) AS bucket,\n")
	sqlBuffer.WriteString("    AVG(record_value)  AS avg_value,\n")
	sqlBuffer.WriteString("    MIN(record_value)  AS min_value,\n")
	sqlBuffer.WriteString("    MAX(record_value)  AS max_value,\n")
	sqlBuffer.WriteString("    COUNT(*)           AS sample_count\n")
	sqlBuffer.WriteString("FROM log_records\n")
	sqlBuffer.WriteString("GROUP BY primary_id, record_name, bucket\n")
	sqlBuffer.WriteString("WITH NO DATA;\n\n")
	sqlBuffer.WriteString("CREATE MATERIALIZED VIEW IF NOT EXISTS log_semester\n")
	sqlBuffer.WriteString("WITH (timescaledb.continuous) AS\n")
	sqlBuffer.WriteString("SELECT\n")
	sqlBuffer.WriteString("    primary_id,\n")
	sqlBuffer.WriteString("    record_name,\n")
	sqlBuffer.WriteString("    time_bucket('6 months', record_ts) AS bucket,\n")
	sqlBuffer.WriteString("    AVG(record_value)  AS avg_value,\n")
	sqlBuffer.WriteString("    MIN(record_value)  AS min_value,\n")
	sqlBuffer.WriteString("    MAX(record_value)  AS max_value,\n")
	sqlBuffer.WriteString("    COUNT(*)           AS sample_count\n")
	sqlBuffer.WriteString("FROM log_records\n")
	sqlBuffer.WriteString("GROUP BY primary_id, record_name, bucket\n")
	sqlBuffer.WriteString("WITH NO DATA;\n\n")
	sqlBuffer.WriteString("CREATE MATERIALIZED VIEW IF NOT EXISTS log_yearly\n")
	sqlBuffer.WriteString("WITH (timescaledb.continuous) AS\n")
	sqlBuffer.WriteString("SELECT\n")
	sqlBuffer.WriteString("    primary_id,\n")
	sqlBuffer.WriteString("    record_name,\n")
	sqlBuffer.WriteString("    time_bucket('1 year', record_ts) AS bucket,\n")
	sqlBuffer.WriteString("    AVG(record_value)  AS avg_value,\n")
	sqlBuffer.WriteString("    MIN(record_value)  AS min_value,\n")
	sqlBuffer.WriteString("    MAX(record_value)  AS max_value,\n")
	sqlBuffer.WriteString("    COUNT(*)           AS sample_count\n")
	sqlBuffer.WriteString("FROM log_records\n")
	sqlBuffer.WriteString("GROUP BY primary_id, record_name, bucket\n")
	sqlBuffer.WriteString("WITH NO DATA;\n\n")
	// Políticas de refresco automático para cada aggregate.
	sqlBuffer.WriteString("SELECT add_continuous_aggregate_policy('log_daily',    start_offset => INTERVAL '3 days',   end_offset => INTERVAL '1 hour',  schedule_interval => INTERVAL '1 hour',  if_not_exists => TRUE);\n")
	sqlBuffer.WriteString("SELECT add_continuous_aggregate_policy('log_weekly',   start_offset => INTERVAL '2 weeks',  end_offset => INTERVAL '1 day',   schedule_interval => INTERVAL '1 day',   if_not_exists => TRUE);\n")
	sqlBuffer.WriteString("SELECT add_continuous_aggregate_policy('log_monthly',  start_offset => INTERVAL '2 months', end_offset => INTERVAL '1 day',   schedule_interval => INTERVAL '1 day',   if_not_exists => TRUE);\n")
	sqlBuffer.WriteString("SELECT add_continuous_aggregate_policy('log_semester', start_offset => INTERVAL '1 year',  end_offset => INTERVAL '1 week',  schedule_interval => INTERVAL '1 week',  if_not_exists => TRUE);\n")
	sqlBuffer.WriteString("SELECT add_continuous_aggregate_policy('log_yearly',   start_offset => INTERVAL '2 years', end_offset => INTERVAL '1 month', schedule_interval => INTERVAL '1 month', if_not_exists => TRUE);\n\n")

	for scanner.Scan() {
		lineCount++
		line := scanner.Text()

		// Ignorar encabezados y lineas vacias
		if lineCount <= 2 || len(line) == 0 {
			continue
		}

		// Extracción de parts[0] (antes de la primer coma)
		commaIdx1 := strings.IndexByte(line, ',')
		if commaIdx1 == -1 {
			continue
		}

		tagNameFull := line[:commaIdx1]
		dotIdx := strings.LastIndexByte(tagNameFull, '.')
		if dotIdx == -1 {
			continue
		}
		recordName := tagNameFull[dotIdx+1:]
		primaryIDRow := primaryIDFile

		// Extracción de parts[1] (entre primera y segunda coma)
		rest := line[commaIdx1+1:]
		commaIdx2 := strings.IndexByte(rest, ',')
		if commaIdx2 == -1 {
			continue
		}

		dateTimeFull := rest[:commaIdx2]
		spaceIdx := strings.IndexByte(dateTimeFull, ' ')
		if spaceIdx == -1 {
			continue
		}
		
		recordDate := dateTimeFull[:spaceIdx]
		recordTimeFull := dateTimeFull[spaceIdx+1:]
		
		var recordTime string
		if len(recordTimeFull) >= 5 {
			recordTime = recordTimeFull[:5]
		} else {
			recordTime = recordTimeFull
		}

		// Extracción de parts[2] (el valor), hasta la prox coma
		rest = rest[commaIdx2+1:]
		commaIdx3 := strings.IndexByte(rest, ',')
		var recordValue string
		if commaIdx3 == -1 {
			// Trim space solo en el último pedazo si hace falta
			recordValue = strings.TrimSpace(rest) 
		} else {
			recordValue = strings.TrimSpace(rest[:commaIdx3])
		}

		// Concatenación lineal (Zero allocations extra comparado a Sprintf)
		// record_ts combina record_date + record_time como TIMESTAMPTZ para la hypertable.
		sqlBuffer.WriteString("INSERT INTO log_records (primary_id, reception_date, record_name, record_ts, record_value) VALUES ('")
		sqlBuffer.WriteString(primaryIDRow)
		sqlBuffer.WriteString("', '")
		sqlBuffer.WriteString(receptionDate)
		sqlBuffer.WriteString("', '")
		sqlBuffer.WriteString(recordName)
		sqlBuffer.WriteString("', '")
		sqlBuffer.WriteString(recordDate)
		sqlBuffer.WriteString(" ")
		sqlBuffer.WriteString(recordTime)
		sqlBuffer.WriteString("', ")
		sqlBuffer.WriteString(recordValue)
		// ON CONFLICT DO NOTHING: si (primary_id, record_name, record_ts) ya existe, ignora la fila.
		// Esto permite reprocesar el mismo CSV sin duplicar datos.
		sqlBuffer.WriteString(") ON CONFLICT (primary_id, record_name, record_ts) DO NOTHING;\n")
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error al leer el contenido del archivo: %w", err)
	}

	// Finished generating SQL.
	file.Close() // Explicitly close before moving

	// Generate target SQL path
	filenameWithoutExt := strings.TrimSuffix(baseName, filepath.Ext(baseName))
	sqlFileName := filenameWithoutExt + ".sql"
	sqlFilePath := filepath.Join(cfg.SqlLogDir, sqlFileName)

	err = os.WriteFile(sqlFilePath, []byte(sqlBuffer.String()), 0666)
	if err != nil {
		return fmt.Errorf("error escribiendo archivo SQL: %w", err)
	}
	
	// Move original CSV
	csvDestPath := filepath.Join(cfg.CsvLogDir, baseName)
	err = os.Rename(filePath, csvDestPath)
	if err != nil {
		return fmt.Errorf("error moviendo el archivo CSV a %s: %w. Asegurate que no haya colisiones de nombres.", csvDestPath, err)
	}

	return nil
}
