package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"csvprocessor/internal/logger"
)

var (
	startTime                time.Time
	filesProcessed           uint64
	filesFailed              uint64
	totalProcessingTimeMs    uint64
	maxProcessingTimeMs      uint64
)

func init() {
	startTime = time.Now()
}

// RecordMetrics guarda de forma segura los atributos de tiempo
func RecordMetrics(success bool, durationMs uint64) {
	if success {
		atomic.AddUint64(&filesProcessed, 1)
		atomic.AddUint64(&totalProcessingTimeMs, durationMs)
		
		// Update Max safely
		for {
			currentMax := atomic.LoadUint64(&maxProcessingTimeMs)
			if durationMs <= currentMax {
				break
			}
			if atomic.CompareAndSwapUint64(&maxProcessingTimeMs, currentMax, durationMs) {
				break
			}
		}
	} else {
		atomic.AddUint64(&filesFailed, 1)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	uptime := time.Since(startTime).String()
	response := map[string]string{
		"status": "UP",
		"uptime": uptime,
	}
	json.NewEncoder(w).Encode(response)
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	processed := atomic.LoadUint64(&filesProcessed)
	failed := atomic.LoadUint64(&filesFailed)
	totalTime := atomic.LoadUint64(&totalProcessingTimeMs)
	maxTime := atomic.LoadUint64(&maxProcessingTimeMs)

	var avgTime uint64 = 0
	if processed > 0 {
		avgTime = totalTime / processed
	}

	response := map[string]interface{}{
		"archivos_procesados":  processed,
		"archivos_fallidos":    failed,
		"promedio_proceso_ms":  avgTime,
		"tiempo_maximo_ms":     maxTime,
	}
	json.NewEncoder(w).Encode(response)
}

func StartServer(port int) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/metrics", metricsHandler)

	addr := fmt.Sprintf("0.0.0.0:%d", port) // 0.0.0.0 para acceso externo
	logger.Event("Iniciando API de Auditoría Remota en %s", addr)
	
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Fallo critico en API remota: %v", err)
		}
	}()
}
