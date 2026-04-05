package worker

import (
	"csvprocessor/internal/api"
	"csvprocessor/internal/config"
	"csvprocessor/internal/logger"
	"csvprocessor/internal/processor"
	"sync"
	"time"
)

// Agent represents a worker that reads files off the queue.
func StartPool(cfg *config.Config, fileChan <-chan string, wg *sync.WaitGroup) {
	for i := 0; i < cfg.MaxAgents; i++ {
		wg.Add(1)
		go agentLoop(i, cfg, fileChan, wg)
	}
}

func agentLoop(agentID int, cfg *config.Config, fileChan <-chan string, wg *sync.WaitGroup) {
	logger.Event("Agente #%d iniciado.", agentID)

	filesProcessed := 0

	for filePath := range fileChan {
		logger.Info("Agente #%d procesando: %s", agentID, filePath)
		
		startProc := time.Now()
		err := processor.ProcessFile(cfg, filePath)
		elapsedMs := uint64(time.Since(startProc).Milliseconds())

		if err != nil {
			logger.Error("Agente #%d falló procesando %s: %v", agentID, filePath, err)
			api.RecordMetrics(false, elapsedMs)
		} else {
			logger.Info("Agente #%d terminó exitosamente con: %s", agentID, filePath)
			api.RecordMetrics(true, elapsedMs)
		}

		filesProcessed++

		// Check if we reached the max limit for this agent
		if filesProcessed >= cfg.MaxFilesPerAgent {
			logger.Event("Agente #%d alcanzó el límite de archivos (%d). Deteniendo temporalmente...", agentID, filesProcessed)
			break
		}
	}

	// Comprobar si el canal de archivos sigue abierto
	// Si acabamos por límite, relanzamos. Si no, terminamos porque el canal se cerró.
	isClosed := false
	if filesProcessed < cfg.MaxFilesPerAgent {
		isClosed = true
	}

	logger.Event("Agente #%d terminado.", agentID)
	wg.Done()

	if !isClosed {
		wg.Add(1)
		go agentLoop(agentID, cfg, fileChan, wg)
	}
}
