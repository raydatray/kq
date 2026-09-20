package events

import (
	"encoding/json"
	"io"
	"sync"
)

// Writer encodes events as newline-delimited JSON safely for concurrent use.
type Writer struct {
	mu      sync.Mutex
	encoder *json.Encoder
}

// NewWriter wraps output for event emission.
func NewWriter(output io.Writer) *Writer {
	return &Writer{encoder: json.NewEncoder(output)}
}

// Write encodes one event as a single JSON line.
func (w *Writer) Write(event Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.encoder.Encode(event)
}
