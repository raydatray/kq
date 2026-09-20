package events

import (
	"encoding/json"
	"errors"
	"io"
	"sync"
)

const writerBufferSize = 4096

var errWriterClosed = errors.New("events: writer is closed")

// Writer blocks only when its bounded event buffer is full; Close drains it.
type Writer struct {
	mu        sync.RWMutex
	encoder   *json.Encoder
	events    chan Event
	done      chan struct{}
	closeOnce sync.Once
	closed    bool
	err       error
}

func NewWriter(output io.Writer) *Writer {
	writer := &Writer{
		encoder: json.NewEncoder(output),
		events:  make(chan Event, writerBufferSize),
		done:    make(chan struct{}),
	}
	go writer.run()
	return writer
}

func (w *Writer) Write(event Event) error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return errWriterClosed
	}

	w.events <- event
	return nil
}

func (w *Writer) Close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		close(w.events)
		w.mu.Unlock()
	})
	<-w.done
	return w.err
}

func (w *Writer) run() {
	defer close(w.done)
	for event := range w.events {
		if w.err != nil {
			continue
		}
		w.err = w.encoder.Encode(event)
	}
}
