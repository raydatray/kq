package events

import (
	"encoding/json"
	"errors"
	"io"
	"sync"
)

const writerBufferSize = 4096

var errWriterClosed = errors.New("events: writer is closed")

// Writer encodes events as newline-delimited JSON on a dedicated goroutine.
type Writer struct {
	mu        sync.RWMutex
	encoder   *json.Encoder
	events    chan Event
	done      chan struct{}
	closeOnce sync.Once
	closed    bool
	err       error
}

// NewWriter wraps output with a bounded, lossless asynchronous event stream.
func NewWriter(output io.Writer) *Writer {
	writer := &Writer{
		encoder: json.NewEncoder(output),
		events:  make(chan Event, writerBufferSize),
		done:    make(chan struct{}),
	}
	go writer.run()
	return writer
}

// Write queues one event, blocking only when the bounded buffer is full.
func (w *Writer) Write(event Event) error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return errWriterClosed
	}

	w.events <- event
	return nil
}

// Close drains queued events and reports the first encoding error.
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
