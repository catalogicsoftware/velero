/*
Copyright the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package logging

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	logsproto "catalogicsoftware.com/archimedes/kubeagentproto/protogen"
)

const (
	logBufferSize     = 200
	grpcServerAddress = "localhost:50061"
	reconnectInterval = 1 * time.Second
)

// AmdsGrpcHook sends log entries to a gRPC server asynchronously using a pool of reusable objects.
type AmdsGrpcHook struct {
	mu                    sync.Mutex
	stream                logsproto.VeleroLogStreamer_StreamLogsClient
	conn                  *grpc.ClientConn
	logChannel            chan *logsproto.VeleroLogEntry
	once                  sync.Once
	shutdown              chan struct{}
	lastConnectionAttempt time.Time
	entryPool             sync.Pool // Pool to reuse LogEntry objects.
}

// NewAmdsGrpcHook creates a new, optimized hook for streaming logs.
func NewAmdsGrpcHook() *AmdsGrpcHook {
	h := &AmdsGrpcHook{
		logChannel: make(chan *logsproto.VeleroLogEntry, logBufferSize),
		shutdown:   make(chan struct{}),
	}
	// Initialize the pool. The New function is called when the pool is empty.
	h.entryPool.New = func() interface{} {
		return &logsproto.VeleroLogEntry{
			Fields: make(map[string]string),
		}
	}
	return h
}

// Levels returns the log levels that this hook will be triggered for.
func (h *AmdsGrpcHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.WarnLevel,
		logrus.ErrorLevel,
		logrus.FatalLevel,
		logrus.PanicLevel,
	}
}

// Fire is called by logrus. It is highly optimized to be non-blocking and avoid memory allocations.
func (h *AmdsGrpcHook) Fire(entry *logrus.Entry) error {
	h.once.Do(func() {
		go h.processLogQueue()
	})

	// Get a reusable LogEntry object from the pool.
	logEntry := h.entryPool.Get().(*logsproto.VeleroLogEntry)

	// Populate the object with data from the logrus entry.
	logEntry.Level = entry.Level.String()
	logEntry.Message = entry.Message
	logEntry.Timestamp = timestamppb.New(entry.Time)
	for k, v := range entry.Data {
		logEntry.Fields[k] = fmt.Sprintf("%v", v)
	}

	// Attempt a non-blocking send to the channel.
	select {
	case h.logChannel <- logEntry:
		// The log was successfully queued.
	default:
		// The buffer is full. We drop the log.
		h.resetLogEntry(logEntry)
		h.entryPool.Put(logEntry)
	}

	return nil
}

// processLogQueue is the long-running goroutine that sends logs from the buffer.
func (h *AmdsGrpcHook) processLogQueue() {
	for {
		select {
		case logEntry := <-h.logChannel:
			h.mu.Lock()
			if h.stream == nil {
				if err := h.connect(); err != nil {
					// The connect function already logs the specific error.
					h.mu.Unlock()
					h.resetLogEntry(logEntry)
					h.entryPool.Put(logEntry)
					continue
				}
			}

			err := h.stream.Send(logEntry)
			if err != nil {
				h.closeStream()
			}
			h.mu.Unlock()

			h.resetLogEntry(logEntry)
			h.entryPool.Put(logEntry)

		case <-h.shutdown:
			h.mu.Lock()
			h.closeStream()
			h.mu.Unlock()
			return
		}
	}
}

// resetLogEntry clears the fields of a LogEntry so it can be safely reused.
func (h *AmdsGrpcHook) resetLogEntry(entry *logsproto.VeleroLogEntry) {
	entry.Level = ""
	entry.Message = ""
	entry.Timestamp = nil
	for k := range entry.Fields {
		delete(entry.Fields, k)
	}
}

// connect establishes the gRPC connection and stream. Must be called with the mutex locked.
func (h *AmdsGrpcHook) connect() error {
	if time.Since(h.lastConnectionAttempt) < reconnectInterval {
		return fmt.Errorf("reconnect backoff active")
	}
	h.lastConnectionAttempt = time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, grpcServerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("did not connect: %w", err)
	}

	client := logsproto.NewVeleroLogStreamerClient(conn)
	stream, err := client.StreamLogs(context.Background())
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to create log stream: %w", err)
	}

	h.conn = conn
	h.stream = stream
	return nil
}

// closeStream safely closes the stream and connection. Must be called with the mutex locked.
func (h *AmdsGrpcHook) closeStream() {
	if h.stream != nil {
		h.stream.CloseSend()
		h.stream = nil
	}
	if h.conn != nil {
		h.conn.Close()
		h.conn = nil
	}
}
