package main

import (
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func newKafkaReader(t string) *kafka.Reader {
	fmt.Println(t)
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{*srcHost},
		GroupID:  *groupID,
		Topic:    t,
		MinBytes: 8,    // 10KB
		MaxBytes: 10e6, // 10MB
		// RetentionTime:  300 * time.Second,
		CommitInterval: time.Millisecond,
		QueueCapacity:  1000,
	})
}

func newKafkaWriter(t string, async bool) *kafka.Writer {
	fmt.Println(t)
	return &kafka.Writer{
		Addr:        kafka.TCP(*dstHost),
		Topic:       t,
		Balancer:    &kafka.LeastBytes{},
		Compression: kafka.Lz4,
		Async:       async,
		// RequiredAcks: kafka.RequireOne,
		BatchSize:    1,
		BatchTimeout: 2 * time.Second,
	}
}

// Check the writer status every 5 seconds
func writerStat(w *kafka.Writer) <-chan kafka.WriterStats {
	ws := make(chan kafka.WriterStats)
	go func() {
		for {
			ws <- w.Stats()
			time.Sleep(5 * time.Second)
		}
	}()
	return ws
}

// Check the reader status every 5 seconds
func readerStat(r *kafka.Reader) <-chan kafka.ReaderStats {
	rs := make(chan kafka.ReaderStats)
	go func() {
		for {
			rs <- r.Stats()
			time.Sleep(5 * time.Second)
		}
	}()
	return rs
}
