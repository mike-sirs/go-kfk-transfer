package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func NewKafkaReader(t string) *kafka.Reader {
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

func NewKafkaWriter(t string, async bool) *kafka.Writer {
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
func WriterStat(w *kafka.Writer) <-chan kafka.WriterStats {
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
func ReaderStat(r *kafka.Reader) <-chan kafka.ReaderStats {
	rs := make(chan kafka.ReaderStats)
	go func() {
		for {
			rs <- r.Stats()
			time.Sleep(5 * time.Second)
		}
	}()
	return rs
}

func PrintStats(r *kafka.Reader, w *kafka.Writer) {
	rs := ReaderStat(r)
	go func() {
		for {
			r := <-rs
			fmt.Printf("Reader stats -> Messages: %d, Timeouts: %v, Errors: %d, QueueCapacity: %d\n", r.Messages, r.Timeouts, r.Errors, r.QueueCapacity)
		}
	}()

	ws := WriterStat(w)
	go func() {
		for {
			w := <-ws
			fmt.Printf("Writer stats -> Messages: %d, Timeouts: %v, Errors: %d, QueueCapacity: %d\n", w.Messages, w.WriteTimeout, w.Errors, w.QueueCapacity)
		}
	}()
}

func WriteAndCommit(ctx context.Context, w *kafka.Writer, r *kafka.Reader, m kafka.Message) error {
	err := w.WriteMessages(ctx, kafka.Message{
		Offset:  m.Offset,
		Key:     m.Key,
		Value:   m.Value,
		Headers: m.Headers,
		Time:    m.Time,
	})
	if err != nil {
		fmt.Println("Error writing msg", err)
		return err
	}
	err = r.CommitMessages(ctx, m)
	if err != nil {
		fmt.Println("Error committing msg offset", err)
		return err
	}
	return nil
}
