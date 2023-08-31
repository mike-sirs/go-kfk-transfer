package main

import (
	"context"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
	"gopkg.in/go-playground/pool.v3"
)

var (
	doBackup      = flag.Bool("b", false, "Backup")
	doRestore     = flag.Bool("r", false, "Restore")
	doTransfer    = flag.Bool("t", false, "Transfer")
	backupPath    = flag.String("bp", "/tmp", "Backup path")
	restorePath   = flag.String("rp", "/tmp", "Restore path to dir")
	srcHost       = flag.String("s", "", "Source kafka brocker address with port")
	dstHost       = flag.String("d", "", "Destination kafka brocker address with port.")
	srcTopic      = flag.String("st", "", "Source topic name")
	dstTopic      = flag.String("dt", "", "Destination topic name")
	groupID       = flag.String("gid", "go-kafka-transfer-0001", "Group ID")
	workers       = flag.Int("w", runtime.NumCPU(), "Number of workers.")
	retries       = flag.Int("ret", 4, "Number of retries.")
	timeStamp     = flag.Int64("ts", 0, "Timestamp, a message timestamp. The default is 0, continues replication")
	stopAtZeroLag = flag.Bool("zl", false, "Stop at consumer group ZeroLag")
	batchSize     = flag.Int("bs", 1, "BatchSize, how many messages will be buffered before being sent to a partition")
)

func restore(ctx context.Context, cancel context.CancelFunc, t string) error {
	kfkMessage := &kafka.Message{}
	files, err := os.ReadDir(*restorePath)
	if err != nil {
		return err
	}

	w := NewKafkaWriter(t, false, *batchSize)
	defer w.Close()
	defer fmt.Println("end of restore")

	ws := WriterStat(w)
	go func() {
	loop:
		for {
			select {
			case w := <-ws:
				fmt.Printf("Writer stats -> Topic: %s, Messages: %d, Timeouts: %v, Errors: %d, QueueCapacity: %d\n", w.Topic, w.Messages, w.WriteTimeout, w.Errors, w.QueueCapacity)
			case <-ctx.Done():
				break loop
			}
		}
	}()

	// files to restore
	for _, v := range files {
		if fext := strings.Split(v.Name(), "."); fext[len(fext)-1] != "gob" {
			// if not *.gob skip to the next file
			continue
		}

		f, err := os.Open(*restorePath + "/" + v.Name())
		if err != nil {
			cancel()
			return err
		}
		decoder := gob.NewDecoder(f)
		for {
			err = decoder.Decode(kfkMessage)
			if err != nil {
				if err == io.EOF {
					fmt.Printf("End of file. Done restoring: %s\n", f.Name())
					break
				}
				fmt.Println("Error reading msg", err)
				cancel()
				return err
			}
			err = w.WriteMessages(ctx, *kfkMessage)
			if err != nil {
				cancel()
				return err
			}
		}
		f.Close()
	}

	cancel()
	return nil
}

// Set timestamp(ts) to 0 for continues replication. st srcTopic, dt dstTopic.
func transfer(ctx context.Context, cancel context.CancelFunc, ts int64, st, dt string) error {
	r := NewKafkaReader(st)
	defer r.Close()
	w := NewKafkaWriter(dt, true, *batchSize)
	defer w.Close()

	PrintStats(r, w)

	if *stopAtZeroLag {
		go func() {
			time.Sleep(30 * time.Second) // Init delay to avoid false zero lag value
			for {
				// check read lag every 5 sec.
				time.Sleep(10 * time.Second)
				stat := r.Stats()
				if stat.Lag == 0 {
					fmt.Println("Consumer group read lag 0, exiting.")
					// wait 30sec to make sure queue was written
					time.Sleep(30 * time.Second)
					cancel()
				}
			}
		}()
	}

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			cancel()
			return err
		}
		if ts > 0 && m.Time.Unix() > ts {
			// it has to check every partition before exit
			continue
		}
		err = WriteAndCommit(ctx, w, r, m)
		if err != nil {
			fmt.Println("Last message info, hm:", m.HighWaterMark, "pt:", m.Partition, "offst:", m.Offset, err)
			cancel()
			return err
		}
	}
}

func backup(ctx context.Context, t string) error {
	endTime := time.Now()
	r := NewKafkaReader(t)
	defer r.Close()
	rs := ReaderStat(r)
	cctx, cancel := context.WithCancel(ctx)
	g, gctx := errgroup.WithContext(cctx)
	c := make(chan kafka.Message)

	// create dir with topic_name
	err := os.Mkdir(*backupPath+"/"+t, os.ModeDir)
	if err != nil {
		cancel()
		return err
	}

	g.Go(func() error {
		file, err := os.Create(*backupPath + "/" + t + "/" + t + "_" + endTime.Format(time.RFC3339) + ".gob")
		if err != nil {
			return err
		}
		enc := gob.NewEncoder(file)
	loop:
		for {
			select {
			case msg := <-c:
				err := enc.Encode(msg)
				if err != nil {
					return err
				}
			case <-gctx.Done():
				break loop
			}
		}
		return nil
	})

	g.Go(func() error {
		i := 0
	loop:
		for {
			select {
			case s := <-rs:
				fmt.Printf("Read stats -> Topic: %s, Messages: %d, Timeouts: %v, Errors: %d, QueueCapacity: %d, Lag: %d\n", s.Topic, s.Messages, s.Timeouts, s.Errors, s.QueueCapacity, s.Lag)
				if s.Messages == 0 {
					i++
				} else {
					i = 0
				}
				if i >= *retries {
					cancel()
					close(c)
					fmt.Printf("Cancel context after %d retries. No new messages.\n", *retries)
					break loop
				}
			case <-gctx.Done():
				break loop
			}
		}
		return nil
	})

	g.Go(func() error {
		for {
			m, err := r.FetchMessage(gctx)
			if err != nil {
				return err
			}
			if m.Time.After(endTime) {
				cancel()
				close(c)
				fmt.Printf("Cancel context, done reading %s.\n", m.Topic)
				break
			}
			err = r.CommitMessages(ctx, m)
			if err != nil {
				return err
			}
			msg := kafka.Message{
				Offset: m.Offset,
				Key:    m.Key,
				Value:  m.Value,
				Time:   m.Time,
			}
			c <- msg
		}
		return nil
	})

	return g.Wait()
}

func backupJob(ctx context.Context, t string) func(wu pool.WorkUnit) (interface{}, error) {
	return func(wu pool.WorkUnit) (interface{}, error) {
		if wu.IsCancelled() {
			return nil, nil
		}
		err := backup(ctx, t)
		return nil, err
	}
}

func backupAll(ctx context.Context, t string) error {
	p := pool.NewLimited(uint(*workers))
	b := p.Batch()

	ts, err := getTopicsByRegex(t)
	if err != nil {
		return err
	}

	for _, v := range ts {
		b.Queue(backupJob(ctx, v))
	}
	b.QueueComplete()

	var mu error
	for r := range b.Results() {
		if r.Error() != nil {
			mu = multierror.Append(mu, r.Error())
		}
	}

	return mu
}

func getTopicsByRegex(r string) ([]string, error) {
	conn, err := kafka.Dial("tcp", *srcHost)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var topics []string

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, err
	}

	m := map[string]struct{}{}
	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}

	for k := range m {
		r, err := regexp.MatchString(r, k)
		if err != nil {
			return nil, err
		}
		if r {
			topics = append(topics, k)
		}
	}
	return topics, nil
}

func waitSignals(ctx context.Context, cancel context.CancelFunc) error {
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		select {
		case s := <-c:
			fmt.Printf("Recive OS signal: %s\n", s)
			cancel()
			return nil
		case <-gctx.Done():
			return gctx.Err()
		}
	})

	return g.Wait()
}

func main() {
	flag.Parse()

	cctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(cctx)

	g.Go(func() error {
		return waitSignals(ctx, cancel)
	})

	switch {
	case *doBackup:
		g.Go(func() error {
			return backupAll(ctx, *srcTopic)
		})
	case *doRestore:
		err := restore(cctx, cancel, *dstTopic)
		if err != nil {
			log.Fatalln("Restore:", err)
		}
	case *doTransfer:
		err := transfer(ctx, cancel, *timeStamp, *srcTopic, *dstTopic)
		if err != nil {
			log.Fatalln("Transfer:", err)
		}
	default:
		fmt.Println("Use one of the following modes: backup (-b), restore (-r), transfer (-t).")
		cancel()
	}

	if err := g.Wait(); err != nil {
		log.Fatalln("Exit:", err)
	}
}
