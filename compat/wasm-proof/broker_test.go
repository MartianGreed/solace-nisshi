package proof

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func brokerAddress(t *testing.T) string {
	t.Helper()
	address := os.Getenv("BOOTSTRAP_SERVERS")
	if address == "" {
		t.Fatal("BOOTSTRAP_SERVERS must identify the WASI broker")
	}
	return address
}

func client(t *testing.T, options ...kgo.Opt) *kgo.Client {
	t.Helper()
	defaults := []kgo.Opt{
		kgo.SeedBrokers(brokerAddress(t)),
		kgo.RequestTimeoutOverhead(3 * time.Second),
		kgo.ProduceRequestTimeout(5 * time.Second),
		kgo.RecordDeliveryTimeout(10 * time.Second),
		kgo.FetchMaxWait(50 * time.Millisecond),
	}
	cl, err := kgo.NewClient(append(defaults, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func TestKeyedRecordsSurviveConnections(t *testing.T) {
	codecs := []struct {
		name  string
		codec kgo.CompressionCodec
	}{
		{"none", kgo.NoCompression()}, {"gzip", kgo.GzipCompression()},
		{"snappy", kgo.SnappyCompression()}, {"lz4", kgo.Lz4Compression()},
		{"zstd", kgo.ZstdCompression()},
	}
	for _, codec := range codecs {
		t.Run(codec.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			topic := fmt.Sprintf("wasi-%s-%d", codec.name, time.Now().UnixNano())
			admin := client(t)
			request := kmsg.NewPtrCreateTopicsRequest()
			request.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 2, ReplicationFactor: 1}}
			raw, err := admin.Request(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			created := raw.(*kmsg.CreateTopicsResponse)
			if len(created.Topics) != 1 || created.Topics[0].ErrorCode != 0 {
				t.Fatalf("create topic: %+v", created)
			}
			duplicate, err := admin.Request(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if code := duplicate.(*kmsg.CreateTopicsResponse).Topics[0].ErrorCode; code != kerr.TopicAlreadyExists.Code {
				t.Fatalf("duplicate topic code = %d", code)
			}
			admin.Close()

			produce := func(round int) {
				producer := client(t, kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerBatchCompression(codec.codec))
				defer producer.Close()
				var records []*kgo.Record
				for partition := int32(0); partition < 2; partition++ {
					for index := 0; index < 3; index++ {
						offset := round*3 + index
						records = append(records, &kgo.Record{
							Topic: topic, Partition: partition,
							Key:     []byte(fmt.Sprintf("key-%d-%d", partition, offset)),
							Value:   []byte(fmt.Sprintf("payload-λ-%d-%d\x00", partition, offset)),
							Headers: []kgo.RecordHeader{{Key: "proof", Value: []byte(codec.name)}},
						})
					}
				}
				results := producer.ProduceSync(ctx, records...)
				if err := results.FirstErr(); err != nil {
					t.Fatal(err)
				}
				for index, record := range records {
					expected := int64(round*3 + index%3)
					if record.Offset != expected {
						t.Fatalf("produce partition %d offset %d; want %d", record.Partition, record.Offset, expected)
					}
				}
			}
			produce(0)
			produce(1)

			consumer := client(t, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {0: kgo.NewOffset().AtStart(), 1: kgo.NewOffset().AtStart()},
			}))
			next := map[int32]int64{0: 0, 1: 0}
			count := 0
			for count < 12 {
				fetches := consumer.PollRecords(ctx, 12-count)
				if errs := fetches.Errors(); len(errs) != 0 {
					t.Fatalf("fetch: %v", errs)
				}
				fetches.EachRecord(func(record *kgo.Record) {
					expectedOffset, ok := next[record.Partition]
					if !ok {
						t.Fatalf("unexpected partition %d", record.Partition)
					}
					if record.Topic != topic || record.Offset != expectedOffset {
						t.Fatalf("record topic=%q partition=%d offset=%d; want %q offset=%d", record.Topic, record.Partition, record.Offset, topic, expectedOffset)
					}
					expectedKey := []byte(fmt.Sprintf("key-%d-%d", record.Partition, expectedOffset))
					expectedValue := []byte(fmt.Sprintf("payload-λ-%d-%d\x00", record.Partition, expectedOffset))
					if !bytes.Equal(record.Key, expectedKey) || !bytes.Equal(record.Value, expectedValue) {
						t.Fatalf("record %d/%d key=%q value=%q", record.Partition, record.Offset, record.Key, record.Value)
					}
					if len(record.Headers) != 1 || record.Headers[0].Key != "proof" || string(record.Headers[0].Value) != codec.name {
						t.Fatalf("headers: %+v", record.Headers)
					}
					next[record.Partition]++
					count++
				})
				if ctx.Err() != nil {
					t.Fatal(ctx.Err())
				}
			}
			if next[0] != 6 || next[1] != 6 {
				t.Fatalf("partition counts: %v", next)
			}
			t.Logf("%s: topic created; 12 exact keyed records, headers and offsets across two partitions and separate producers/consumer", codec.name)
		})
	}
}

func TestMalformedFramesCloseOnlyTheirConnection(t *testing.T) {
	for _, length := range []int32{-1, 0, 16*1024*1024 + 1} {
		connection, err := net.DialTimeout("tcp", brokerAddress(t), 3*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := connection.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		prefix := make([]byte, 4)
		binary.BigEndian.PutUint32(prefix, uint32(length))
		if _, err := connection.Write(prefix); err != nil {
			t.Fatal(err)
		}
		response := make([]byte, 1)
		_, err = connection.Read(response)
		connection.Close()
		if err == nil {
			t.Fatalf("frame length %d received a response", length)
		}
		if networkError, ok := err.(net.Error); ok && networkError.Timeout() {
			t.Fatalf("frame length %d was not closed: %v", length, err)
		}
		if err != io.EOF {
			t.Logf("frame length %d closed: %v", length, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client(t).Ping(ctx); err != nil {
		t.Fatalf("broker failed after malformed frames: %v", err)
	}
}
