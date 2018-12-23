package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/bytewatch/dolphinbeat/sink/kafka/client/go"
	"gopkg.in/Shopify/sarama.v1"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	gBrokerList     = flag.String("b", "", "Kafka broker list")
	gTopic          = flag.String("t", "", "Kafka topic name")
	gPartition      = flag.Int64("p", 0, "Kafka topic partition")
	gOffset         = flag.Int64("o", -1, "Kafka partition offset start to consume")
	gCount          = flag.Int64("c", 0, "Kafka message count to consume")
	gSeq            = flag.Uint64("s", 0, "The last seq number until kafka message at offset-1")
	gSkipColumnName = flag.Bool("scn", false, "Don't print column names in results")
	gSkipOffset     = flag.Bool("so", false, "Don't print kafka offset info in results")
	gPretty         = flag.Bool("pretty", false, "Print pretty json in results")
)

func main() {
	flag.Parse()
	if *gBrokerList == "" || *gTopic == "" {
		flag.Usage()
		return
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	inputCh := make(chan *client.InputArg, 1024)
	cfg := client.Config{InputCh: inputCh, LastCommitSeq: *gSeq}
	decoder, err := client.NewOperationDecoder(&cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "new decoder error: %s\n", err)
		return
	}

	consumer, err := sarama.NewConsumer(strings.Split(*gBrokerList, ","), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "new consumer error: %s\n", err)
		return
	}
	partitionConsumer, err := consumer.ConsumePartition(*gTopic, int32(*gPartition), *gOffset)
	if err != nil {
		fmt.Fprintf(os.Stderr, "new partition consumer error: %s\n", err)
		return
	}

	if !*gSkipColumnName {
		if *gSkipOffset {
			fmt.Printf("operation\n")
		} else {
			fmt.Printf("seq\toffset\t\toperation\n")
		}
	}

	count := 0
	messagesCh := partitionConsumer.Messages()
	for {
		select {
		case msg := <-messagesCh:
			count++
			inputArg := client.InputArg{Data: msg.Value, Offset: msg.Offset}
			select {
			case inputCh <- &inputArg:
			case <-sigCh:
				return
			}
			if int64(count) == *gCount {
				close(inputCh)
				messagesCh = nil
			}

		case outputResult := <-decoder.Output():
			if outputResult == nil {
				// Decoder quited
				if err = <-decoder.Err(); err != nil {
					fmt.Fprintf(os.Stderr, "decoder quited with error: %s\n", err)
					return
				}
				// We have decode all kafka msgs
				return
			}
			for _, op := range outputResult.Ops {
				var buf []byte
				if *gPretty {
					buf, err = json.MarshalIndent(op, "", "  ")
				} else {
					buf, err = json.Marshal(op)
				}
				if err != nil {
					fmt.Fprintf(os.Stderr, "json marshal error: %s\n", err)
					return
				}
				if *gSkipOffset {
					fmt.Printf("%s\n", buf)
				} else {
					fmt.Printf("%d-%d\t%d-%d\t\t%s\n", outputResult.BeginSeq, outputResult.EndSeq, outputResult.BeginOffset, outputResult.EndOffset, buf)
				}

			}

		case <-sigCh:
			return
		}
	}

	return

}
