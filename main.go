package main

import (
	"bufio"
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"

	config "github.com/VorobevPavel-dev/kafka2carbon/Config"
)

var (
	outChannel        chan []byte
	configFile        string
	dstAddrConnection net.Conn
	consumer          Consumer
	userSession       config.Config
)

//Consumer messages you that kafka2carbon is ready
type Consumer struct {
	ready chan bool
}

func init() {
	logPrefix := "[main][Init]"
	flag.StringVar(&configFile, "config", "/etc/kafka2carbon/config.toml", "Path to config file")
	flag.Parse()

	userSession = config.Parse(configFile)

	//Preparing log target
	if userSession.LoggingSettings.Output == "stderr" {
		log.SetOutput(os.Stderr)
	} else {
		//Check if file not exists
		if _, err := os.Stat(userSession.LoggingSettings.Output); os.IsNotExist(err) {
			//If not exists - creates it
			_, err := os.Create(userSession.LoggingSettings.Output)
			if err != nil {
				log.Fatalf("%s Cannot create log file %s", logPrefix, userSession.LoggingSettings.Output)
			}
		}
		//Try to open log file
		file, err := os.Open(userSession.LoggingSettings.Output)
		if err != nil {
			log.Fatalf("%s Cannot open log file %s", logPrefix, userSession.LoggingSettings.Output)
		} else {
			log.SetOutput(file)
		}
		log.Infof("%s Log target initialized at %s", logPrefix, userSession.LoggingSettings.Output)
	}

	//Getting log level
	switch userSession.LoggingSettings.LogLevel {
	case "panic":
		log.SetLevel(log.PanicLevel)
		break
	case "fatal":
		log.SetLevel(log.FatalLevel)
		break
	case "error":
		log.SetLevel(log.ErrorLevel)
		break
	case "warn":
		log.SetLevel(log.WarnLevel)
		break
	case "info":
		log.SetLevel(log.InfoLevel)
		break
	case "debug":
		log.SetLevel(log.DebugLevel)
		break
	case "trace":
		log.SetLevel(log.TraceLevel)
		break
	}

	var err error
	dstAddrConnection = nil
	//Preparing connection to target to send data
	for i := 1; i <= userSession.GeneralSettings.RetryCount; i++ {
		dstAddrConnection, err = net.Dial("tcp", userSession.ConnectionSettings.Destination)
		log.Warnf("%s Trying to connect to destination target. Attempts left: %d", logPrefix, userSession.GeneralSettings.RetryCount-i)
		// If no error then target is avaliable
		if err == nil {
			log.Infof("%s Connection to destination target (%s) establihed", logPrefix, userSession.ConnectionSettings.Destination)
			break
		}
		time.Sleep(time.Duration(userSession.GeneralSettings.TCPConnectionRetryTimeout) * time.Second)
	}
	if dstAddrConnection == nil {
		log.Fatalf("%s Cannot connect to destination", logPrefix)
	}
	//Init main channel for gathering and sending data
	outChannel = make(chan []byte, userSession.GeneralSettings.BufferSize)
}

func main() {
	logPrefix := "[main][main]"
	//Starting function to build messages in buckets
	go buildMessage()

	//Starting a new Sarama consumer
	log.Infof("%s Starting a new Sarama consumer", logPrefix)
	saramaConfig := sarama.NewConfig()

	//If marking every message enabled
	if userSession.GeneralSettings.MarkEveryMessage > 0 {
		log.Infof("%s Mark every %d messages enabled", logPrefix, userSession.GeneralSettings.MarkEveryMessage)
	}

	//Setting max processing time
	if userSession.GeneralSettings.ConsumerMaxProcessingTime > 0 {
		log.Infof("%s Max processing time for sarama is %d", logPrefix, userSession.GeneralSettings.ConsumerMaxProcessingTime)
		saramaConfig.Consumer.MaxProcessingTime = time.Duration(userSession.GeneralSettings.ConsumerMaxProcessingTime) * time.Millisecond
	}

	//Trying to parse kafka version
	version, err := sarama.ParseKafkaVersion(userSession.ConnectionSettings.Version)
	if err != nil {
		log.Fatalf("%s Cannot parse kafka version", logPrefix)
	} else {
		saramaConfig.Version = version
	}

	//Setting FetchDefault property
	saramaConfig.Consumer.Fetch.Default = int32(userSession.ConnectionSettings.FetchDefault) * 1024 * 1024
	log.Infof("%s Fetch default is %d", logPrefix, saramaConfig.Consumer.Fetch.Default)

	//Setting offset
	if userSession.ConnectionSettings.Oldest {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
		log.Infof("%s Set offset to oldest", logPrefix)
	} else {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
		log.Infof("%s Set offset to newest", logPrefix)
	}

	//Building a consumer
	consumer := Consumer{
		ready: make(chan bool),
	}
	log.Infof("%s Sarama consumer was created", logPrefix)

	ctx, cancel := context.WithCancel(context.Background())

	//Creating a new consumer group
	client, err := sarama.NewConsumerGroup(
		userSession.ConnectionSettings.Brokers,
		userSession.ConnectionSettings.Group,
		saramaConfig,
	)
	if err != nil {
		log.Panicf("%s Cannot create new consumer group %v", logPrefix, err)
	}
	log.Infof("%s Created new consumer group", logPrefix)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(
				ctx,
				[]string{userSession.ConnectionSettings.Topic},
				&consumer,
			); err != nil {
				log.Fatal("%s Error while consuming %v", logPrefix, err)
			}
			if ctx.Err() != nil {
				log.Fatalf("%s Context error %v", logPrefix, err)
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready
	log.Infof("%s Consumer is ready", logPrefix)
	log.Debugf("%s Sarama consumer up and running\n", logPrefix)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Errorf("%s Terminating thus context is closed\n", logPrefix)
	case <-sigterm:
		log.Errorf("%s Terminating via signal\n", logPrefix)
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("%s Error closing client:\n%v\n", logPrefix, err)
	}
}

func buildMessage() {
	var messagesCount int32
	var sb strings.Builder
	for {
		message := <-outChannel
		sb.WriteString(string(message))
		// messagesCount++
		atomic.AddInt32(&messagesCount, 1)
		if messagesCount >= userSession.GeneralSettings.BucketSize {
			messagesCount = 0
			// Should bound num of goroutines
			go sendMessage(sb.String())
			sb.Reset()
		}
	}
}

func sendMessage(message string) {
	logPrefix := "[main][sendMessage]"
	// May be bad thing
	writer := bufio.NewWriter(dstAddrConnection)
	_, err := writer.Write([]byte(message))
	if err == nil {
		_ = writer.Flush()
	} else {
		//If error occures -> no connection
		for i := 1; i <= userSession.GeneralSettings.RetryCount; i++ {
			dstAddrConnection, err = net.Dial("tcp", userSession.ConnectionSettings.Destination)
			log.Warnf("%s Trying to connect to destination target. Attempts left: %d", logPrefix, userSession.GeneralSettings.RetryCount-i)
			// If no error then target is avaliable
			if err == nil {
				log.Infof("%s Connection to destination target (%s) establihed", logPrefix, userSession.ConnectionSettings.Destination)
				break
			}
			time.Sleep(time.Duration(userSession.GeneralSettings.TCPConnectionRetryTimeout) * time.Second)
		}
		if dstAddrConnection == nil {
			log.Fatalf("%s Cannot connect to destination", logPrefix)
		}
	}
}

//Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		outChannel <- message.Value
	}
	return nil
}
