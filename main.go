package main

import (
	"context"
	"errors"
	"flag"
	"net"
	"os"
	"os/signal"
	"sync"
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
	client            sarama.ConsumerGroup
)

func init() {
	logPrefix := "[main][Init]"
	flag.StringVar(&configFile, "config", "/etc/kafka2carbon/config.toml", "Path to config file")
	flag.Parse()

	userSession = config.Parse(configFile)

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
		file, err := os.OpenFile(userSession.LoggingSettings.Output, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			log.Fatalf("%s Cannot open log file %s", logPrefix, userSession.LoggingSettings.Output)
		} else {
			log.SetOutput(file)
		}
		defer file.Close()
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
	log.Println("%s Current messaging level: %s", logPrefix, userSession.LoggingSettings.LogLevel)

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
	//Setting close handler
	setupCloseHandler(cancel)

	//Creating a new consumer group
	client, err = sarama.NewConsumerGroup(
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
				log.Fatalf("%s Error while consuming %v", logPrefix, err)
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
}

func setupCloseHandler(cancel context.CancelFunc) {
	client.Close()
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Infof("[main][setupCloseHandler] Closing app")
		cancel()
	}()
}

func retryConnection() error {
	logPrefix := "[main][retryConnection]"
	var err error
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
		return errors.New("Cannot connect to destination")
	}
	return nil
}
