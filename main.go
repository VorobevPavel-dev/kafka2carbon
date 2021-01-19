package main

import (
	"flag"
	"net"
	"os"
	"time"

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
	outChannel = make(chan []byte)
}
