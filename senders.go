package main

import (
	"bufio"
	"net"
	"strings"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

//clearBuffer reads all messages from outChannel and tries to send it
//Count of messages cannot be greater than BucketSize thus build message will run first
func clearBuffer() {
	var sb strings.Builder
	for message := range outChannel {
		sb.Write(message)
	}
	close(outChannel)
	go sendMessage(sb.String())
	sb.Reset()
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
	dstAddrConnection, err := net.Dial("tcp", userSession.ConnectionSettings.Destination)
	if err != nil {
		connErr := retryConnection()
		if connErr != nil {
			log.Fatalf("%s Cannot connect to destination", logPrefix)
		}
	}
	writer := bufio.NewWriter(dstAddrConnection)
	_, err = writer.Write([]byte(message))
	if err == nil {
		err = writer.Flush()
	} else {
		log.Errorf("%s Cannot flush buffer", logPrefix)
	}

}
