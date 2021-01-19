package main

import (
	"flag"
	"net"
)

var (
	outChannel        chan []byte
	configFile        string
	dstAddrConnection net.Conn
	consumer          Consumer
)

//Consumer messages you that kafka2carbon is ready
type Consumer struct {
	ready chan bool
}

func init() {
	flag.StringVar(&configFile, "config", "/etc/kafka2carbon/config.toml", "Path to config file")
	flag.Parse()
}
