package config

import (
	"encoding/json"
	"io/ioutil"
	"unicode/utf8"

	"github.com/BurntSushi/toml"
	log "github.com/sirupsen/logrus"
)

//Config is a base for all settings specified in config file
type Config struct {
	GeneralSettings    General    `toml:"General"`
	LoggingSettings    Logging    `toml:"Logging"`
	ConnectionSettings Connection `toml:"Connection"`
	MessageSettings    Message    `toml:"Message"`
}

//General is a part of Config
type General struct {
	ConsumerMaxProcessingTime int   `toml:"ConsumerMaxProcessingTime"`
	MarkEveryMessage          int64 `toml:"MarkEveryMessage"`
	HTTPStatisticsPort        int   `toml:"HTTPStatisticsPort"`
	//Time in seconds to wait for new connection
	TCPConnectionRetryTimeout int `toml:"TCPConnectionRetryTimeout"`
	RetryCount                int `toml:"RetryCount"`
}

//Connection is a part of Config
type Connection struct {
	Brokers      []string `toml:"Brokers"`
	Version      string   `toml:"Version"`
	Group        string   `toml:"Group"`
	Topic        string   `toml:"Topic"`
	Oldest       bool     `toml:"Oldest"`
	Destination  string   `toml:"Destination"`
	FetchDefault int      `toml:"FetchDefault"`
}

//Message is a part of Config
type Message struct {
	Filter        bool   `toml:"Filter"`
	BlacklistFile string `toml:"BlacklistFile"`
}

//Logging is a part of Config
type Logging struct {
	LogLevel string `toml:"LogLevel"`
	Output   string `toml:"Output"`
}

//Parse will take config file and return Config instance
//Also checks file for correct filling and fatals if file is incorrect
//Args:
//	configFile(string) - path to config file provided as `-config` flag
//Returns:
//	Config{}
func Parse(configFile string) Config {
	logPrefix := "[config][Parse]"
	data, err := ioutil.ReadFile(configFile)
	//Check if file is avaliable
	if err != nil {
		log.Fatalf("%s Cannot find or read config file %s %v", logPrefix, configFile, err)
	}
	var conf Config
	_, err = toml.Decode(string(data), &conf)
	//Check if file is correct as TOML
	if err != nil {
		log.Fatalf("%s Cannot parse TOML config file %s %v", logPrefix, configFile, err)
	}
	//Check if any brokers provided
	if len(conf.ConnectionSettings.Brokers) == 0 {
		log.Fatalf("%s No brokers provided.", logPrefix)
	}
	//Check if topic is specified
	if utf8.RuneCountInString(conf.ConnectionSettings.Topic) == 0 {
		log.Fatalf("%s No topic specified.", logPrefix)
	}
	//Check if group is specified
	if utf8.RuneCountInString(conf.ConnectionSettings.Group) == 0 {
		log.Fatalf("%s No group specified", logPrefix)
	}
	//Check if destination is specified
	if utf8.RuneCountInString(conf.ConnectionSettings.Destination) == 0 {
		log.Fatalf("%s No destination specified", logPrefix)
	}
	return conf
}

//ToString returns a JSON as string
//Args
//	None
//Returns
//	string
func (c *Config) ToString() string {
	toReturn, _ := json.MarshalIndent(c, "", "\t")
	return string(toReturn)
}
