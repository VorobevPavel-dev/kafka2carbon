package config

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
	TCPConnectionRetryTimeout int   `toml:"TCPConnectionRetryTimeout"`
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
