package main

import "github.com/Shopify/sarama"

//Consumer messages you that kafka2carbon is ready
type Consumer struct {
	ready chan bool
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
	for {
		select {
		case <-stop:
			return nil
		default:
			for message := range claim.Messages() {
				outChannel <- message.Value
			}
		}
	}
}
