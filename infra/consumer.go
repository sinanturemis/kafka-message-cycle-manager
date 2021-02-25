package infra

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
)

type EventHandler interface {
	Setup(sarama.ConsumerGroupSession) error
	Cleanup(sarama.ConsumerGroupSession) error
	ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error
}

type Consumer interface {
	Subscribe(handler EventHandler) chan error
	Unsubscribe()
}

type kafkaConsumer struct {
	topic         string
	consumerGroup sarama.ConsumerGroup
}

func NewConsumer(connectionParams ConnectionModel) Consumer {
	consumerGroup, err := sarama.NewConsumerGroup(connectionParams.Brokers, connectionParams.ConsumerGroupID, connectionParams.ClientConfig)
	if err != nil {
		panic(err)
	}

	return &kafkaConsumer{
		topic:         connectionParams.Topic,
		consumerGroup: consumerGroup,
	}
}

func (consumer *kafkaConsumer) Subscribe(handler EventHandler) chan error {
	consumerError := make(chan error)
	ctx := context.Background()
	go func() {
		for {
			if err := consumer.consumerGroup.Consume(ctx, []string{consumer.topic}, handler); err != nil {
				consumerError <- err
				return
			}

			if ctx.Err() != nil {
				consumerError <- ctx.Err()
				return
			}
		}
	}()
	go func() {
		for err := range consumer.consumerGroup.Errors() {
			fmt.Println("Error from consumer group : ", err.Error())
		}
	}()
	return consumerError
}

func (consumer *kafkaConsumer) Unsubscribe() {
	if err := consumer.consumerGroup.Close(); err != nil {
		fmt.Println("Client wasn't closed ", err)
	}
	fmt.Println("Kafka consumer closed")
}
