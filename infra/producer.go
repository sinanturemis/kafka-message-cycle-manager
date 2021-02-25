package infra

import (
	"github.com/Shopify/sarama"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/configuration"
)

type MessageHeader struct {
	Key   string
	Value string
}
type SyncProducer interface {
	Publish(topic string, key string, payload []byte, headers []MessageHeader) error
	Close() error
}

type kafkaSyncProducer struct {
	syncProducer sarama.SyncProducer
}

func NewSyncProducer(config configuration.KafkaConfig) SyncProducer {
	clientConfig := GetKafkaClientConfig(config.ClientConfig, configuration.OffsetOldest)
	syncProducer, err := sarama.NewSyncProducer(config.Brokers, clientConfig)
	if err != nil {
		panic(err)
	}

	return kafkaSyncProducer{
		syncProducer: syncProducer,
	}
}

func (producer kafkaSyncProducer) Publish(topic string, key string, payload []byte, headers []MessageHeader) error {
	message := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.ByteEncoder(payload),
		Headers: getSaramaMessageHeaders(headers),
	}
	_, _, err := producer.syncProducer.SendMessage(message)
	return err
}

func getSaramaMessageHeaders(headers []MessageHeader) []sarama.RecordHeader {
	saramaHeaders := make([]sarama.RecordHeader, 0)
	for _, header := range headers {
		saramaHeaders = append(saramaHeaders, sarama.RecordHeader{Key: []byte(header.Key), Value: []byte(header.Value)})
	}
	return saramaHeaders
}

func (producer kafkaSyncProducer) Close() error {
	return producer.syncProducer.Close()
}
