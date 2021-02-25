package configuration

import "time"

func NewKafkaConfig(brokers []string, kafkaVersion string) KafkaConfig {
	return KafkaConfig{
		Brokers:      brokers,
		ClientConfig: newClientConfig(kafkaVersion),
	}
}
func newClientConfig(kafkaVersion string) ClientConfig {
	return ClientConfig{
		KafkaVersion:              kafkaVersion,
		ClientId:                  "cosmos_kafka_consumer",
		ReadTimeout:               40 * time.Second,
		DialTimeout:               40 * time.Second,
		WriteTimeout:              40 * time.Second,
		MetadataRetryMax:          3,
		MetadataRetryBackoff:      1 * time.Second,
		ConsumerSessionTimeout:    30 * time.Second,
		ConsumerHeartbeatInterval: 3 * time.Second,
		ConsumerMaxProcessingTime: 500 * time.Millisecond,
		ConsumerFetchDefault:      2048 * 1024,
		ProducerRetryMax:          3,
		ProducerRetryBackoff:      1 * time.Second,
		ProducerTimeout:           10 * time.Second,
		ProducerMaxMessageBytes:   2000000,
	}
}

func NewSimpleTopicConfig(topicName, consumerGroup string) TopicConfig {
	return TopicConfig{
		ConsumerGroup:     consumerGroup,
		TopicName:         topicName,
		PartitionCount:    1,
		ReplicationFactor: 1,
		AutoOffsetReset:   OffsetOldest,
	}
}

func NewComplexTopicConfig(topicName, consumerGroup, retryTopicName, retryConsumerGroup, errorTopicName, errorConsumerGroup string) TopicConfig {
	return TopicConfig{
		ConsumerGroup:             consumerGroup,
		TopicName:                 topicName,
		RetryConsumerGroup:        retryConsumerGroup,
		RetryTopicName:            retryTopicName,
		ErrorConsumerGroup:        errorConsumerGroup,
		ErrorTopicName:            errorTopicName,
		RetryLimit:                1,
		ShovelLimit:               1,
		ShovelRunningDurationInMs: 10000,
		ShovelIntervalInMs:        30000,
		PartitionCount:            1,
		ReplicationFactor:         1,
		RetryIntervalInMs:         3000,
		AutoOffsetReset:           OffsetOldest,
	}
}
