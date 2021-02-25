package configuration

import (
	"time"
)

type KafkaConfig struct {
	Brokers      []string     `json:"brokers"`
	ClientConfig ClientConfig `json:"clientConfig"`
}

type ClientConfig struct {
	KafkaVersion              string `json:"kafkaVersion"`
	ClientId                  string `json:"clientId"`
	ReadTimeout               time.Duration
	DialTimeout               time.Duration
	WriteTimeout              time.Duration
	MetadataRetryMax          int
	MetadataRetryBackoff      time.Duration
	ConsumerSessionTimeout    time.Duration
	ConsumerHeartbeatInterval time.Duration
	ConsumerMaxProcessingTime time.Duration
	ConsumerFetchDefault      int32
	ProducerRetryMax          int
	ProducerRetryBackoff      time.Duration
	ProducerTimeout           time.Duration
	ProducerMaxMessageBytes   int
}

type TopicConfig struct {
	TopicName                 string              `json:"topicName"`
	ConsumerGroup             string              `json:"consumerGroup"`
	RetryTopicName            string              `json:"retryTopicName"`
	RetryConsumerGroup        string              `json:"retryConsumerGroup"`
	ErrorTopicName            string              `json:"errorTopicName"`
	ErrorConsumerGroup        string              `json:"errorConsumerGroup"`
	RetryLimit                int                 `json:"retryLimit"`
	RetryIntervalInMs         int                 `json:"retryIntervalInMs"`
	ShovelLimit               int                 `json:"shovelLimit"`
	ShovelRunningDurationInMs int                 `json:"shovelRunningDurationInMs"`
	ShovelIntervalInMs        int                 `json:"shovelIntervalInMs"`
	PartitionCount            int                 `json:"partitionCount"`
	ReplicationFactor         uint16              `json:"replicationFactor"`
	RetryPartitionCount       int                 `json:"retryPartitionCount"`
	RetryReplicationFactor    uint16              `json:"retryReplicationFactor"`
	ErrorPartitionCount       int                 `json:"errorPartitionCount"`
	ErrorReplicationFactor    uint16              `json:"errorReplicationFactor"`
	AutoOffsetReset           AutoOffsetResetType `json:"autoOffsetReset"`
}

type AutoOffsetResetType string

const (
	OffsetNewest AutoOffsetResetType = "OffsetNewest"
	OffsetOldest AutoOffsetResetType = "OffsetOldest"
)
