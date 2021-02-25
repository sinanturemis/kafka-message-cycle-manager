package infra

import (
	"github.com/Shopify/sarama"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/configuration"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/infra/interceptors"
)

func GetKafkaClientConfig(clientConfig configuration.ClientConfig, autoOffsetResetType configuration.AutoOffsetResetType) *sarama.Config {
	parsedKafkaVersion, err := sarama.ParseKafkaVersion(clientConfig.KafkaVersion)
	if err != nil {
		panic(err)
	}

	config := sarama.NewConfig()
	config.Version = parsedKafkaVersion
	config.ClientID = clientConfig.ClientId

	//Connection
	config.Net.ReadTimeout = clientConfig.ReadTimeout
	config.Net.DialTimeout = clientConfig.DialTimeout
	config.Net.WriteTimeout = clientConfig.WriteTimeout

	//Metadata
	config.Metadata.Retry.Max = clientConfig.MetadataRetryMax
	config.Metadata.Retry.Backoff = clientConfig.MetadataRetryBackoff

	//Consumer
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = getSaramaOffset(autoOffsetResetType)
	config.Consumer.Group.Session.Timeout = clientConfig.ConsumerSessionTimeout
	config.Consumer.Group.Heartbeat.Interval = clientConfig.ConsumerHeartbeatInterval
	config.Consumer.MaxProcessingTime = clientConfig.ConsumerMaxProcessingTime
	config.Consumer.Fetch.Default = clientConfig.ConsumerFetchDefault
	config.Consumer.Interceptors = []sarama.ConsumerInterceptor{interceptors.NewCorrelationIdInterceptor()}

	//Producer
	config.Producer.Retry.Max = clientConfig.ProducerRetryMax
	config.Producer.Retry.Backoff = clientConfig.ProducerRetryBackoff
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Timeout = clientConfig.ProducerTimeout
	config.Producer.MaxMessageBytes = clientConfig.ProducerMaxMessageBytes

	return config
}

func getSaramaOffset(autoOffsetResetType configuration.AutoOffsetResetType) int64 {
	switch autoOffsetResetType {
	case configuration.OffsetNewest:
		return sarama.OffsetNewest
	case configuration.OffsetOldest:
		return sarama.OffsetOldest
	default:
		panic("Invalid AutoOffsetResetType")
	}
}
