package interceptors

import (
	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-uuid"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/models"
)

type correlationIdInterceptor struct {
}

func NewCorrelationIdInterceptor() *correlationIdInterceptor {
	return &correlationIdInterceptor{}
}

func (interceptor *correlationIdInterceptor) OnConsume(message *sarama.ConsumerMessage) {
	correlationId := ""
	for _, header := range message.Headers {
		if string(header.Key) == models.CorrelationId {
			correlationId = string(header.Value)
		}
	}

	if correlationId == "" {
		correlationId, _ = uuid.GenerateUUID()
	}

	replaceHeaderValue(message, models.CorrelationId, correlationId)
}

func replaceHeaderValue(message *sarama.ConsumerMessage, key string, value string) {
	isFound := false
	for i := 0; i < len(message.Headers); i++ {
		if string(message.Headers[i].Key) == key {
			isFound = true
			message.Headers[i].Value = []byte(value)
			break
		}
	}

	if !isFound {
		message.Headers = append(message.Headers, &sarama.RecordHeader{
			Key:   []byte(key),
			Value: []byte(value),
		})
	}
}
