package behaviours

import (
	"strconv"

	"github.com/Shopify/sarama"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/executor"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/infra"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/models"
)

type Behaviour interface {
	OperateEvent(message *sarama.ConsumerMessage) (bool, error)
}

func NewBehaviour(producer infra.SyncProducer, behaviourModel models.BehaviourModel, executor executor.LogicExecutor, behaviourType models.ConsumerBehaviourType) Behaviour {
	switch behaviourType {
	case models.MainBehaviour:
		return newMainBehaviour(producer, behaviourModel, executor)
	case models.RetryBehaviour:
		return newRetryBehaviour(producer, behaviourModel, executor)
	case models.ErrorBehaviour:
		return newShovelBehaviour(producer, behaviourModel)
	default:
		panic("Invalid Behaviour Type")
	}
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

func setTraceHeader(message *sarama.ConsumerMessage, err error, stackTrace string) {
	traceHeaders := map[string]string{
		"kafka_dlt-original-topic":          message.Topic,
		"kafka_dlt-original-partition":      string(message.Partition),
		"kafka_dlt-original-offset":         strconv.FormatInt(message.Offset, 10),
		"kafka_dlt-original-timestamp":      message.Timestamp.Format("2006-01-02 15:04:05"),
		"kafka_dlt-original-timestamp-type": "CreateTime",
		"kafka_dlt-exception-fqcn":          err.Error(),
		"kafka_dlt-exception-message":       err.Error(),
		"kafka_dlt-exception-stacktrace":    stackTrace,
	}
	for key, value := range traceHeaders {
		replaceHeaderValue(message, key, value)
	}
}

func getHeaderValue(message *sarama.ConsumerMessage, key string) string {
	for _, header := range message.Headers {
		if string(header.Key) == key {
			return string(header.Value)
		}
	}
	return ""
}

func getHeaderValueWithDefault(message *sarama.ConsumerMessage, key string, defaultValue string) string {
	for _, header := range message.Headers {
		if string(header.Key) == key {
			return string(header.Value)
		}
	}
	return defaultValue
}

func getHeaders(message *sarama.ConsumerMessage) []infra.MessageHeader {
	headers := make([]infra.MessageHeader, 0)
	for _, header := range message.Headers {
		headers = append(headers, infra.MessageHeader{Key: string(header.Key[:]), Value: string(header.Value[:])})
	}
	return headers
}

func getConsumerContext(message *sarama.ConsumerMessage) models.ConsumerContext {
	return models.ConsumerContext{
		CorrelationId: getHeaderValue(message, models.CorrelationId),
	}
}
