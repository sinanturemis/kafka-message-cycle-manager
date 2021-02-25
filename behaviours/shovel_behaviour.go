package behaviours

import (
	"fmt"
	"strconv"

	"github.com/Shopify/sarama"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/infra"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/models"
)

type shovelBehaviour struct {
	recoverProducer infra.SyncProducer
	behaviourModel  models.BehaviourModel
}

func newShovelBehaviour(recoverProducer infra.SyncProducer, behaviourModel models.BehaviourModel) Behaviour {
	return &shovelBehaviour{
		recoverProducer: recoverProducer,
		behaviourModel:  behaviourModel,
	}
}

func (shovelBehaviour *shovelBehaviour) canBeMoved(currentShovelCount int) bool {
	return shovelBehaviour.behaviourModel.ShovelLimit > currentShovelCount
}

func (shovelBehaviour *shovelBehaviour) OperateEvent(message *sarama.ConsumerMessage) (bool, error) {
	shovelCount, _ := strconv.Atoi(getHeaderValueWithDefault(message, models.ShovelCountKey, string(models.DefaultShovelCount)))
	if !shovelBehaviour.canBeMoved(shovelCount) {
		return true, nil
	}
	shovelCount++
	replaceHeaderValue(message, models.ShovelCountKey, strconv.Itoa(shovelCount))
	replaceHeaderValue(message, models.RetryCountKey, strconv.Itoa(models.DefaultRetryCount))

	fmt.Printf("Shovel - value: %s \n", string(message.Value))
	err := shovelBehaviour.recoverProducer.Publish(
		shovelBehaviour.behaviourModel.Retry,
		string(message.Key[:]),
		message.Value,
		getHeaders(message),
	)

	return err == nil, err
}
