package handlers

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/behaviours"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/infra"
)

type syncEventHandler struct {
	runningKey string
	service    behaviours.Behaviour
}

func NewSyncEventHandler(service behaviours.Behaviour) infra.EventHandler {
	return &syncEventHandler{
		service:    service,
		runningKey: uuid.New().String(),
	}
}

func (e *syncEventHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (e *syncEventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (e *syncEventHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			isOk, err := e.service.OperateEvent(message)
			if err != nil {
				fmt.Println("Error executing err: ", err)
			}
			if isOk {
				session.MarkMessage(message, "")
			}
		case <-session.Context().Done():
			return nil
		}
	}
}
