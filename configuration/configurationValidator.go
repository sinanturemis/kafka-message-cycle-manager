package configuration

import (
	"encoding/json"
	"fmt"
	"github.com/thedevsaddam/govalidator"
	"strings"
)

func ValidateKafkaConfig(config *KafkaConfig) {
	validationRules := govalidator.MapData{
		"brokers":      []string{"required", "min:1", "arrayDoesntContainWhitespace"},
		"kafkaVersion": []string{"required"},
	}

	opts := govalidator.Options{
		Data:  config,
		Rules: validationRules,
	}

	validator := govalidator.New(opts)
	validationErrors := validator.ValidateStruct()

	if validationErrors != nil && len(validationErrors) > 0 {
		data, _ := json.MarshalIndent(validationErrors, "", "  ")
		panic(string(data))
	}
}

func ValidateTopicConfig(config *TopicConfig) {
	mainValidationRules := govalidator.MapData{
		"topicName":         []string{"required", "shouldNotBeWhitespace"},
		"consumerGroup":     []string{"required", "shouldNotBeWhitespace"},
		"partitionCount":    []string{"required", "min:1"},
		"replicationFactor": []string{"required", "min:1"},
		"autoOffsetReset":   []string{"required", "autoOffsetReset"},
	}

	retryValidationRules := govalidator.MapData{
		"retryConsumerGroup": []string{"required", "shouldNotBeWhitespace"},
		"retryTopicName":     []string{"required", "shouldNotBeWhitespace"},
		"retryLimit":         []string{"required", "min:1"},
		"retryIntervalInMs":  []string{"required", "min:1"},
	}

	errorValidationRules := govalidator.MapData{
		"errorConsumerGroup":        []string{"required", "shouldNotBeWhitespace"},
		"errorTopicName":            []string{"required", "shouldNotBeWhitespace"},
		"shovelLimit":               []string{"required", "min:1"},
		"shovelRunningDurationInMs": []string{"required", "min:1"},
		"shovelIntervalInMs":        []string{"required", "min:1"},
	}

	rules := mainValidationRules
	if config.RetryLimit > 0 {
		for key, value := range retryValidationRules {
			rules[key] = value
		}
	}
	if config.ShovelLimit > 0 {
		for key, value := range errorValidationRules {
			rules[key] = value
		}
	}

	opts := govalidator.Options{
		Data:  config,
		Rules: rules,
	}

	validator := govalidator.New(opts)
	validationErrors := validator.ValidateStruct()

	if validationErrors != nil && len(validationErrors) > 0 {
		data, _ := json.MarshalIndent(validationErrors, "", "  ")
		panic(string(data))
	}

}

func AddCustomConfigValidationRules() {
	govalidator.AddCustomRule("arrayDoesntContainWhitespace", arrayDoesntContainWhitespaceValidator)
	govalidator.AddCustomRule("autoOffsetReset", autoOffsetResetValidator)
	govalidator.AddCustomRule("shouldNotBeWhitespace", shouldNotBeWhitespaceValidator)
}

//Custom Validators
func arrayDoesntContainWhitespaceValidator(field string, rule string, message string, value interface{}) error {
	list := value.([]string)
	for _, listItem := range list {
		if strings.TrimSpace(listItem) == "" {
			return fmt.Errorf("the values of %s field shouldn't be empty or whitespace", field)
		}
	}
	return nil
}

func shouldNotBeWhitespaceValidator(field string, rule string, message string, value interface{}) error {
	val := value.(string)
	if val != "" && strings.TrimSpace(val) == "" {
		return fmt.Errorf("the %s field shouldn't be whitespace", field)
	}

	return nil
}

func autoOffsetResetValidator(field string, rule string, message string, value interface{}) error {
	val := value.(AutoOffsetResetType)
	switch val {
	case OffsetOldest, OffsetNewest:
		return nil
	default:
		return fmt.Errorf("the %s field should be OffsetOldest or OffsetNewest", field)
	}
}
