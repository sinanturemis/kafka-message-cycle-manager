# kafka-message-cycle-manager
kafka-message-cycle-manager arka planda [Sarama](https://github.com/Shopify/sarama/) adlı kütüphaneyi kullanarak kafka üzerinde consume-retry-shovel sürecini yöneten bir kütüphanedir.

## İçerik. 
-  Kullanımı
	- Basit bir consume-retry-shovel yapısının sağlanması
- Konfigürasyon Ayarları
	- Kafka ve Topic Konfigürasyonu
	- Client Konfigürasyonu

## Kullanımı
Bu paket, kafka'yı dinlemeye başlarken paketin içerisinde bulunan executor.LogicExecutor adlı interface'i implement eden bir pointer receiver alır. Bu interface 2 farklı function'a sahiptir. Ana topic'ten okunanlar ConsumeMain adlı fonksiyona, retry topic'ten okunanlar ConsumeRetry adlı fonksiyona aktarılır. Bu fonksiyonların dönüş tipi 'error'dur. Dönen datanın error/nil olmasına göre kendi business'ını işler.

Bu kütüphaneyi kullanan her uygulama her bir consume işlemi için şöyle bir executor yazmalıdır.
```
package mysamplepackage  
  
import (  
   "fmt"  
   "gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/executor"
   "time")  
  
type topicExecutor struct {  
   //...  
}  
  
func NewTopicExecutor() executor.LogicExecutor {  
   return &topicExecutor{}  
}  
  
func (topicExecutor *topicExecutor) Consume(value []byte) error {  
   fmt.Printf("Main topic is consumed: %s - timestamp:%s \n", string(value), time.Now())
   // Do whatever you would like with the consumed value  
   return nil  
}  
  
func (topicExecutor *topicExecutor) ConsumeRetry(value []byte) error {  
   fmt.Printf("Retry topic is consumed: %s - timestamp:%s \n", string(value), time.Now())
   // Do whatever you would like with the consumed value    
   return nil  
}
```
Bu paket consume-retry-shovel yapısını desteklemek için yazılsa da bazı farklı kullanım gereksinimlerini farklı konfigürasyonlarla karşılayabilir. 

### Basit bir consume-retry-shovel yapısı
Basit bir consume-retry-shovel işlemini gerçekleştirmek için uygulama ayağa kalkarken aşağıdaki kod bloğu çalıştırılmalıdır.

```
kafkaConfig := configuration.NewKafkaConfig(brokerList, kafkaVersion) 
// kafkaConfig := configuration.NewKafkaConfig([]string{"localhost:9092"}, "2.6.0") 

topicConfig := configuration.NewTopicConfig(mainTopicName, mainConsumerGroup, retryTopicName, retryConsumerGroup, errorTopicName, errorConsumerGroup)
// topicConfig := configuration.NewTopicConfig("mainTopicName", "mainConsumerGroup", "retryTopicName", "retryConsumerGroup", "errorTopicName", "errorConsumerGroup")  

kafkaManager := managers.NewKafkaManager(kafkaConfig)
kafkaManager.Start(topicConfig, NewTopicExecutor()) 
```

> configuration.NewKafkaConfig ve configuration.NewTopicConfig bazı varsayılan konfigürasyonları getirir. İhtiyaç duyulması durumunda bu konfigürasyonlar ezilebilir ve kafkaManager.Start fonksiyonuna ezilmiş hali verilebilir. Konfigürasyonlar hakkında detaylı bilgi Konfigürasyon başlığı altında bulunabilir.

## Konfigürasyon

### Kafka Config
...
### Topic Config
...
