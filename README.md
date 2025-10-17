## Usage

Inside the quarkus config:

```
mp.messaging.incoming.some-topic.topic=some
mp.messaging.incoming.some-topic.connector=parallel-kafka #name of the connector
mp.messaging.incoming.some-topic.concurrency=50 #number of parallel consumers
mp.messaging.incoming.some-topic.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
mp.messaging.incoming.some-topic.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```