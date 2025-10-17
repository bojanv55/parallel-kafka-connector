## Usage

Inside the quarkus config:

```
mp.messaging.incoming.some-topic.topic=some

#name of the connector
mp.messaging.incoming.some-topic.connector=parallel-kafka

#number of parallel consumers 
mp.messaging.incoming.some-topic.concurrency=50

#lock on key/partition or no lock at all
mp.messaging.incoming.some-topic.ordering=io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED

#when to commit
mp.messaging.incoming.some-topic.commit-mode=io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS

mp.messaging.incoming.some-topic.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
mp.messaging.incoming.some-topic.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```