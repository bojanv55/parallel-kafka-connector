package me.vukas.parallel;

import io.smallrye.mutiny.subscription.UniEmitter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public record EmitterConsumerRecord<K,V>(UniEmitter<?> emitter, ConsumerRecord<K, V> message) {}
