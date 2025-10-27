package me.vukas.parallel;

import org.apache.kafka.clients.consumer.Consumer;

public record ProcessorConsumer<K, V>(MutinyVertxProcessor<K, V> processor, Consumer<K, V> consumer) {
}
