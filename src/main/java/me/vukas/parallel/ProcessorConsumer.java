package me.vukas.parallel;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.checkerframework.checker.units.qual.A;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

public final class ProcessorConsumer<K, V> {
    private MutinyVertxProcessor<K, V> processor;
    private Consumer<K, V> consumer;

    private final Map<String, Object> kafkaConfiguration;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final ParallelSettings parallelSettings;

    public ProcessorConsumer(Map<String, Object> kafkaConfiguration, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, ParallelSettings parallelSettings) {
        this.kafkaConfiguration = kafkaConfiguration;
        this.keyDeserializer = Objects.requireNonNull(keyDeserializer);
        this.valueDeserializer = Objects.requireNonNull(valueDeserializer);
        this.parallelSettings = Objects.requireNonNull(parallelSettings);
    }

    public synchronized void recreateConsumerProcessor(){
        this.consumer = new KafkaConsumer<>(kafkaConfiguration, keyDeserializer, valueDeserializer);

        ParallelConsumerOptions<K, V> options =
                ParallelConsumerOptions.<K, V>builder()
                        .consumer(consumer)
                        .ordering(parallelSettings.ordering())
                        .commitMode(parallelSettings.commitMode())
                        .maxConcurrency(parallelSettings.concurrency())
                        .build();

        int cores = Runtime.getRuntime().availableProcessors();
        VertxOptions vertxOptions = (new VertxOptions()).setWorkerPoolSize(cores);
        this.processor = new MutinyVertxProcessor<>(Vertx.vertx(vertxOptions), options);
    }

    public synchronized void recreate(Set<String> topics, Pattern pattern, java.util.function.Consumer<EmitterConsumerRecord<K, V>> emit) {
        recreateConsumerProcessor();

        this.processor.onRecord(ctxRecord -> {
            try {
                ConsumerRecord<K, V> record = ctxRecord.getSingleConsumerRecord();
                return Uni.createFrom().emitter(uniEmitter -> emit.accept(new EmitterConsumerRecord<>(uniEmitter, record)));
            } catch (Exception e) {
                log.error("Failed to create emitter", e);
                return Uni.createFrom().voidItem();
            }
        });

        // Actually subscribe to Kafka topics
        var p = this.processor;
        if (topics != null) {
            p.subscribe(topics);
        }
        if (pattern != null) {
            p.subscribe(pattern);
        }
    }

    public synchronized MutinyVertxProcessor<K, V> processor() {
        if(consumer == null) {
            recreateConsumerProcessor();
        }
        return processor;
    }

    public synchronized Consumer<K, V> consumer() {
        if(consumer == null) {
            recreateConsumerProcessor();
        }
        return consumer;
    }
}
