package me.vukas.parallel;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiEmitter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

public class ParallelKafkaDistributor<K,V> {
    private final List<MultiEmitter<? super EmitterConsumerRecord<K, V>>> subscribers = new CopyOnWriteArrayList<>();
    private final AtomicInteger nextIndex = new AtomicInteger(0);
    private final AtomicBoolean started = new AtomicBoolean(false);

    private final MutinyVertxProcessor<K, V> processor;
    private final Set<String> topics;

    public ParallelKafkaDistributor(MutinyVertxProcessor<K, V> processor, Set<String> topics) {
        this.processor = processor;
        this.topics = topics;
    }

    public Multi<EmitterConsumerRecord<K, V>> createMulti() {
        return Multi.createFrom().emitter(emitter -> {
            subscribers.add(emitter);

            if (started.compareAndSet(false, true)) {
                processor.onRecord(ctxRecord -> {
                    try {
                        ConsumerRecord<K, V> record = ctxRecord.getSingleConsumerRecord();
                        return Uni.createFrom().emitter(uniEmitter -> emit(new EmitterConsumerRecord<>(uniEmitter, record)));
                    } catch (Exception e) {
                        log.error("Failed to create emitter", e);
                        return Uni.createFrom().voidItem();
                    }
                });

                // Actually subscribe to Kafka topics
                processor.subscribe(topics);
            }

            emitter.onTermination(() -> subscribers.remove(emitter));
        });
    }

    private void emit(EmitterConsumerRecord<K, V> item) {
        if (subscribers.isEmpty()) {
            log.warn("No subscribers, dropping message.");
            return;
        }
        int index = Math.floorMod(nextIndex.getAndIncrement(), subscribers.size());
        subscribers.get(index).emit(item);
    }
}
