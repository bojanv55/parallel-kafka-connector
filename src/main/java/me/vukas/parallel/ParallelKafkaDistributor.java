package me.vukas.parallel;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiEmitter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

public class ParallelKafkaDistributor<K,V> {
    private final List<MultiEmitter<? super EmitterConsumerRecord<K, V>>> subscribers = new CopyOnWriteArrayList<>();
    private final AtomicInteger nextIndex = new AtomicInteger(0);
    private final AtomicBoolean started = new AtomicBoolean(false);

    private final AtomicReference<ProcessorConsumer<K, V>> pcRef;
    private final Set<String> topics;
    private final Pattern pattern;
    private final Map<TopicPartition, Optional<Long>> offsetSeeks;

    public ParallelKafkaDistributor(AtomicReference<ProcessorConsumer<K, V>> pcRef, Set<String> topics, Pattern pattern, Map<TopicPartition, Optional<Long>> offsetSeeks) {
        this.pcRef = pcRef;
        this.topics = topics;
        this.pattern = pattern;
        this.offsetSeeks = offsetSeeks;
    }

    public Multi<EmitterConsumerRecord<K, V>> createMulti() {
        return Multi.createFrom().emitter(emitter -> {
            subscribers.add(emitter);

            if (started.compareAndSet(false, true)) {
                pcRef.get().processor().onRecord(ctxRecord -> {
                    try {
                        ConsumerRecord<K, V> record = ctxRecord.getSingleConsumerRecord();
                        return Uni.createFrom().emitter(uniEmitter -> emit(new EmitterConsumerRecord<>(uniEmitter, record)));
                    } catch (Exception e) {
                        log.error("Failed to create emitter", e);
                        return Uni.createFrom().voidItem();
                    }
                });

                // Actually subscribe to Kafka topics
                var p = pcRef.get().processor();
                if(topics != null) {
                    p.subscribe(topics);
                }
                if(pattern != null) {
                    p.subscribe(pattern);
                }
                if(offsetSeeks!=null){
                    var c = pcRef.get().consumer();
                    c.assign(offsetSeeks.keySet());
                    for (Map.Entry<TopicPartition, Optional<Long>> tpOffset : offsetSeeks.entrySet()) {
                        Optional<Long> seek = tpOffset.getValue();
                        if (seek.isPresent()) {
                            long offset = seek.get();
                            if (offset == -1) {
                                c.seekToEnd(Collections.singleton(tpOffset.getKey()));
                            } else if (offset == 0) {
                                c.seekToBeginning(Collections.singleton(tpOffset.getKey()));
                            } else {
                                c.seek(tpOffset.getKey(), offset);
                            }
                        }
                    }
                }
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
