package me.vukas.parallel;

import io.confluent.parallelconsumer.internal.DrainingCloseable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.MultiEmitterProcessor;
import io.smallrye.mutiny.subscription.MultiEmitter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

public class ParallelKafkaDistributor<K, V> {
    private final List<MultiEmitter<? super EmitterConsumerRecord<K, V>>> subscribers = new CopyOnWriteArrayList<>();
    private final AtomicInteger nextIndex = new AtomicInteger(0);
    private final AtomicBoolean started = new AtomicBoolean(false);

    private final AtomicReference<ProcessorConsumer<K, V>> pcRef;
    private final Set<String> topics;
    private final Pattern pattern;
    private final Map<TopicPartition, Optional<Long>> offsetSeeks;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public ParallelKafkaDistributor(AtomicReference<ProcessorConsumer<K, V>> pcRef, Set<String> topics, Pattern pattern, Map<TopicPartition, Optional<Long>> offsetSeeks) {
        this.pcRef = pcRef;
        this.topics = topics;
        this.pattern = pattern;
        this.offsetSeeks = offsetSeeks;
    }

    public Multi<EmitterConsumerRecord<K, V>> createMulti() {

        //run monitoring
        executor.scheduleAtFixedRate(() -> {
            var p = pcRef.get().processor();
            if(p.isClosedOrFailed()){
                log.info("Processor is closed or failed. Attempting to restart...");
                try{
                    p.close(Duration.ofSeconds(5), DrainingCloseable.DrainingMode.DRAIN);
                }
                catch (Exception e){
                    log.warn("Failed to close", e);
                }

                try{
                    pcRef.get().recreate(topics, pattern, offsetSeeks, this::emit); //recreate all
                    log.info("Parallel consumer restarted");
                }
                catch (Exception e){
                    log.warn("Failed to restart", e);
                }
            }
        }, 0, 1, TimeUnit.SECONDS);

        return Multi.createFrom().emitter(emitter -> {
            subscribers.add(emitter);

            if (started.compareAndSet(false, true)) {
                //init conusmer and processor
                pcRef.get().recreate(topics, pattern, offsetSeeks, this::emit);
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
