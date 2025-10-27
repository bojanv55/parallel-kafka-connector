package me.vukas.parallel;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.DrainingCloseable;
import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.fault.DeserializerWrapper;
import io.smallrye.reactive.messaging.kafka.impl.ConfigurationCleaner;
import io.smallrye.reactive.messaging.kafka.impl.JsonHelper;
import io.smallrye.reactive.messaging.kafka.impl.KafkaPollingThread;
import io.smallrye.reactive.messaging.kafka.impl.RuntimeKafkaSourceConfiguration;
import io.smallrye.reactive.messaging.providers.helpers.ConfigUtils;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions.ex;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

public class ParallelKafkaConsumer<K, V> {

    private final AtomicBoolean closed = new AtomicBoolean(true);

    private final Uni<ProcessorConsumer<K, V>> processorUni;
    private final AtomicReference<ProcessorConsumer<K, V>> pcRef = new AtomicReference<>();
    private final RuntimeKafkaSourceConfiguration configuration;
    private final Duration pollTimeout;
    private final String consumerGroup;

    private final ScheduledExecutorService kafkaWorker;
    private final Map<String, Object> kafkaConfiguration;

    private ParallelKafkaDistributor<K, V> distributor;

    public ParallelKafkaConsumer(KafkaConnectorIncomingConfiguration config,
                                 ParallelSettings parallelSettings,
                                 Instance<ClientCustomizer<Map<String, Object>>> configCustomizers,
                                 Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers,
                                 String consumerGroup, int index,
                                 BiConsumer<Throwable, Boolean> reportFailure,
                                 Context context,
                                 java.util.function.Consumer<Consumer<K, V>> onConsumerCreated) {
        this(getKafkaConsumerConfiguration(config, configCustomizers, consumerGroup, index),
                parallelSettings,
                createDeserializationFailureHandler(true, deserializationFailureHandlers, config),
                createDeserializationFailureHandler(false, deserializationFailureHandlers, config),
                RuntimeKafkaSourceConfiguration.buildFromConfiguration(config),
                config.getLazyClient(),
                config.getPollTimeout(),
                config.getFailOnDeserializationFailure(),
                onConsumerCreated,
                reportFailure,
                context);
    }

    public ParallelKafkaConsumer(Map<String, Object> kafkaConfiguration,
                                 ParallelSettings parallelSettings,
                                 DeserializationFailureHandler<K> keyDeserializationFailureHandler,
                                 DeserializationFailureHandler<V> valueDeserializationFailureHandler,
                                 RuntimeKafkaSourceConfiguration config,
                                 boolean lazyClient,
                                 int pollTimeout,
                                 boolean failOnDeserializationFailure,
                                 java.util.function.Consumer<Consumer<K, V>> onConsumerCreated,
                                 BiConsumer<Throwable, Boolean> reportFailure,
                                 Context context) {
        this.configuration = config;
        this.kafkaConfiguration = kafkaConfiguration;

        String keyDeserializerCN = (String) kafkaConfiguration.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        String valueDeserializerCN = (String) kafkaConfiguration.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        this.consumerGroup = (String) kafkaConfiguration.get(ConsumerConfig.GROUP_ID_CONFIG);

        if (valueDeserializerCN == null) {
            throw ex.missingValueDeserializer(config.getChannel(), config.getChannel());
        }

        Deserializer<K> keyDeserializer = new DeserializerWrapper<>(keyDeserializerCN, true,
                keyDeserializationFailureHandler, reportFailure, failOnDeserializationFailure);
        Deserializer<V> valueDeserializer = new DeserializerWrapper<>(valueDeserializerCN, false,
                valueDeserializationFailureHandler, reportFailure, failOnDeserializationFailure);

        // Configure the underlying deserializers
        keyDeserializer.configure(kafkaConfiguration, true);
        valueDeserializer.configure(kafkaConfiguration, false);

        this.pollTimeout = Duration.ofMillis(pollTimeout);

        kafkaWorker = Executors.newSingleThreadScheduledExecutor(KafkaPollingThread::new);

        processorUni = Uni.createFrom().item(() -> pcRef.updateAndGet(c -> {
                    if (c != null) {
                        return c;
                    } else {
                        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(kafkaConfiguration, keyDeserializer, valueDeserializer);

                        ParallelConsumerOptions<K, V> options =
                                ParallelConsumerOptions.<K, V>builder()
                                        .consumer(consumer)
                                        .ordering(parallelSettings.ordering())
                                        .commitMode(parallelSettings.commitMode())
                                        .maxConcurrency(parallelSettings.concurrency())
                                        .build();



                        int cores = Runtime.getRuntime().availableProcessors();
                        VertxOptions vertxOptions = (new VertxOptions()).setWorkerPoolSize(cores);
                        MutinyVertxProcessor<K, V> processor = new MutinyVertxProcessor<>(Vertx.vertx(vertxOptions), options);

                        closed.set(false);
                        return new ProcessorConsumer<>(processor, consumer);
                    }
                })).memoize().until(closed::get)
                .runSubscriptionOn(kafkaWorker);
        if (!lazyClient) {
            processorUni.await().indefinitely();
        }
    }

    @CheckReturnValue
    public Multi<EmitterConsumerRecord<K, V>> subscribe(Set<String> topics) {
        distributor = new ParallelKafkaDistributor<>(pcRef, topics, null, null);
        return distributor.createMulti();
    }

    public Multi<EmitterConsumerRecord<K, V>> subscribe(Pattern pattern) {
        distributor = new ParallelKafkaDistributor<>(pcRef, null, pattern, null);
        return distributor.createMulti();
    }

    public Multi<EmitterConsumerRecord<K, V>> assignAndSeek(Map<TopicPartition, Optional<Long>> offsetSeeks) {
        distributor = new ParallelKafkaDistributor<>(pcRef, null, null, offsetSeeks);
        return distributor.createMulti();
    }

    public void close() {
        int timeout = configuration.getCloseTimeout();
        if (closed.compareAndSet(false, true)) {
            // Interrupt processor
            MutinyVertxProcessor<K, V> processor = pcRef.get().processor();
            processor.close(Duration.ofMillis(timeout * 2L), DrainingCloseable.DrainingMode.DRAIN);
        }
    }

    private static Map<String, Object> getKafkaConsumerConfiguration(KafkaConnectorIncomingConfiguration configuration,
                                                                     Instance<ClientCustomizer<Map<String, Object>>> configInterceptors,
                                                                     String consumerGroup, int index) {
        Map<String, Object> map = new HashMap<>();
        JsonHelper.asJsonObject(configuration.config())
                .forEach(e -> map.put(e.getKey(), e.getValue().toString()));
        map.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

        if (!map.containsKey(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG)) {
            // If no backoff is set, use 10s, it avoids high load on disconnection.
            map.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "10000");
        }

        String servers = configuration.getBootstrapServers();
        if (!map.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            log.configServers(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        }

        if (!map.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            log.keyDeserializerOmitted();
            map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, configuration.getKeyDeserializer());
        }

        if (!map.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            log.disableAutoCommit(configuration.getChannel());
            map.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        }

        // Consumer id generation:
        // 1. If no client id set in the config, set it to channel name, the prefix default value is "kafka-consumer-",
        // 1. If a client id set in the config, prefix with the default value "",
        // In any case if consumer index is -1, suffix is "", otherwise, suffix the index.

        String suffix = index == -1 ? ("") : ("-" + index);
        map.compute(ConsumerConfig.CLIENT_ID_CONFIG, (k, configured) -> {
            if (configured == null) {
                String prefix = configuration.getClientIdPrefix().orElse("kafka-consumer-");
                // Case 1
                return prefix + configuration.getChannel() + suffix;
            } else {
                String prefix = configuration.getClientIdPrefix().orElse("");
                // Case 2
                return prefix + configured + suffix;
            }
        });

        ConfigurationCleaner.cleanupConsumerConfiguration(map);

        return ConfigUtils.customize(configuration.config(), configInterceptors, map);
    }

    @SuppressWarnings({ "unchecked" })
    public static <T> DeserializationFailureHandler<T> createDeserializationFailureHandler(boolean isKey,
                                                                                           Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers,
                                                                                           KafkaConnectorIncomingConfiguration configuration) {
        String name = isKey ? configuration.getKeyDeserializationFailureHandler().orElse(null)
                : configuration.getValueDeserializationFailureHandler().orElse(null);

        if (name == null) {
            return null;
        }

        Instance<DeserializationFailureHandler<?>> matching = deserializationFailureHandlers
                .select(Identifier.Literal.of(name));
        if (matching.isUnsatisfied()) {
            // this `if` block should be removed when support for the `@Named` annotation is removed
            matching = deserializationFailureHandlers.select(NamedLiteral.of(name));
            if (!matching.isUnsatisfied()) {
                ProviderLogging.log.deprecatedNamed();
            }
        }

        if (matching.isUnsatisfied()) {
            throw ex.unableToFindDeserializationFailureHandler(name, configuration.getChannel());
        } else if (matching.stream().count() > 1) {
            throw ex.unableToFindDeserializationFailureHandler(name, configuration.getChannel(),
                    (int) matching.stream().count());
        } else if (matching.stream().count() == 1) {
            return (DeserializationFailureHandler<T>) matching.get();
        } else {
            return null;
        }
    }

    public Map<String, ?> configuration() {
        return kafkaConfiguration;
    }

    public Consumer<K, V> unwrap() {
        return pcRef.get().consumer();
    }

    public Uni<Set<TopicPartition>> getAssignments() {
        return Uni.createFrom().item(unwrap().assignment());
    }

    public String get(String attribute) {
        return (String) kafkaConfiguration.get(attribute);
    }
}
