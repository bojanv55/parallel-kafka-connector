package me.vukas.parallel;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.*;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.smallrye.reactive.messaging.kafka.tracing.KafkaOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.kafka.tracing.KafkaTrace;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Vertx;
import jakarta.enterprise.inject.Instance;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions.ex;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;
import static io.smallrye.reactive.messaging.kafka.impl.KafkaSource.getOffsetSeeks;

public class ParallelKafkaSource<K, V> {

    private final Multi<IncomingKafkaRecord<K, V>> stream;
    private final KafkaConnectorIncomingConfiguration configuration;
    private final boolean isHealthEnabled;
    private final boolean isHealthReadinessEnabled;
    private final boolean isCloudEventEnabled;
    private final String channel;
    private volatile boolean subscribed;
    private final ParallelKafkaSourceHealth health;

    private final List<Throwable> failures = new ArrayList<>();

    private final String group;
    private final int index;
    private final Set<String> topics;

    private final boolean isTracingEnabled;

    private final ParallelKafkaConsumer<K, V> client;

    private final ContextInternal context;

    private final KafkaOpenTelemetryInstrumenter kafkaInstrumenter;

    private final Instance<KafkaConsumerRebalanceListener> consumerRebalanceListeners;

    public ParallelKafkaSource(Vertx vertx, String consumerGroup, KafkaConnectorIncomingConfiguration config, ParallelSettings parallelSettings, Instance<OpenTelemetry> openTelemetryInstance, Instance<KafkaCommitHandler.Factory> commitHandlerFactories, Instance<KafkaFailureHandler.Factory> failureHandlerFactories, Instance<KafkaConsumerRebalanceListener> consumerRebalanceListeners, KafkaCDIEvents kafkaCDIEvents, Instance<ClientCustomizer<Map<String, Object>>> configCustomizers, Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers, int index) {

        this.group = consumerGroup;
        this.index = index;
        this.consumerRebalanceListeners = consumerRebalanceListeners;
        this.topics = getTopics(config);
        String seekToOffset = config.getAssignSeek().orElse(null);

        Pattern pattern;
        if (config.getPattern()) {
            pattern = Pattern.compile(config.getTopic().orElseThrow(() -> new IllegalArgumentException("Invalid Kafka incoming configuration for channel `" + config.getChannel() + "`, `pattern` must be used with the `topic` attribute")));
            log.configuredPattern(config.getChannel(), pattern.toString());
        } else {
            log.configuredTopics(config.getChannel(), topics);
            pattern = null;
        }

        configuration = config;
        // We cannot use vertx.getOrCreate context as it would retrieve the same one everytime.
        // It associates the context with the caller thread which will always be the same.
        // So, we force the creation of different event loop context.
        context = ((VertxInternal) vertx.getDelegate()).createEventLoopContext();
        // fire consumer event (e.g. bind metrics)
        client = new ParallelKafkaConsumer<>(config, parallelSettings, configCustomizers, deserializationFailureHandlers, consumerGroup, index, this::reportFailure, getContext().getDelegate(), c -> kafkaCDIEvents.consumer().fire(c));

        if (configuration.getHealthEnabled()) {
            health = new ParallelKafkaSourceHealth(this, configuration, client, topics, pattern);
        } else {
            health = null;
        }

        isTracingEnabled = this.configuration.getTracingEnabled();
        isHealthEnabled = this.configuration.getHealthEnabled();
        isHealthReadinessEnabled = this.configuration.getHealthReadinessEnabled();
        isCloudEventEnabled = this.configuration.getCloudEvents();
        channel = this.configuration.getChannel();

        Multi<EmitterConsumerRecord<K, V>> multi;
        if (pattern != null) {
            multi = client.subscribe(pattern);
        } else {
            multi = client.subscribe(topics);
        }

        multi = multi.onSubscription().invoke(() -> {
            subscribed = true;
            final String groupId = client.get(ConsumerConfig.GROUP_ID_CONFIG);
            final String clientId = client.get(ConsumerConfig.CLIENT_ID_CONFIG);
            log.connectedToKafka(clientId, config.getBootstrapServers(), groupId, topics);
        });

        multi = multi.onFailure().invoke(t -> {
            log.unableToReadRecord(topics, t);
            reportFailure(t, false);
        });

        Multi<IncomingKafkaRecord<K, V>> incomingMulti = multi.onItem().transform(rec -> new IncomingKafkaRecord<>(rec.message(), channel, index, new KafkaCommitHandler() {
            @Override
            public <K1, V1> Uni<Void> handle(IncomingKafkaRecord<K1, V1> record) {
                //do ACK
                rec.emitter().complete(null);
                return Uni.createFrom().voidItem();
            }
        }, new KafkaFailureHandler() {
            @Override
            public <K1, V1> Uni<Void> handle(IncomingKafkaRecord<K1, V1> record, Throwable reason, Metadata metadata) {
                //do NACK
                rec.emitter().fail(reason);
                return Uni.createFrom().voidItem();
            }
        }, false, isTracingEnabled)).onItem().invoke(() -> System.out.println("EMITTING ITEM"));

        if (config.getTracingEnabled()) {
            incomingMulti = incomingMulti.onItem().invoke(record -> incomingTrace(record, false));
        }
        this.stream = incomingMulti.onFailure().invoke(t -> reportFailure(t, false));

        if (isTracingEnabled) {
            kafkaInstrumenter = KafkaOpenTelemetryInstrumenter.createForSource(openTelemetryInstance);
        } else {
            kafkaInstrumenter = null;
        }
    }

    public Multi<IncomingKafkaRecord<K, V>> getStream() {
        return stream;
    }

    io.vertx.mutiny.core.Context getContext() {
        return new io.vertx.mutiny.core.Context(context);
    }

    public static Set<String> getTopics(KafkaConnectorIncomingConfiguration config) {
        String list = config.getTopics().orElse(null);
        String top = config.getTopic().orElse(null);
        String channel = config.getChannel();
        boolean isPattern = config.getPattern();

        if (list != null && top != null) {
            throw ex.invalidTopics(channel, "topic");
        }

        if (list != null && isPattern) {
            throw ex.invalidTopics(channel, "pattern");
        }

        if (list != null) {
            String[] strings = list.split(",");
            return Arrays.stream(strings).map(String::trim).collect(Collectors.toSet());
        } else if (top != null) {
            return Collections.singleton(top);
        } else {
            return Collections.singleton(channel);
        }
    }

    public synchronized void reportFailure(Throwable failure, boolean fatal) {
        if (failure instanceof RebalanceInProgressException) {
            // Just log the failure - it will be retried
            log.failureReportedDuringRebalance(topics, failure);
            return;
        }
        log.failureReported(topics, failure);
        // Don't keep all the failures, there are only there for reporting.
        if (failures.size() == 10) {
            failures.remove(0);
        }
        failures.add(failure);

        if (fatal) {
            if (client != null) {
                client.close();
            }
        }
    }

    public void closeQuietly() {
        try {
            this.client.close();
        } catch (Throwable e) {
            log.exceptionOnClose(e);
        }

        if (health != null) {
            health.close();
        }
    }

    public boolean hasSubscribers() {
        return subscribed;
    }

    public void isAlive(HealthReport.HealthReportBuilder builder) {
        if (isHealthEnabled) {
            List<Throwable> actualFailures;
            synchronized (this) {
                actualFailures = new ArrayList<>(failures);
            }
            if (!actualFailures.isEmpty()) {
                builder.add(channel, false, actualFailures.stream().map(Throwable::getMessage).collect(Collectors.joining()));
            } else {
                builder.add(channel, true);
            }
        }

        // If health is disabled, do not add anything to the builder.
    }

    public void isReady(HealthReport.HealthReportBuilder builder) {
        // This method must not be called from the event loop.
        if (health != null && isHealthReadinessEnabled) {
            health.isReady(builder);
        }
        // If health is disabled, do not add anything to the builder.
    }

    public void isStarted(HealthReport.HealthReportBuilder builder) {
        // This method must not be called from the event loop.
        if (health != null) {
            health.isStarted(builder);
        }
        // If health is disabled, do not add anything to the builder.
    }

    public void incomingTrace(IncomingKafkaRecord<K, V> kafkaRecord, boolean insideBatch) {
        if (isTracingEnabled) {
            KafkaTrace kafkaTrace = new KafkaTrace.Builder().withPartition(kafkaRecord.getPartition()).withTopic(kafkaRecord.getTopic()).withOffset(kafkaRecord.getOffset()).withHeaders(kafkaRecord.getHeaders()).withGroupId(client.get(ConsumerConfig.GROUP_ID_CONFIG)).withClientId(client.get(ConsumerConfig.CLIENT_ID_CONFIG)).build();

            kafkaInstrumenter.traceIncoming(kafkaRecord, kafkaTrace, !insideBatch);
        }
    }

    String getConsumerGroup() {
        return group;
    }

    int getConsumerIndex() {
        return index;
    }

    Instance<KafkaConsumerRebalanceListener> getConsumerRebalanceListeners() {
        return consumerRebalanceListeners;
    }
}
