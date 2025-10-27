package me.vukas.parallel;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.opentelemetry.api.OpenTelemetry;
import io.quarkus.arc.Unremovable;
import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.KafkaCDIEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.smallrye.reactive.messaging.kafka.impl.ConfigHelper;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.impl.ConcurrencyConnectorConfig;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

@Unremovable
@ApplicationScoped
@Connector(ParallelKafkaConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "bootstrap.servers", alias = "kafka.bootstrap.servers", type = "string", defaultValue = "localhost:9092", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "A comma-separated list of host:port to use for establishing the initial connection to the Kafka cluster.")

@ConnectorAttribute(name = "topics", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "A comma-separating list of topics to be consumed. Cannot be used with the `topic` or `pattern` properties")
@ConnectorAttribute(name = "key.deserializer", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "The deserializer classname used to deserialize the record's key", defaultValue = "org.apache.kafka.common.serialization.StringDeserializer")
@ConnectorAttribute(name = "value.deserializer", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "The deserializer classname used to deserialize the record's value", mandatory = true)
@ConnectorAttribute(name = "group.id", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "A unique string that identifies the consumer group the application belongs to. If not set, a unique, generated id is used")
public class ParallelKafkaConnector implements InboundConnector, HealthReporter {

    public static final String CONNECTOR_NAME = "parallel-kafka";

    @Inject
    ExecutionHolder executionHolder;

    private final List<ParallelKafkaSource<?, ?>> sources = new CopyOnWriteArrayList<>();

    @Inject
    @Any
    Instance<Map<String, Object>> configurations;

    @Inject
    Instance<OpenTelemetry> openTelemetryInstance;

    @Inject
    @Any
    Instance<KafkaCommitHandler.Factory> commitHandlerFactories;

    @Inject
    @Any
    Instance<KafkaFailureHandler.Factory> failureHandlerFactories;

    @Inject
    @Any
    Instance<KafkaConsumerRebalanceListener> consumerRebalanceListeners;

    @Inject
    KafkaCDIEvents kafkaCDIEvents;

    @Inject
    @Any
    Instance<ClientCustomizer<Map<String, Object>>> configCustomizers;

    @Inject
    @Any
    Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers;

    private Vertx vertx;

    private final AtomicReference<Flow.Publisher<? extends Message<?>>> publisher = new AtomicReference<>(null);

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        sources.forEach(ParallelKafkaSource::closeQuietly);
    }

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
    }

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        return publisher.updateAndGet(existing -> {
            if (existing == null) {
                return createPublisher(config);
            } else {
                return existing;
            }
        });
    }

    private Flow.Publisher<? extends Message<?>> createPublisher(Config config) {
        Config channelConfiguration = ConfigHelper.retrieveChannelConfiguration(configurations, config);

        var concurrency = ConcurrencyConnectorConfig.getConcurrency(channelConfiguration);
        var ordering = ParallelConnectorConfig.getOrdering(channelConfiguration);
        var commitMode = ParallelConnectorConfig.getCommitMode(channelConfiguration);

        ParallelSettings parallelSettings = new ParallelSettings(
                concurrency.orElse(1),
                ordering.orElse(ParallelConsumerOptions.ProcessingOrder.KEY)
                , commitMode.orElse(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC));

        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(channelConfiguration);

        String group = ic.getGroupId().orElseGet(() -> {
            String s = UUID.randomUUID().toString();
            log.noGroupId(s);
            return s;
        });

        ParallelKafkaSource<Object, Object> source = new ParallelKafkaSource<>(vertx, group, ic, parallelSettings,
                openTelemetryInstance,
                commitHandlerFactories, failureHandlerFactories,
                consumerRebalanceListeners,
                kafkaCDIEvents, configCustomizers, deserializationFailureHandlers, -1);
        sources.add(source);

        return source.getStream();
    }

    @Override
    public HealthReport getStartup() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (ParallelKafkaSource<?, ?> source : sources) {
            source.isStarted(builder);
        }
        return builder.build();
    }

    @Override
    public HealthReport getReadiness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (ParallelKafkaSource<?, ?> source : sources) {
            source.isReady(builder);
        }
        return builder.build();
    }

    @Override
    public HealthReport getLiveness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (ParallelKafkaSource<?, ?> source : sources) {
            source.isAlive(builder);
        }
        return builder.build();
    }
}
