package me.vukas.parallel;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.config.inject.ConfigExtension;
import io.smallrye.reactive.messaging.kafka.KafkaCDIEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.commit.*;
import io.smallrye.reactive.messaging.kafka.fault.*;
import io.smallrye.reactive.messaging.kafka.impl.KafkaClientServiceImpl;
import io.smallrye.reactive.messaging.kafka.impl.TopicPartitions;
import io.smallrye.reactive.messaging.kafka.reply.KafkaRequestReplyFactory;
import io.smallrye.reactive.messaging.kafka.reply.UUIDCorrelationIdHandler;
import io.smallrye.reactive.messaging.kafka.transactions.KafkaTransactionsFactory;
import io.smallrye.reactive.messaging.providers.MediatorFactory;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.providers.extension.*;
import io.smallrye.reactive.messaging.providers.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.providers.impl.ConnectorFactories;
import io.smallrye.reactive.messaging.providers.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.providers.locals.ContextDecorator;
import io.smallrye.reactive.messaging.providers.metrics.MetricDecorator;
import io.smallrye.reactive.messaging.providers.metrics.MicrometerDecorator;
import io.smallrye.reactive.messaging.providers.wiring.Wiring;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.BeanManager;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class WeldTestBase {

    public static Instance<KafkaCommitHandler.Factory> commitHandlerFactories = new MultipleInstance<>(
            new KafkaThrottledLatestProcessedCommit.Factory(),
            new KafkaLatestCommit.Factory(),
            new KafkaIgnoreCommit.Factory(),
            new KafkaCheckpointCommit.Factory(new SingletonInstance<>("file",
                    new FileCheckpointStateStore.Factory(UnsatisfiedInstance.instance()))));

    public static Instance<KafkaFailureHandler.Factory> failureHandlerFactories = new MultipleInstance<>(
            new KafkaFailStop.Factory(),
            new KafkaDeadLetterQueue.Factory(),
            new KafkaIgnoreFailure.Factory());

    protected Weld weld;
    protected WeldContainer container;

    @BeforeEach
    public void initWeld() {
        weld = new Weld();

        // SmallRye config
        ConfigExtension extension = new ConfigExtension();
        weld.addExtension(extension);

        weld.addBeanClass(MediatorFactory.class);
        weld.addBeanClass(MediatorManager.class);
        weld.addBeanClass(InternalChannelRegistry.class);
        weld.addBeanClass(ConnectorFactories.class);
        weld.addBeanClass(ConfiguredChannelFactory.class);
        weld.addBeanClass(ChannelProducer.class);
        weld.addBeanClass(ExecutionHolder.class);
        weld.addBeanClass(WorkerPoolRegistry.class);
        weld.addBeanClass(HealthCenter.class);
        weld.addBeanClass(Wiring.class);
        weld.addExtension(new ReactiveMessagingExtension());

        weld.addBeanClass(EmitterFactoryImpl.class);
        weld.addBeanClass(MutinyEmitterFactoryImpl.class);
        weld.addBeanClass(LegacyEmitterFactoryImpl.class);
        weld.addBeanClass(KafkaTransactionsFactory.class);
        weld.addBeanClass(KafkaRequestReplyFactory.class);
        weld.addBeanClass(UUIDCorrelationIdHandler.class);

        weld.addBeanClass(KafkaThrottledLatestProcessedCommit.Factory.class);
        weld.addBeanClass(KafkaLatestCommit.Factory.class);
        weld.addBeanClass(KafkaIgnoreCommit.Factory.class);
        weld.addBeanClass(KafkaCheckpointCommit.Factory.class);
        weld.addBeanClass(FileCheckpointStateStore.Factory.class);
        weld.addBeanClass(KafkaFailStop.Factory.class);
        weld.addBeanClass(KafkaIgnoreFailure.Factory.class);
        weld.addBeanClass(KafkaDeadLetterQueue.Factory.class);
        weld.addBeanClass(KafkaDeadLetterSerializationHandler.class);
        weld.addBeanClass(KafkaCDIEvents.class);
        weld.addBeanClass(ParallelKafkaConnector.class);
        weld.addBeanClass(KafkaConnector.class);
        weld.addBeanClass(KafkaClientServiceImpl.class);
        weld.addBeanClass(MetricDecorator.class);
        weld.addBeanClass(MicrometerDecorator.class);
        weld.addBeanClass(ContextDecorator.class);
        weld.addBeanClass(ObservationDecorator.class);
        weld.addBeanClass(OutgoingObservationDecorator.class);
        weld.addBeanClass(PausableChannelDecorator.class);
        weld.disableDiscovery();
    }

    @AfterEach
    public void stopContainer() {
        if (container != null) {
            // Explicitly close the connector
            getBeanManager().createInstance()
                    .select(ParallelKafkaConnector.class, ConnectorLiteral.of("parallel-kafka")).get().terminate(new Object());
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        TopicPartitions.clearCache();
    }

    public BeanManager getBeanManager() {
        if (container == null) {
            runApplication(new MapBasedConfig());
        }
        return container.getBeanManager();
    }

    public void addBeans(Class<?>... clazzes) {
        weld.addBeanClasses(clazzes);
    }

    public <T> T get(Class<T> clazz) {
        return getBeanManager().createInstance().select(clazz).get();
    }

    public <T> T runApplication(MapBasedConfig config, Class<T> clazz) {
        weld.addBeanClass(clazz);
        runApplication(config);
        return get(clazz);
    }

    public void runApplication(MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            MapBasedConfig.cleanup();
        }

        container = weld.initialize();
    }

    public static void addConfig(KafkaMapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            KafkaMapBasedConfig.cleanup();
        }
    }

    public HealthCenter getHealth() {
        if (container == null) {
            throw new IllegalStateException("Application not started");
        }
        return container.getBeanManager().createInstance().select(HealthCenter.class).get();
    }

    public boolean isStarted() {
        return getHealth().getStartup().isOk();
    }

    public boolean isReady() {
        return getHealth().getReadiness().isOk();
    }

    public boolean isAlive() {
        return getHealth().getLiveness().isOk();
    }

}
