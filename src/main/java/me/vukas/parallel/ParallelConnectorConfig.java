package me.vukas.parallel;

import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.smallrye.reactive.messaging.providers.impl.ConnectorConfig;
import org.eclipse.microprofile.config.Config;

import java.util.Locale;
import java.util.Optional;

public class ParallelConnectorConfig extends ConnectorConfig {
    protected ParallelConnectorConfig(String prefix, Config overall, String channel) {
        super(prefix, overall, channel);
    }

    public static Optional<ProcessingOrder> getOrdering(Config connectorConfig) {
        return connectorConfig.getOptionalValue("ordering", String.class).map(value -> {
            String simpleName = value.contains(".") ? value.substring(value.lastIndexOf('.') + 1) : value;
            return ProcessingOrder.valueOf(simpleName.toUpperCase(Locale.ROOT));
        });
    }

    public static Optional<CommitMode> getCommitMode(Config connectorConfig) {
        return connectorConfig.getOptionalValue("commit-mode", String.class).map(value -> {
            String simpleName = value.contains(".") ? value.substring(value.lastIndexOf('.') + 1) : value;
            return CommitMode.valueOf(simpleName.toUpperCase(Locale.ROOT));
        });
    }
}
