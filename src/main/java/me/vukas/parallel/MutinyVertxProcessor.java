package me.vukas.parallel;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.vertx.VertxParallelEoSStreamProcessor;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.function.Function;

import static io.vertx.core.Promise.promise;

public class MutinyVertxProcessor<K, V> extends VertxParallelEoSStreamProcessor<K, V> {
    private final Vertx vertx;

    public MutinyVertxProcessor(Vertx vertx, ParallelConsumerOptions<K, V> options) {
        super(vertx, null, options);
        this.vertx = vertx;
    }

    public <T> void onRecord(Function<PollContext<K, V>, Uni<T>> mutinyFunction) {
        Function<PollContext<K, V>, Future<?>> wrapped = pc -> uniToVertxFuture(mutinyFunction.apply(pc));
        vertxFuture(wrapped);
    }

    private <T> Future<T> uniToVertxFuture(Uni<T> uni) {
        Promise<T> promise = promise();
        vertx.getOrCreateContext().runOnContext(v ->
                uni.subscribe().with(
                        promise::complete,
                        promise::fail
                )
        );
        return promise.future();
    }
}
