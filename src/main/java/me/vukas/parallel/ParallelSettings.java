package me.vukas.parallel;

import io.confluent.parallelconsumer.ParallelConsumerOptions;

public record ParallelSettings(Integer concurrency, ParallelConsumerOptions.ProcessingOrder ordering,
                               ParallelConsumerOptions.CommitMode commitMode) {
}
