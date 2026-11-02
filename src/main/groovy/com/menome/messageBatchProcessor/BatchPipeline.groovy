package com.menome.messageBatchProcessor

import groovy.transform.CompileStatic
import io.micrometer.core.instrument.MeterRegistry
import org.neo4j.driver.Driver
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers

import java.time.Duration
import java.util.concurrent.Callable
import java.util.function.BiConsumer
import java.util.function.Function
import java.util.function.Supplier

/**
 * Parsing and writing as two stages (KANBAN E4-1, E4-2): while a batch is being written to Neo4j the next one is already
 * being parsed, and with more than one writer several batches are written at the same time.
 *
 *   batches -> parse (parsers threads, order kept) -> write (writers threads) -> afterWrite -> results
 *
 * Parsing needs no database and writing is mostly waiting for it, so a single parser keeps a few writers busy. The
 * number of batches in flight is bounded by parsers + writers: nothing is read ahead without limit.
 *
 * With one writer batches are written in the order they arrive, so the last message for a node still wins across
 * batches. With several writers two batches that carry the same node can commit in either order, so the node ends up with
 * the properties of whichever committed last; use more than one writer only when that does not matter, or when messages
 * for one node arrive in one batch.
 *
 * T identifies a batch to the caller (the deliveries to acknowledge, say). If writing or afterWrite fails, onFailure is
 * called with the batch's token, the stage pauses for failureBackoff, and the batch produces no result: the stream goes on.
 */
@CompileStatic
class BatchPipeline {

    static class Batch<T> {
        final T token
        final List<String> messages

        Batch(T token, List<String> messages) {
            this.token = token
            this.messages = messages
        }
    }

    static class Processed<T> {
        final T token
        final MessageBatchResult result

        Processed(T token, MessageBatchResult result) {
            this.token = token
            this.result = result
        }
    }

    private static class Staged<T> {
        final Batch<T> batch
        final MessageBatchProcessor.ParsedBatch parsed

        Staged(Batch<T> batch, MessageBatchProcessor.ParsedBatch parsed) {
            this.batch = batch
            this.parsed = parsed
        }
    }

    static <T> Flux<Processed<T>> run(Flux<Batch<T>> batches, Driver driver, MeterRegistry registry, int parsers, int writers,
                                      BiConsumer<T, MessageBatchResult> afterWrite, BiConsumer<T, Throwable> onFailure, Duration failureBackoff) {
        // Per subscription, not per call: a consumer that resubscribes after an error must not meet disposed schedulers
        Flux.defer({
            Scheduler parseScheduler = Schedulers.newParallel("batch-parse", Math.max(1, parsers))
            Scheduler writeScheduler = Schedulers.newBoundedElastic(Math.max(1, writers), Integer.MAX_VALUE, "batch-write")

            batches
                    .flatMapSequential({ Batch<T> batch ->
                        Mono.fromCallable({ new Staged<T>(batch, MessageBatchProcessor.parse(batch.messages)) } as Callable<Staged<T>>)
                                .subscribeOn(parseScheduler)
                    } as Function<Batch<T>, Mono<Staged<T>>>, Math.max(1, parsers), 1)
                    .flatMap({ Staged<T> staged ->
                        Mono.fromCallable({
                            MessageBatchResult result = MessageBatchProcessor.write(staged.parsed, driver, registry)
                            afterWrite.accept(staged.batch.token, result)
                            new Processed<T>(staged.batch.token, result)
                        } as Callable<Processed<T>>)
                                .subscribeOn(writeScheduler)
                                .onErrorResume({ Throwable failure ->
                                    onFailure.accept(staged.batch.token, failure)
                                    Mono.delay(failureBackoff).then(Mono.<Processed<T>> empty())
                                } as Function<Throwable, Mono<Processed<T>>>)
                    } as Function<Staged<T>, Mono<Processed<T>>>, Math.max(1, writers), 1)
                    .doFinally({
                        parseScheduler.dispose()
                        writeScheduler.dispose()
                    })
        } as Supplier<Flux<Processed<T>>>)
    }
}
