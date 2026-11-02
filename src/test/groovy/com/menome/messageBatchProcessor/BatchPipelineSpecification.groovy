package com.menome.messageBatchProcessor

import com.menome.benchmark.Workloads
import org.neo4j.driver.Driver
import org.neo4j.driver.Result
import org.neo4j.driver.Session
import org.neo4j.driver.TransactionCallback
import org.neo4j.driver.TransactionContext
import reactor.core.publisher.Flux
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import static org.awaitility.Awaitility.await

/** KANBAN E4-1 / E4-2: what the pipeline guarantees, shown with gates instead of timing. No database. */
class BatchPipelineSpecification extends Specification {

    /** Records when it is read, so a test can see that a batch has started to be parsed. */
    static class TouchList extends AbstractList<String> {
        final List<String> inner
        volatile boolean touched = false

        TouchList(List<String> inner) { this.inner = inner }

        @Override String get(int index) { touched = true; inner.get(index) }
        @Override int size() { inner.size() }
    }

    /**
     * A Driver (plain proxies: it is called from several threads) whose every write transaction calls onWrite(n), n = 1, 2, ...
     * onWrite may block or throw. Counts transactions running at the same moment.
     */
    static class FakeDriver {
        final AtomicInteger started = new AtomicInteger()
        final AtomicInteger running = new AtomicInteger()
        final AtomicInteger maxRunning = new AtomicInteger()
        final Queue<String> writerThreads = new ConcurrentLinkedQueue<>()
        Closure onWrite = { int n -> }

        Driver driver() {
            Result result = [consume: { null }] as Result
            TransactionContext tx = [run: { Object... args -> result }] as TransactionContext
            Session session = [executeWrite: { Object... args ->
                int n = started.incrementAndGet()
                int now = running.incrementAndGet()
                maxRunning.accumulateAndGet(now, { a, b -> Math.max(a, b) })
                writerThreads.add(Thread.currentThread().name)
                try {
                    onWrite(n)
                    ((TransactionCallback) args[0]).execute(tx)
                } finally {
                    running.decrementAndGet()
                }
            }, close: { }] as Session
            [session: { Object... args -> session }, close: { }] as Driver
        }
    }

    private static List<BatchPipeline.Batch<Integer>> batches(int count, int size = 20) {
        List<String> all = Workloads.build("node-only", count * size, 1).messages
        (0..<count).collect { int i -> new BatchPipeline.Batch<Integer>(i, all.subList(i * size, (i + 1) * size)) }
    }

    private static Flux<BatchPipeline.Processed<Integer>> pipeline(List<BatchPipeline.Batch<Integer>> input, FakeDriver fake, int parsers, int writers,
                                                                    Closure afterWrite = { token, result -> }, Closure onFailure = { token, failure -> }) {
        BatchPipeline.run(Flux.fromIterable(input), fake.driver(), null, parsers, writers,
                { token, result -> afterWrite(token, result) } as java.util.function.BiConsumer,
                { token, failure -> onFailure(token, failure) } as java.util.function.BiConsumer, Duration.ofMillis(10))
    }

    def "every batch is written and its result handed on, in order"() {
        given:
        FakeDriver fake = new FakeDriver()

        when:
        List<BatchPipeline.Processed<Integer>> results = pipeline(batches(8), fake, 1, 1).collectList().block(Duration.ofSeconds(20))

        then:
        results*.token == (0..<8).toList()
        results.every { it.result.batchSummary.successCount == 20 && it.result.batchSummary.errorCount == 0 }
        fake.started.get() == 8   // node-only: one write transaction per batch
    }

    def "the next batch is parsed while the previous one is still being written"() {
        given:
        CountDownLatch releaseFirstWrite = new CountDownLatch(1)
        FakeDriver fake = new FakeDriver(onWrite: { int n -> if (n == 1) releaseFirstWrite.await(20, TimeUnit.SECONDS) })
        List<BatchPipeline.Batch<Integer>> input = batches(3)
        TouchList second = new TouchList(input[1].messages)
        input[1] = new BatchPipeline.Batch<Integer>(1, second)

        when: "the first write is stuck"
        CompletableFuture<List> done = pipeline(input, fake, 1, 1).collectList().toFuture()
        await().atMost(10, TimeUnit.SECONDS).until { fake.started.get() == 1 }

        then: "batch 2 is parsed anyway, although batch 1 has not been written"
        await().atMost(10, TimeUnit.SECONDS).until { second.touched }
        releaseFirstWrite.count == 1
        !done.isDone()

        when:
        releaseFirstWrite.countDown()

        then:
        done.get(20, TimeUnit.SECONDS)*.token == [0, 1, 2]
    }

    def "with two writers two batches are written at the same time, and with one writer never"() {
        given:
        CountDownLatch bothInside = new CountDownLatch(writers)
        FakeDriver fake = new FakeDriver(onWrite: { int n ->
            bothInside.countDown()
            bothInside.await(5, TimeUnit.SECONDS)
        })

        when:
        List results = pipeline(batches(6), fake, 2, writers).collectList().block(Duration.ofSeconds(30))

        then:
        results.size() == 6
        fake.maxRunning.get() == writers

        where:
        writers << [1, 2, 3]
    }

    def "writes happen on the writer threads, not on the parser threads or the caller"() {
        given:
        FakeDriver fake = new FakeDriver()

        when:
        pipeline(batches(4), fake, 1, 2).collectList().block(Duration.ofSeconds(20))

        then:
        fake.writerThreads.every { it.startsWith("batch-write") }
    }

    def "order is kept with several parsers even when an early batch is the slowest to parse"() {
        given:
        FakeDriver fake = new FakeDriver()
        List<String> big = Workloads.build("wide", 600, 1).messages
        List<BatchPipeline.Batch<Integer>> input = [new BatchPipeline.Batch<Integer>(0, big)] + (1..5).collect {
            new BatchPipeline.Batch<Integer>(it, Workloads.build("node-only", 1, it).messages)
        }

        when:
        List<BatchPipeline.Processed<Integer>> results = pipeline(input, fake, 3, 1).collectList().block(Duration.ofSeconds(30))

        then:
        results*.token == [0, 1, 2, 3, 4, 5]
    }

    def "a batch that cannot be written is reported to onFailure and the stream carries on"() {
        given: "the second write fails like a lost connection"
        FakeDriver fake = new FakeDriver(onWrite: { int n -> if (n == 2) throw new IllegalStateException("connection lost") })
        List failed = new CopyOnWriteArrayList()
        List written = new CopyOnWriteArrayList()

        when:
        List<BatchPipeline.Processed<Integer>> results = pipeline(batches(4), fake, 1, 1,
                { token, result -> written << token }, { token, failure -> failed << [token, failure.message] }).collectList().block(Duration.ofSeconds(20))

        then:
        failed == [[1, "connection lost"]]
        written == [0, 2, 3]
        results*.token == [0, 2, 3]
    }

    def "afterWrite runs for each batch on the writer thread, before the result is emitted"() {
        given:
        FakeDriver fake = new FakeDriver()
        List<String> threads = new CopyOnWriteArrayList<>()
        List<Integer> seen = new CopyOnWriteArrayList<>()

        when:
        pipeline(batches(3), fake, 1, 1, { token, result -> threads << Thread.currentThread().name; seen << token }).collectList().block(Duration.ofSeconds(20))

        then:
        seen == [0, 1, 2]
        threads.every { it.startsWith("batch-write") }
    }

    def "a failing afterWrite (for example acking) is a failure of that batch"() {
        given:
        FakeDriver fake = new FakeDriver()
        List failed = new CopyOnWriteArrayList()

        when:
        List results = pipeline(batches(3), fake, 1, 1,
                { token, result -> if (token == 1) throw new IllegalStateException("cannot ack") }, { token, failure -> failed << token }).collectList().block(Duration.ofSeconds(20))

        then:
        failed == [1]
        results*.token == [0, 2]
    }

    def "only a few batches are read ahead of a writer that is stuck"() {
        given:
        CountDownLatch release = new CountDownLatch(1)
        FakeDriver fake = new FakeDriver(onWrite: { int n -> release.await(20, TimeUnit.SECONDS) })
        AtomicInteger pulled = new AtomicInteger()
        Flux<BatchPipeline.Batch<Integer>> source = Flux.fromIterable(batches(50)).doOnNext { pulled.incrementAndGet() }

        when:
        CompletableFuture<List> done = BatchPipeline.run(source, fake.driver(), null, 1, 1,
                { t, r -> } as java.util.function.BiConsumer, { t, f -> } as java.util.function.BiConsumer, Duration.ofMillis(10)).collectList().toFuture()
        await().atMost(10, TimeUnit.SECONDS).until { fake.started.get() == 1 }
        Thread.sleep(500)

        then: "one being written, one parsed and waiting, one more at most in flight: not the 50 on offer"
        pulled.get() <= 4

        cleanup:
        release.countDown()
        done.get(30, TimeUnit.SECONDS)
    }
}
