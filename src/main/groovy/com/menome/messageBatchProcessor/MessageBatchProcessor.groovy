package com.menome.messageBatchProcessor

import com.menome.message.Neo4JConnection
import com.menome.message.Neo4JNode
import com.menome.messageParser.ConvertJSonToNeo4JObjects
import com.menome.util.ApplicationConfiguration
import com.menome.util.Cypher
import com.menome.util.PreferenceType
import groovy.transform.CompileStatic
import io.micrometer.core.instrument.MeterRegistry
import org.everit.json.schema.Schema
import org.everit.json.schema.ValidationException
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject
import org.json.JSONTokener
import org.neo4j.driver.Driver
import org.neo4j.driver.Session
import org.neo4j.driver.TransactionCallback
import org.neo4j.driver.TransactionContext
import org.neo4j.driver.exceptions.ClientException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Duration

/** The hot path of the server: statically compiled; the per-message loop is plain loops, no closures. */
@CompileStatic
class MessageBatchProcessor {

    static Logger log = LoggerFactory.getLogger(MessageBatchProcessor.class)
    static Schema schema
    static boolean enableValidation = true  //Should only be used for testing purposes

    // Load the harvester schema (used to validate incoming JSON messages) from the packaged jar
    static {
        String jsonSchema = MessageBatchProcessor.getResourceAsStream("/harvester_schema.json")?.text
        if (jsonSchema != null) {
            if (ApplicationConfiguration.getString(PreferenceType.SHOW_CONNECTION_LOG_OUTPUT) == "Y") {
                log.info("Message schema validation enabled. harvester_schema.json will be used to validate all incoming messages.")
            }
            schema = SchemaLoader.load(new JSONObject(new JSONTokener(jsonSchema)))
        } else {
            log.warn("Message schema validation disabled. harvester_schema.json cannot be located.")
        }
    }

    /**
     * Writes a batch of messages to the graph.
     *
     * Messages that are invalid, or that Neo4j rejects as data errors, are reported in the result; the rest are applied.
     * Infrastructure failures (connection loss, authentication, exhausted transient retries) are NOT data errors: they
     * propagate as exceptions so the caller can retry the whole batch. Every statement is an idempotent MERGE, so
     * reprocessing a batch that partially succeeded is safe.
     *
     * This is parse() followed by write() on the calling thread. BatchPipeline runs the two on different threads so
     * that the next batch is parsed while this one is being written.
     */
    static MessageBatchResult process(List<String> messages, Driver driver, MeterRegistry registry = null) {
        write(parse(messages), driver, registry)
    }

    /** Everything parse() found out about a batch, handed from the parsing thread to the writing thread. */
    static class ParsedBatch {
        final List<String> messages
        final List<MessageError> errors = new ArrayList<>()
        final Set<Integer> failedMessages = new HashSet<Integer>()
        final StageTimings timings = new StageTimings()
        final Map<String, MessageBatchCollection> nodeGroups = new LinkedHashMap<>()
        final Map<String, MessageConnectionCollection> connectionGroups = new LinkedHashMap<>()
        /** Time parse() itself took: the parsing thread's own work, not time spent waiting for a writer. */
        long parseNanos

        ParsedBatch(List<String> messages) {
            this.messages = messages
        }
    }

    /** CPU only, no database: validates and converts every message and groups nodes and connections by signature. */
    static ParsedBatch parse(List<String> messages) {
        long parseStart = System.nanoTime()
        ParsedBatch batch = new ParsedBatch(messages)
        StageTimings timings = batch.timings
        for (int index = 0; index < messages.size(); index++) {
            String jsonMessage = messages.get(index)
            long start = System.nanoTime()
            try {
                if (enableValidation) {
                    validateMessage(jsonMessage)
                }
                Tuple2<List<Neo4JNode>, List<Neo4JConnection>> converted = ConvertJSonToNeo4JObjects.fromJson(jsonMessage, index)
                long parsedAt = System.nanoTime()
                timings.add(StageTimings.PARSE, parsedAt - start)
                start = parsedAt
                for (Neo4JNode node : converted.v1) {
                    String signature = node.signature()
                    MessageBatchCollection group = batch.nodeGroups.get(signature)
                    if (group == null) {
                        group = new MessageBatchCollection(signature)
                        batch.nodeGroups.put(signature, group)
                    }
                    group.addNode(node)
                }
                for (Neo4JConnection connection : converted.v2) {
                    String signature = connection.signature()
                    MessageConnectionCollection group = batch.connectionGroups.get(signature)
                    if (group == null) {
                        group = new MessageConnectionCollection(signature)
                        batch.connectionGroups.put(signature, group)
                    }
                    group.addConnection(connection)
                }
                timings.add(StageTimings.GROUP, System.nanoTime() - start)
            } catch (ValidationException ex) {
                timings.add(StageTimings.PARSE, System.nanoTime() - start)
                batch.errors.add(new MessageError(ex.errorMessage, jsonMessage))
                batch.failedMessages.add(index)
            } catch (Exception ex) {
                // Malformed JSON, unexpected structure... one bad message must never take down the batch
                timings.add(StageTimings.PARSE, System.nanoTime() - start)
                batch.errors.add(new MessageError(ex.toString(), jsonMessage))
                batch.failedMessages.add(index)
            }
        }
        batch.parseNanos = System.nanoTime() - parseStart
        batch
    }

    /**
     * Statements and rows go to Neo4j in a deterministic order, so that writers working on overlapping nodes at the same
     * time take their locks in the same order and do not deadlock each other (KANBAN E4-2/E3-7).
     *
     * One global order, by node label and then key, across BOTH phases: node statements are ordered by label, and
     * relationship statements by the label of the node they point at (their destination, usually the shared one), then
     * source label, then type. If the two phases disagreed (relationships by type name, say) a writer in the relationship
     * phase holding an Activity and wanting an Account could meet a writer in the node phase holding that Account and
     * wanting that Activity: a deadlock, measured as a collapse to a fifth of the throughput with two writers.
     * Groups of one label keep their first-seen order, which keeps "the last message wins" for a node that appears under
     * two property sets.
     */
    private static final Comparator<MessageBatchCollection> NODE_GROUP_ORDER = { MessageBatchCollection a, MessageBatchCollection b ->
        a.prototypeNode().nodeType.compareTo(b.prototypeNode().nodeType)
    } as Comparator<MessageBatchCollection>

    private static final Comparator<MessageConnectionCollection> CONNECTION_GROUP_ORDER = { MessageConnectionCollection a, MessageConnectionCollection b ->
        connectionOrderKey(a).compareTo(connectionOrderKey(b))
    } as Comparator<MessageConnectionCollection>

    private static final Comparator<Neo4JNode> NODE_ROW_ORDER = { Neo4JNode a, Neo4JNode b ->
        LockOrder.compare(a.identity(), b.identity())
    } as Comparator<Neo4JNode>

    /** Destination first: it is the shared (hub) end of most relationships. */
    private static final Comparator<Neo4JConnection> CONNECTION_ROW_ORDER = { Neo4JConnection a, Neo4JConnection b ->
        int byDestination = LockOrder.compare(a.destination.identity(), b.destination.identity())
        byDestination != 0 ? byDestination : LockOrder.compare(a.source.identity(), b.source.identity())
    } as Comparator<Neo4JConnection>

    /** Merges before deletes, then destination label (same order as the node phase), source label, relationship type. */
    private static String connectionOrderKey(MessageConnectionCollection group) {
        Neo4JConnection connection = group.prototypeRelationship()
        (connection.deleteRelationship ? "1|" : "0|") + connection.destination.nodeType + "|" + connection.source.nodeType + "|" + connection.relationshipName
    }

    /** Applies a parsed batch to the graph. */
    static MessageBatchResult write(ParsedBatch parsed, Driver driver, MeterRegistry registry = null) {
        long writeStart = System.nanoTime()
        StageTimings timings = parsed.timings

        Session session = driver.session()
        try {
            // Schema DDL is not part of a write transaction: make sure every merged label has its constraint first
            long indexStart = System.nanoTime()
            for (MessageBatchCollection group : parsed.nodeGroups.values()) {
                if (!group.prototypeNode().deleteNode) {
                    SchemaManager.ensure(group.prototypeNode(), session)
                }
            }
            timings.add(StageTimings.INDEX, System.nanoTime() - indexStart)

            // Each phase is ONE write transaction holding all of its statements (E3-2): a batch of the sample message
            // costs 2 commits instead of 7. If Neo4j rejects the data the transaction rolls back as a whole and the
            // phase is re-run one statement at a time to find the culprit (see executePhase).
            Context context = new Context(session: session, messages: parsed.messages, errors: parsed.errors, failedMessages: parsed.failedMessages, timings: timings)

            // 1. Merge nodes
            List<MessageBatchCollection> nodeGroups = new ArrayList<>(parsed.nodeGroups.values())
            nodeGroups.sort(NODE_GROUP_ORDER)
            List<Statement> nodeMerges = new ArrayList<>()
            List<Statement> nodeDeletes = new ArrayList<>()
            for (MessageBatchCollection group : nodeGroups) {
                List<Neo4JNode> rows = group.nodeList()
                rows.sort(NODE_ROW_ORDER)
                Statement statement = new Statement(cypher: group.prototypeNode().toCypher(), items: new ArrayList<Object>(rows), parameters: NODE_PARAMETERS)
                (group.prototypeNode().deleteNode ? nodeDeletes : nodeMerges).add(statement)
            }
            executePhase(context, nodeMerges)

            // 2. Connect nodes, then remove relationships. Connections of messages that failed above are skipped:
            //    their endpoints may not exist, and the message is already reported as failed.
            List<MessageConnectionCollection> connectionGroups = new ArrayList<>(parsed.connectionGroups.values())
            connectionGroups.sort(CONNECTION_GROUP_ORDER)
            List<Statement> relationships = new ArrayList<>()
            for (MessageConnectionCollection group : connectionGroups) {
                List<Neo4JConnection> rows = group.connectionList()
                rows.sort(CONNECTION_ROW_ORDER)
                Statement live = new Statement(cypher: group.prototypeRelationship().toCypher(), items: new ArrayList<Object>(rows),
                        skipFailedMessages: true, parameters: CONNECTION_PARAMETERS).withoutFailed(parsed.failedMessages)
                if (live.items) relationships.add(live)
            }
            if (ApplicationConfiguration.getString(PreferenceType.RELATIONSHIP_TRANSACTIONS).equalsIgnoreCase("statement")) {
                // A relationship write-locks both ends until commit. One transaction per statement keeps a hub locked only
                // while the statement that points at it runs, instead of until the whole phase has committed.
                for (Statement relationship : relationships) {
                    executePhase(context, [relationship.withoutFailed(parsed.failedMessages)].findAll { it.items } as List<Statement>)
                }
            } else {
                executePhase(context, relationships)
            }

            // 3. Delete nodes last so earlier statements in the batch still see them
            executePhase(context, nodeDeletes)
        } finally {
            session.close()
        }

        // This batch's own work: parsing plus writing, not the time it waited between the two
        Duration batchDuration = Duration.ofNanos(parsed.parseNanos + (System.nanoTime() - writeStart))
        int failed = parsed.failedMessages.size()
        MessageBatchSummary summary = new MessageBatchSummary(parsed.messages.size() - failed, failed, batchDuration, timings)
        recordMetrics(registry, summary)
        new MessageBatchResult(summary, parsed.errors)
    }

    /** What is shared by every statement of one batch. */
    private static class Context {
        Session session
        List<String> messages
        List<MessageError> errors
        Set<Integer> failedMessages
        StageTimings timings
    }

    private static final Closure<Map> NODE_PARAMETERS = { List<Object> items -> MessageBatchCollection.asParameterList((List<Neo4JNode>) (List) items) }
    private static final Closure<Map> CONNECTION_PARAMETERS = { List<Object> items -> MessageConnectionCollection.asParameterList((List<Neo4JConnection>) (List) items) }

    /** Position of the item's source message in the batch: items are Neo4JNodes or Neo4JConnections. */
    private static int sourceIndexOf(Object item) {
        item instanceof Neo4JNode ? ((Neo4JNode) item).sourceIndex : ((Neo4JConnection) item).sourceIndex
    }

    /** One `UNWIND $parms AS param <cypher>` statement over a group of nodes or connections. */
    private static class Statement {
        String cypher
        List<Object> items
        Closure<Map> parameters
        /** Connections: drop items whose message has failed meanwhile, since their endpoints may not exist. */
        boolean skipFailedMessages = false

        Statement withoutFailed(Set<Integer> failedMessages) {
            if (!skipFailedMessages) return this
            List<Object> live = new ArrayList<>(items.size())
            for (Object item : items) {
                if (!failedMessages.contains(sourceIndexOf(item))) live.add(item)
            }
            withItems(live)
        }

        Statement withItems(List<Object> newItems) {
            new Statement(cypher: cypher, items: newItems, parameters: parameters, skipFailedMessages: skipFailedMessages)
        }
    }

    /**
     * Writes every statement of a phase in one transaction. When Neo4j rejects the batch because of the data, the
     * transaction has rolled back, so nothing is half-applied; find the offending messages by re-running the phase one
     * statement at a time, each bisected on its own (executeOne). All statements are idempotent MERGEs/deletes, so the
     * re-run is safe. Anything that is not a data error is rethrown and the caller requeues the batch.
     */
    private static void executePhase(Context context, List<Statement> statements) {
        if (!statements) return
        try {
            writeTransaction(context, statements)
        } catch (ClientException exception) {
            if (!isDataError(exception)) throw exception
            if (statements.size() == 1) {
                // Known to fail as a whole: go straight to bisecting it instead of re-running it first
                isolate(context, statements.first(), exception)
            } else {
                for (Statement statement : statements) {
                    executeOne(context, statement.withoutFailed(context.failedMessages))
                }
            }
        }
    }

    /** One statement in its own transaction; bisect on a data error. */
    private static void executeOne(Context context, Statement statement) {
        if (!statement.items) return
        try {
            writeTransaction(context, [statement])
        } catch (ClientException exception) {
            if (!isDataError(exception)) throw exception
            isolate(context, statement, exception)
        }
    }

    /** `statement` is known to fail with a data error: split it until single items are left, and report those. */
    private static void isolate(Context context, Statement statement, ClientException cause) {
        if (statement.items.size() > 1) {
            for (List<Object> half : MessageBatchCollection.split(statement.items)) {
                executeOne(context, statement.withItems(half))
            }
        } else {
            Object item = statement.items.get(0)
            int index = sourceIndexOf(item)
            String sourceMessage = index >= 0 ? context.messages.get(index) : null
            context.errors.add(new MessageError(cause.toString(), sourceMessage ?: item.toString()))
            if (index >= 0) context.failedMessages.add(index)
        }
    }

    /** One write transaction (retried by the driver on transient errors) running the statements in order. */
    private static void writeTransaction(Context context, List<Statement> statements) {
        long writeStart = System.nanoTime()
        long statementNanos = 0
        int attempts = 0
        try {
            context.session.executeWrite({ TransactionContext tx ->
                attempts++
                for (Statement statement : statements) {
                    long statementStart = System.nanoTime()
                    try {
                        Map<String, Object> parameters = (Map<String, Object>) statement.parameters.call(statement.items)
                        tx.run(Cypher.versioned("UNWIND \$parms AS param " + statement.cypher), parameters).consume()
                    } finally {
                        // Summed across statements and transient retries; what executeWrite spends besides this is begin/commit/backoff
                        statementNanos += System.nanoTime() - statementStart
                    }
                }
                null
            } as TransactionCallback<Object>)
        } finally {
            // Recorded before any bisecting, so re-run halves are not counted twice
            StageTimings timings = context.timings
            timings.add(StageTimings.STATEMENT, statementNanos)
            timings.add(StageTimings.COMMIT, System.nanoTime() - writeStart - statementNanos)
            timings.retries += Math.max(0, attempts - 1)
            timings.transactions++
        }
    }

    private static final List<String> DATA_ERROR_CODE_PREFIXES = ["Neo.ClientError.Statement.", "Neo.ClientError.Schema."]
    // Raised by the driver itself, before anything is sent, so there is no server code to look at
    private static final List<String> DRIVER_VALUE_ERRORS = ["Unable to convert ", "Unsupported parameters type: "]

    /**
     * Only what is known to be caused by the message is a data error: statement and schema errors (a value Neo4j
     * cannot store, a constraint the data violates) and values the driver cannot convert. Everything else - cluster
     * errors such as NotALeader during a failover, a read-only or unavailable database, security, timeouts, and codes
     * we have not classified - is infrastructure: it propagates so the batch is requeued rather than dead-lettered.
     * An allow-list, because wrongly dead-lettering good messages loses data while wrongly requeueing only retries.
     */
    static boolean isDataError(ClientException exception) {
        String code = exception.code() ?: ""
        for (String prefix : DATA_ERROR_CODE_PREFIXES) {
            if (code.startsWith(prefix)) return true
        }
        if (code.startsWith("Neo.")) return false
        String message = exception.message ?: ""
        for (String driverError : DRIVER_VALUE_ERRORS) {
            if (message.startsWith(driverError)) return true
        }
        false
    }

    static void validateMessage(String message) {
        if (schema != null) {
            schema.validate(new JSONObject(message))
        }
    }

    private static void recordMetrics(MeterRegistry registry, MessageBatchSummary summary) {
        if (registry) {
            registry.counter("messageserver.messages.processed").increment(summary.successCount)
            registry.counter("messageserver.messages.failed").increment(summary.errorCount)
            registry.timer("messageserver.batch.duration").record(summary.batchProcessingDuration)
            // One sample per batch per stage; Prometheus: messageserver_stage_duration_seconds_{count,sum,max}{stage="..."}
            summary.stages.durations().each { String stage, Duration duration ->
                registry.timer("messageserver.stage.duration", "stage", stage).record(duration)
            }
            registry.counter("messageserver.write.transactions").increment(summary.stages.transactions)
            if (summary.stages.retries > 0) {
                registry.counter("messageserver.write.retries").increment(summary.stages.retries)
            }
        }
    }
}
