package com.menome.benchmark

import com.menome.util.messageBuilder.Connection
import com.menome.util.messageBuilder.MessageBuilder

/**
 * Workload profiles for the benchmark harness (KANBAN E1-1). Every profile is a pure function of (count, seed), so two
 * runs of the same profile send identical messages. Everything is tagged with SOURCE_SYSTEM so it can be removed.
 */
class Workloads {

    static final String SOURCE_SYSTEM = "menome_benchmark"
    static final String PRIMARY_TYPE = "BenchNode"

    static final Map<String, String> DESCRIPTIONS = [
            "node-only"  : "(a) 1 node with 5 properties, no connections",
            "hubs3"      : "(b) 1 node + 3 relationships to 3 shared hub nodes (the sample employee message shape)",
            "hub-fanout" : "(c) 1 node + 1 relationship, every message points at the same single hub",
            "wide"       : "(d) 1 node with 41 properties + 3 relationships into pools of 5000/10/5000 nodes (Symend-like)",
            "unique-dest": "(e) 1 node + 3 relationships, each to a destination node no other message uses",
    ]

    static List<String> ids() {
        DESCRIPTIONS.keySet().toList()
    }

    /** keyOffset shifts every primary key (and, for unique-dest, every destination key) to make a batch of never-seen nodes. */
    static Workload build(String id, int count, long seed, long keyOffset = 0) {
        if (!DESCRIPTIONS.containsKey(id)) {
            throw new IllegalArgumentException("Unknown profile '$id'. Known: ${ids().join(', ')}")
        }
        Random random = new Random(seed)
        int connectionCount = 0
        List<String> messages = (0..<count).collect { int i ->
            long key = keyOffset + i
            switch (id) {
                case "node-only":
                    return message(key, narrowProperties(random), [])
                case "hubs3":
                    connectionCount = 3
                    return message(key, narrowProperties(random), [
                            connection("BenchOffice", "WORKS_IN", [BenchId: (long) random.nextInt(3)]),
                            connection("BenchProject", "WORKS_ON", [BenchId: (long) random.nextInt(3)]),
                            connection("BenchTeam", "MEMBER_OF", [BenchId: (long) random.nextInt(3)])])
                case "hub-fanout":
                    connectionCount = 1
                    return message(key, narrowProperties(random), [connection("BenchHub", "POINTS_AT", [BenchId: 0L])])
                case "wide":
                    connectionCount = 3
                    long account = random.nextInt(5000)
                    long dialer = random.nextInt(5000)
                    return message(key, wideProperties(random), [
                            connection("BenchAccount", "ENTRY_FOR_ACCOUNT", [ACCTNUM: account]),
                            connection("BenchActivity", "ACTIVITY", [ACTIVITY_CODE: (long) random.nextInt(10)]),
                            connection("BenchDialer", "DIALER", [SNAPSHOT: dialer, ACCTNUM: dialer])])
                case "unique-dest":
                    connectionCount = 3
                    return message(key, narrowProperties(random), [
                            connection("BenchOffice", "WORKS_IN", [BenchId: key]),
                            connection("BenchProject", "WORKS_ON", [BenchId: key]),
                            connection("BenchTeam", "MEMBER_OF", [BenchId: key])])
            }
        }
        new Workload(id: id, description: DESCRIPTIONS[id], nodeType: PRIMARY_TYPE, messages: messages,
                writesPerMessage: 1 + connectionCount)
    }

    private static String message(long key, Map properties, List<Connection> connections) {
        MessageBuilder.builder()
                .Name("${PRIMARY_TYPE} ${key}".toString())
                .NodeType(PRIMARY_TYPE)
                .Priority(1)
                .SourceSystem(SOURCE_SYSTEM)
                .ConformedDimensions([BenchId: key])
                .Properties(properties)
                .Connections(connections)
                .build()
                .toJSON()
    }

    private static Connection connection(String nodeType, String relType, Map dimensions) {
        Connection.builder()
                .Name("${nodeType} ${dimensions.values().join('-')}".toString())
                .NodeType(nodeType)
                .RelType(relType)
                .ForewardRel(true)
                .ConformedDimensions(dimensions)
                .Properties(["SourceSystem": SOURCE_SYSTEM])
                .build()
    }

    private static Map narrowProperties(Random random) {
        [Prop1: randomText(random), Prop2: randomText(random), Prop3: (long) random.nextInt(1000),
         Prop4: (long) random.nextInt(1000), Prop5: random.nextBoolean()]
    }

    /** 41 properties like the Symend collection event: mostly text, some whole numbers, some ISO dates. */
    private static Map wideProperties(Random random) {
        Map<String, Object> properties = [:]
        26.times { properties.put("TEXT_$it".toString(), randomText(random)) }
        5.times { properties.put("NUM_$it".toString(), (long) random.nextInt(100)) }
        10.times { properties.put("DATE_$it".toString(), "2026-10-${10 + random.nextInt(18)}T00:00:00Z".toString()) }
        properties
    }

    private static String randomText(Random random) {
        new UUID(random.nextLong(), random.nextLong()).toString()
    }
}
