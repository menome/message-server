package com.menome.benchmark

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.menome.messageBatchProcessor.MessageBatchProcessor
import spock.lang.Specification

class WorkloadsSpecification extends Specification {

    private static JsonObject parse(String message) {
        JsonParser.parseString(message).asJsonObject
    }

    def "every profile produces messages that pass harvester schema validation"() {
        when:
        Workload workload = Workloads.build(profile, 50, 1)
        workload.messages.each { MessageBatchProcessor.validateMessage(it) }

        then:
        workload.messages.size() == 50
        workload.writesPerMessage == writes

        where:
        profile       | writes
        "node-only"   | 1
        "hubs3"       | 4
        "hub-fanout"  | 2
        "wide"        | 4
        "unique-dest" | 4
    }

    def "the same seed gives identical messages and a different seed does not"() {
        expect:
        Workloads.build("wide", 20, 7).messages == Workloads.build("wide", 20, 7).messages
        Workloads.build("wide", 20, 7).messages != Workloads.build("wide", 20, 8).messages
    }

    def "primary node keys are unique so the graph ends up with one node per message"() {
        when:
        Workload workload = Workloads.build("node-only", 100, 1)
        Set keys = workload.messages.collect { parse(it).getAsJsonObject("ConformedDimensions").get("BenchId").asLong }.toSet()

        then:
        keys.size() == 100
        workload.messages.every { parse(it).getAsJsonArray("Connections").size() == 0 }
    }

    def "wide profile carries 41 properties"() {
        expect:
        Workloads.build("wide", 5, 1).messages.every { parse(it).getAsJsonObject("Properties").size() == 41 }
    }

    def "hub-fanout points every message at one hub and unique-dest never reuses a destination"() {
        when:
        Set hubKeys = destinationKeys(Workloads.build("hub-fanout", 100, 1))
        Set uniqueKeys = destinationKeys(Workloads.build("unique-dest", 100, 1))

        then:
        hubKeys.size() == 1
        uniqueKeys.size() == 300
    }

    def "whole numbers are written as integers so they stay Long"() {
        expect:
        Workloads.build("node-only", 1, 1).messages.first().contains('"BenchId":0')
        !Workloads.build("node-only", 1, 1).messages.first().contains('"BenchId":0.0')
    }

    def "unknown profile is rejected with the list of known ones"() {
        when:
        Workloads.build("nope", 1, 1)

        then:
        IllegalArgumentException e = thrown()
        e.message.contains("hubs3")
    }

    def "percentile uses nearest rank and does not reorder its input"() {
        given:
        List<Double> values = (100..1).collect { it as double }

        expect:
        Stats.percentile(values, 50) == 50d
        Stats.percentile(values, 99) == 99d
        Stats.percentile(values, 100) == 100d
        Stats.percentile([7d], 99) == 7d
        Stats.percentile([], 50) == 0d
        values.first() == 100d
    }

    private static Set destinationKeys(Workload workload) {
        workload.messages.collectMany { String message ->
            parse(message).getAsJsonArray("Connections").collect {
                it.asJsonObject.get("NodeType").asString + it.asJsonObject.getAsJsonObject("ConformedDimensions").toString()
            }
        }.toSet()
    }
}
