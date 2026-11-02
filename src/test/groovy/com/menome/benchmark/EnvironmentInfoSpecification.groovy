package com.menome.benchmark

import spock.lang.Specification

class EnvironmentInfoSpecification extends Specification {

    def "the harness refuses to run unless the Neo4j environment is declared"() {
        when:
        EnvironmentInfo.checkDeclared(new BenchmarkRunner.Options())

        then:
        IllegalArgumentException e = thrown()
        e.message.contains("--container")
        e.message.contains("--server-hardware")
    }

    def "a container name or a hardware description is enough to run"() {
        when:
        EnvironmentInfo.checkDeclared(new BenchmarkRunner.Options(container: container, serverHardware: hardware))

        then:
        noExceptionThrown()

        where:
        container     | hardware
        "neo4j-bench" | null
        null          | "m6i.2xlarge, 8 vCPU, 32 GB, gp3 NVMe"
    }

    def "options parse the environment flags"() {
        when:
        BenchmarkRunner.Options options = BenchmarkRunner.parse(["--container=neo4j-bench", "--server-hardware=a box", "--messages=5000"] as String[])

        then:
        options.container == "neo4j-bench"
        options.serverHardware == "a box"
    }

    def "an unknown container is a refusal, not a report with a hole in it"() {
        when:
        EnvironmentInfo.containerLines("no-such-container-${UUID.randomUUID()}")

        then:
        IllegalStateException e = thrown()
        e.message.startsWith("Refusing to run")
    }
}
