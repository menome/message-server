package com.menome.benchmark

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.menome.util.ApplicationConfiguration
import com.menome.util.Cypher
import com.menome.util.Neo4J
import com.menome.util.PreferenceType
import org.neo4j.driver.Driver
import org.neo4j.driver.Session

import java.lang.management.ManagementFactory

/**
 * What a benchmark ran on (KANBAN E1-3). A throughput number without its environment cannot be compared, so the
 * runner calls capture() before writing anything and refuses to run when the server side is not described.
 */
class EnvironmentInfo {

    /** Neo4j settings that decide write throughput; reported with the value the server is actually using. */
    static final List<String> SETTINGS = [
            "server.memory.heap.initial_size", "server.memory.heap.max_size", "server.memory.pagecache.size",
            "db.tx_log.rotation.size", "db.tx_log.rotation.retention_policy", "db.checkpoint",
            "db.checkpoint.interval.time", "db.checkpoint.interval.tx", "db.checkpoint.iops.limit",
            "db.query.default_language", "dbms.cypher.transactions.default_subquery_batch_strategy",
    ].asImmutable()

    /** The report needs to know where the database runs: ask Docker, or have the operator say. */
    static void checkDeclared(BenchmarkRunner.Options options) {
        if (!options.container && !options.serverHardware) {
            throw new IllegalArgumentException(
                    "Refusing to run: the report must describe the Neo4j server's environment. Pass --container=<docker container name> " +
                            "(image, CPU and memory limits are read from Docker; see docker-compose.benchmark.yml) or " +
                            "--server-hardware=\"<cpu, memory, disk of the machine running Neo4j>\" when it runs elsewhere.")
        }
    }

    /** Markdown for the report's environment section. Throws (so the run aborts) if the server side cannot be described. */
    static String capture(Driver driver, BenchmarkRunner.Options options) {
        checkDeclared(options)
        Map<String, String> settings = serverSettings(driver)
        List<String> lines = []
        lines << "- Neo4j: ${Neo4J.version()} ${Neo4J.edition()} at ${ApplicationConfiguration.getString(PreferenceType.NEO4J_HOST)}:${ApplicationConfiguration.getInteger(PreferenceType.NEO4J_BOLT_PORT)}".toString()
        if (options.container) {
            lines.addAll(containerLines(options.container))
        } else {
            lines << "- Neo4j host (declared by operator): ${options.serverHardware}".toString()
        }
        lines << "- Neo4j settings in effect (not set = server default):"
        SETTINGS.each { String name -> lines << "  - `${name}` = ${settings.get(name) ?: 'not set'}".toString() }
        lines << "- Client: ${clientDescription()}".toString()
        lines << "- Cypher: ${Cypher.VERSION}".toString()
        lines << "- Run: ${options.messages} messages per profile, batch ${options.batch}, first ${options.warmup} batches are warm-up and not measured, seed ${options.seed}".toString()
        lines << "- Processing: ${options.writers == 0 ? 'serial (parse and write on one thread)' : options.parsers + ' parser and ' + options.writers + ' writer thread(s) (BatchPipeline)'}".toString()
        lines.join("\n")
    }

    static String clientDescription() {
        Runtime runtime = Runtime.getRuntime()
        long physicalGb = ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).totalMemorySize.intdiv(1024L * 1024 * 1024)
        "Java ${System.getProperty('java.version')}, ${runtime.availableProcessors()} cores, ${physicalGb} GB RAM, " +
                "max heap ${runtime.maxMemory().intdiv(1024 * 1024)} MB, ${System.getProperty('os.name')} ${System.getProperty('os.version')} ${System.getProperty('os.arch')}"
    }

    static Map<String, String> serverSettings(Driver driver) {
        driver.session().withCloseable { Session session ->
            try {
                session.run("SHOW SETTINGS YIELD name, value WHERE name IN \$names RETURN name, value", [names: SETTINGS])
                        .collectEntries { [(it.get("name").asString()): it.get("value").isNull() ? null : it.get("value").asString()] }
            } catch (Exception e) {
                throw new IllegalStateException("Refusing to run: cannot read the server's settings (SHOW SETTINGS needs admin rights): ${e.message}", e)
            }
        }
    }

    /** Image digest, CPU/memory limits of the container and the Docker VM it lives in. */
    static List<String> containerLines(String container) {
        JsonObject inspect
        try {
            inspect = JsonParser.parseString(docker("inspect", container)).asJsonArray.get(0).asJsonObject
        } catch (Exception e) {
            throw new IllegalStateException("Refusing to run: cannot inspect Docker container '${container}': ${e.message}", e)
        }
        JsonObject host = inspect.getAsJsonObject("HostConfig")
        long nanoCpus = host.get("NanoCpus").asLong
        long memory = host.get("Memory").asLong
        String imageId = inspect.get("Image").asString
        String digests = docker("image", "inspect", imageId, "--format", "{{json .RepoDigests}}").trim()
        String vm = docker("info", "--format", "{{.NCPU}} CPUs, {{.MemTotal}} bytes memory, Docker {{.ServerVersion}}").trim()
        [
                "- Container `${container}`: image `${inspect.getAsJsonObject('Config').get('Image').asString}` (id ${imageId}, digests ${digests})".toString(),
                "- Container limits: CPUs ${nanoCpus > 0 ? nanoCpus / 1e9 : 'unlimited'}, memory ${memory > 0 ? (memory / (1024L * 1024 * 1024)) + ' GB' : 'unlimited'}".toString(),
                "- Docker host VM: ${vm}".toString(),
        ]
    }

    private static String docker(String... args) {
        Process process = new ProcessBuilder(["docker"] + args.toList()).redirectErrorStream(true).start()
        String output = process.inputStream.text
        if (process.waitFor() != 0) throw new IllegalStateException(output.trim())
        output
    }
}
