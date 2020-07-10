package com.menome.messageServerEndPoints

import com.menome.util.Neo4J
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@Controller('/DeleteTestMessages')
class DeleteTestMessagesController {

    static Logger log = LoggerFactory.getLogger(Neo4J.class)

    @Get(produces = MediaType.TEXT_HTML)
    String index() {
        log.info("Deleting All Test Nodes")
        Neo4J.deleteAllTestNodes()
        log.info("Done")
        "<h3>Deleted all test messages from Neo4J </h3>"
    }
}