package com.menome.messageServerEndPoints

import com.menome.util.ApplicationConfiguration
import com.menome.util.Neo4J
import com.menome.util.PreferenceType
import io.micronaut.http.HttpResponse
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/** Test tooling. Disabled unless ENABLE_TEST_ENDPOINTS=Y. */
@Controller('/DeleteTestMessages')
class DeleteTestMessagesController {

    static Logger log = LoggerFactory.getLogger(DeleteTestMessagesController.class)

    @Post(produces = MediaType.TEXT_HTML)
    HttpResponse<String> index() {
        if (!ApplicationConfiguration.getBoolean(PreferenceType.ENABLE_TEST_ENDPOINTS)) {
            return HttpResponse.notFound()
        }
        log.info("Deleting All Test Nodes")
        Neo4J.deleteAllTestNodes()
        log.info("Done")
        HttpResponse.ok("<h3>Deleted all test messages from Neo4J </h3>")
    }
}
