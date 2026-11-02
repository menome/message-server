package com.menome.messageBatchProcessor

import org.neo4j.driver.exceptions.ClientException
import spock.lang.Specification

/**
 * Only errors caused by the data may be bisected and dead-lettered. Anything about the cluster, the database's
 * availability or our credentials must propagate so the whole batch is requeued (KANBAN E5-6).
 */
class DataErrorClassificationSpecification extends Specification {

    def "#code is a data error"() {
        expect:
        MessageBatchProcessor.isDataError(new ClientException(code, "boom"))

        where:
        code << [
                "Neo.ClientError.Statement.TypeError",              // nested map as a property value
                "Neo.ClientError.Statement.ArgumentError",
                "Neo.ClientError.Statement.SemanticError",
                "Neo.ClientError.Statement.ParameterMissing",
                "Neo.ClientError.Schema.ConstraintValidationFailed",
        ]
    }

    def "#code is not a data error, so the batch is requeued instead of dead-lettered"() {
        expect:
        !MessageBatchProcessor.isDataError(new ClientException(code, "boom"))

        where:
        code << [
                // A write sent to a follower, or while leadership moves: a failover must never dead-letter messages
                "Neo.ClientError.Cluster.NotALeader",
                "Neo.ClientError.Cluster.Unavailable",
                "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase",
                "Neo.ClientError.General.DatabaseUnavailable",
                "Neo.ClientError.General.TransactionOutOfMemoryError",
                // Not about the message either
                "Neo.ClientError.Security.Unauthorized",
                "Neo.ClientError.Security.Forbidden",
                "Neo.ClientError.Database.DatabaseNotFound",
                "Neo.ClientError.Transaction.TransactionTimedOut",
                "Neo.ClientError.Request.Invalid",
                // Anything we have not classified yet is treated as infrastructure
                "Neo.ClientError.Something.NewInTheNextRelease",
        ]
    }

    def "an exception the driver raises itself (no server code) because a value cannot be converted is a data error"() {
        expect:
        MessageBatchProcessor.isDataError(new ClientException("Unable to convert java.util.UUID to Neo4j Value."))
    }

    def "an exception without a code that is not a value conversion is not a data error"() {
        expect:
        !MessageBatchProcessor.isDataError(new ClientException("Connection to the database terminated"))
    }
}
