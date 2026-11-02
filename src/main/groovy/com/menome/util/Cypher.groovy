package com.menome.util

/** Helpers for safely interpolating identifiers (labels, property keys, relationship types) into Cypher. */
class Cypher {

    /**
     * Cypher language version every statement is pinned to. A database only defaults to Cypher 25 when
     * db.query.default_language says so (a missing setting means Cypher 5), so never rely on the default.
     * Cypher 25 is also required for CALL ... IN CONCURRENT TRANSACTIONS ... DISJOINT BY (Neo4j 2026.06+).
     */
    static final String VERSION = "CYPHER 25"

    static String versioned(String statement) {
        VERSION + " " + statement
    }

    /** Backtick-quotes an identifier, doubling any embedded backtick. */
    static String id(String identifier) {
        "`" + identifier.replace("`", "``") + "`"
    }

    /** Reference to a key of the UNWIND row parameter, e.g. param.`Email`. Also used for param names with prefixes. */
    static String param(String key) {
        "param." + id(key)
    }
}
