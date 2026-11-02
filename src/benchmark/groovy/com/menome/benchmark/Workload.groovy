package com.menome.benchmark

/** A named, reproducible set of harvester messages plus what it costs the graph to apply them. */
class Workload {
    String id
    String description
    /** NodeType (= label) of the primary node, used to verify the number of nodes written. */
    String nodeType
    List<String> messages
    /** Graph writes per message: the primary node merge plus one relationship merge per connection. Hub merges are not counted. */
    int writesPerMessage
}
