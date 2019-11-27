package com.menome.messageBuilder

import groovy.transform.builder.Builder

@Builder
class MessageBuilder {
    String Name
    String NodeType
    Long Priority
    String SourceSystem
    Map ConformedDimensions
}
