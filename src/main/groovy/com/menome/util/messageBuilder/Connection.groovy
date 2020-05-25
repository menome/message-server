package com.menome.util.messageBuilder

import com.google.gson.annotations.SerializedName
import groovy.transform.builder.Builder

@Builder
class Connection {
    @SerializedName("Name")
    String Name
    @SerializedName("NodeType")
    String NodeType
    @SerializedName("Label")
    String Label
    @SerializedName("RelType")
    String RelType
    @SerializedName("ForwardRel")
    boolean ForewardRel
    @SerializedName("ConformedDimensions")
    Map ConformedDimensions
}
