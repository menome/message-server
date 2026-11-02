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
    @SerializedName("Properties")
    Map Properties
    @SerializedName("DateProperties")
    Map DateProperties
    @SerializedName("DeleteProperties")
    List<String> DeleteProperties
    @SerializedName("RelProps")
    Map RelProps
    @SerializedName("DateRelProps")
    Map DateRelProps
    @SerializedName("DeleteRelProps")
    List<String> DeleteRelProps
    @SerializedName("DeleteRelationship")
    Boolean DeleteRelationship
    @SerializedName("DeleteNode")
    Boolean DeleteNode
}
