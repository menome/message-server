package com.menome.util.messageBuilder

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.annotations.SerializedName
import groovy.transform.builder.Builder

@Builder
class MessageBuilder {

    @SerializedName("Name")
    String Name
    @SerializedName("NodeType")
    String NodeType
    @SerializedName("Priority")
    Long Priority
    @SerializedName("SourceSystem")
    String SourceSystem
    @SerializedName("ConformedDimensions")
    Map ConformedDimensions
    @SerializedName("Properties")
    Map Properties
    @SerializedName("DateProperties")
    Map DateProperties
    @SerializedName("DeleteProperties")
    List<String> DeleteProperties
    @SerializedName("DeleteNode")
    Boolean DeleteNode
    @SerializedName("Label")
    String Label
    @SerializedName("Connections")
    List<Connection> Connections = []


    String toJSON() {
        new Gson().toJson(this)
    }

    String toJSONPretty(){
        new GsonBuilder().setPrettyPrinting().create().toJson(this)
    }


}
