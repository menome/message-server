package com.menome.util


import java.util.stream.Stream

class ApplicationConfiguration {

    static String getString(PreferenceType type){
        Stream.of(Optional.ofNullable(System.getenv(type.name())), Optional.ofNullable(System.getProperty(type.name())), Optional.of(type.defaultValue))
                .filter({ o -> o.isPresent() })
                .map({ o-> o.get() })
                .findFirst()
                .get()
    }

    static Integer getInteger(PreferenceType type){
        getString(type).toInteger()
    }
}


