package com.menome.util

import groovy.transform.CompileStatic

@CompileStatic
class ApplicationConfiguration {

    // The process environment cannot change while we run, and System.getenv(name) allocates on every call. Reading
    // configuration happens several times per node (CASE_STYLE), so look each variable up once.
    private static final Map<PreferenceType, String> ENVIRONMENT = new EnumMap<>(PreferenceType)

    static {
        for (PreferenceType type : PreferenceType.values()) {
            String value = System.getenv(type.name())
            if (value) ENVIRONMENT.put(type, value)
        }
    }

    /** Resolution order: environment variable, JVM system property, enum default. */
    static String getString(PreferenceType type) {
        String environment = ENVIRONMENT.get(type)
        if (environment) return environment
        String property = System.getProperty(type.name())
        property ? property : type.defaultValue
    }

    static Integer getInteger(PreferenceType type) {
        getString(type).toInteger()
    }

    static boolean getBoolean(PreferenceType type) {
        getString(type).equalsIgnoreCase("Y") || getString(type).equalsIgnoreCase("true")
    }

    static String errorQueue() {
        getString(PreferenceType.RABBITMQ_ERROR_QUEUE) ?: getString(PreferenceType.RABBITMQ_QUEUE) + ".errors"
    }
}
