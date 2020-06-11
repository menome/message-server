package com.menome.redis

import com.menome.util.PreferenceType
import com.menome.util.Redis
import spock.lang.Specification

class RedisSpecification extends Specification {

    def "test redis connection ok"() {
        given:
        def useRedisKey = PreferenceType.USE_REDIS_CACHE.name()
        String priorSetting = System.getProperty(useRedisKey)
        System.setProperty(useRedisKey, "Y")
        def ok = Redis.connectionOk()
        expect:
        ok
        cleanup:
        if (priorSetting) {
            System.setProperty(useRedisKey, priorSetting)
        }
    }

    def "test redis connection not ok when use redis cache is set to N"() {

        given:
        def useRedisKey = PreferenceType.USE_REDIS_CACHE.name()
        String priorSetting = System.getProperty(useRedisKey)
        System.setProperty(useRedisKey, "N")
        def ok = Redis.connectionOk()

        expect:
        !ok

        cleanup:
        if (priorSetting) {
            System.setProperty(useRedisKey, priorSetting)
        }

    }


    def "test redis set and get key"() {
        given:
        def connection = Redis.connection()
        connection.set("key", "Hello, Redis!")

        expect:
        "Hello, Redis!" == connection.get("key")
        cleanup:
        connection.close()
    }

    def "test redis flush all"() {
        given:
        def connection = Redis.connection()
        connection.set("key", "Hello, Redis!")
        Redis.clearCache()
        expect:
        connection.dbSize() == 0
        cleanup:
        connection.close()
    }
}
