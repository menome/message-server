package com.menome.util

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig

class Redis {

    static Logger log = LoggerFactory.getLogger(Redis.class)

    static JedisPool pool = createConnectionPool()

    static Jedis connection() {
        Jedis jedis = null
        if (ApplicationConfiguration.getString(PreferenceType.USE_REDIS_CACHE) == "Y" && pool) {
            jedis = pool.getResource()
        }
        jedis
    }

    static boolean connectionOk() {
        boolean ok
        try {
            Jedis jedis = connection()
            jedis.ping()
            ok = Boolean.TRUE
        } catch (Exception ignored) {
            ok = Boolean.FALSE
        }
        ok
    }

    static void clearCache() {
        Jedis connection = connection()
        if (connection) {
            connection.flushAll()
            connection.close()
        }
    }

    static JedisPool createConnectionPool() {
        log.info("Connecting to Redis Host:{} on Port:{}", ApplicationConfiguration.getString(PreferenceType.REDIS_HOST), ApplicationConfiguration.getInteger(PreferenceType.REDIS_PORT))
        def jedisPool = new JedisPool(new JedisPoolConfig(), ApplicationConfiguration.getString(PreferenceType.REDIS_HOST), ApplicationConfiguration.getInteger(PreferenceType.REDIS_PORT))
        jedisPool
    }
}
