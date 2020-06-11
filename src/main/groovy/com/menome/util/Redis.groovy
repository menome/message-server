package com.menome.util

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig

class Redis {

    static JedisPool pool = new JedisPool(new JedisPoolConfig(), ApplicationConfiguration.getString(PreferenceType.REDIS_HOST));

    static  Jedis connection() {
        pool.getResource();
    }

    static boolean connectionOk() {
        boolean ok = Boolean.FALSE
        if (ApplicationConfiguration.getString(PreferenceType.USE_REDIS_CACHE) == "Y") {
            try {
                Jedis jedis = connection()
                jedis.ping()
                ok = Boolean.TRUE
            } catch (Exception ignored) {
                ok = Boolean.FALSE
            }
        }
        ok
    }

    static void clearCache(){
        Jedis connection = connection()
        connection.flushAll()
        connection.close()
    }
}
