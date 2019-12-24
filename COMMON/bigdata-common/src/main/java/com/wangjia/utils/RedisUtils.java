package com.wangjia.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Administrator on 2017/7/7.
 */
public class RedisUtils {

    private static String _url = "";
    private static int _port = 6379;
    private static int _dbIndex = 0;

    private static final Map<String, JedisPool> pools = new ConcurrentHashMap<>();

    private static synchronized void initPool(String key, String url, int port) {
        try {
            if (pools.containsKey(key))
                return;
            // 建立连接池配置参数
            JedisPoolConfig config = new JedisPoolConfig();
            // 设置最大连接数
            config.setMaxTotal(256);
            // 设置最大阻塞时间，记住是毫秒数milliseconds
            config.setMaxWaitMillis(10000);
            // 设置空间连接
            config.setMaxIdle(32);
            // 创建连接池
            pools.put(key, new JedisPool(config, url, port));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void set(String url, int port, int dbIndex) {
        _url = url;
        _port = port;
        _dbIndex = dbIndex;
    }

    public static Jedis getConn(String url, int port, int dbIndex) {
        Jedis client = new Jedis(url, port);
        client.select(dbIndex);
//        String resp = client.ping();
//        System.out.println(resp);
        return client;
    }

    public static Jedis getConn() {
        return getConn(_url, _port, _dbIndex);
    }

    public static Jedis getPConn(String url, int port, int dbIndex) {
        String key = url + port;
        if (!pools.containsKey(key)) {
            initPool(key, url, port);
        }
        Jedis conn = pools.get(key).getResource();
        conn.select(dbIndex);
        return conn;
    }

    public static Jedis getPConn() {
        return getPConn(_url, _port, _dbIndex);
    }

    public static void closeConn(Jedis conn) {
        conn.close();
    }
}
