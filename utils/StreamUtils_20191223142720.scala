package com.wangjia.bigdata.core.utils

import com.wangjia.bigdata.core.common.Config
import com.wangjia.utils.RedisUtils
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object StreamUtils {

    /**
      * 保证日志的最大时间
      *
      * @param timeDS
      * @param redisKey
      */
    def saveLogLastTime(timeDS: DStream[Long], redisKey: String): Unit = {
        timeDS.reduce((t1, t2) => if (t1 > t2) t1 else t2)
                .foreachRDD(rdd => {
                    val arry: Array[Long] = rdd.collect()
                    if (!arry.isEmpty) {
                        val conn: Jedis = RedisUtils.getPConn(Config.REDIS_URL, Config.REDIS_PORT, Config.REDIS_DBINDEX)
                        conn.set(redisKey, arry.max.toString)
                        RedisUtils.closeConn(conn)
                    }
                })
    }

}
