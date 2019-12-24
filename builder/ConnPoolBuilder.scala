package com.wangjia.bigdata.core.builder

import java.sql.{Connection, DriverManager, SQLException}

import com.wangjia.utils.RedisUtils
import redis.clients.jedis.Jedis

/**
  * 创建各类连接池对象
  *
  * Created by Administrator on 2018/3/1.
  */
object ConnPoolBuilder {

    /**
      * 建立Redis连接池
      *
      * @param url
      * @param port
      * @param dbIndex
      * @return
      */
    def buildRedisPool(url: String, port: Int, dbIndex: Int): ThreadLocal[Jedis] = {
        new ThreadLocal[Jedis]() {
            override protected def initialValue: Jedis = {
                RedisUtils.getConn(url, port, dbIndex)
            }

            override def get(): Jedis = {
                val jedis: Jedis = super.get()
                val status: String = {
                    try {
                        jedis.ping()
                    } catch {
                        case e: Exception => e.printStackTrace(); ""
                    }
                }
                if (jedis.isConnected && status.equals("PONG"))
                    return jedis
                remove()
                super.get
            }
        }
    }

    /**
      * 建立MySql连接池
      *
      * @param url
      * @param user
      * @param password
      * @param classNmae
      * @return
      */
    def buildMySQLPool(url: String, user: String, password: String, classNmae: String): ThreadLocal[Connection] = {
        Class.forName(classNmae)
        new ThreadLocal[Connection]() {
            val CONN_TIME_OUT_SECONDS: Int = 15

            override protected def initialValue: Connection = {
                var conn: Connection = null
                try {
                    conn = DriverManager.getConnection(url, user, password)
                } catch {
                    case e: ClassNotFoundException => e.printStackTrace()
                    case e: SQLException => e.printStackTrace()
                }
                conn
            }

            override def get(): Connection = {
                val conn: Connection = super.get()
                if (conn != null && conn.isValid(CONN_TIME_OUT_SECONDS))
                    return conn
                remove()
                super.get
            }
        }
    }

}
