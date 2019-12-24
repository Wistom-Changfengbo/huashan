package com.wangjia.bigdata.core.utils

import java.sql.{Connection, _}
import java.util.Properties

import com.wangjia.bigdata.core.common.Config

/**
  * JDBC 基础工具类
  *
  * Created by Administrator on 2017/2/28.
  */
object JDBCBasicUtils {

    private val prop: Properties = {
        val _prop: Properties = new Properties()
        _prop.load(this.getClass.getResourceAsStream(Config.JDBC_PATH))
        Class.forName(_prop.getProperty("classname"))
        _prop
    }

    def getProperty(key: String): String = {
        val str = prop.getProperty(key, null)
        if (str == null) throw new RuntimeException
        str
    }

    def getSql(key: String): String = {
        val sql = prop.getProperty(key, null)
        if (sql == null) throw new RuntimeException
        sql
    }

    def getConn(name: String = null) = {
        var conn: Connection = null
        try {
            val cname: String = if (name == null) "" else name + '.'
            conn = DriverManager.getConnection(prop.getProperty(cname + "url"),
                prop.getProperty(cname + "username"),
                prop.getProperty(cname + "password"))
        } catch {
            case e: ClassNotFoundException => e.printStackTrace()
            case e: SQLException => e.printStackTrace()
        }
        conn
    }

    def getConn(url: String, username: String, password: String, datebase: String): Connection = {
        var conn: Connection = null
        try {
            conn = DriverManager.getConnection(url, username, password)
        } catch {
            case e: ClassNotFoundException => e.printStackTrace()
            case e: SQLException => e.printStackTrace()
        }
        conn
    }

    def getConn(url: String, username: String, password: String): Connection = {
        var conn: Connection = null
        try {
            conn = DriverManager.getConnection(url, username, password)
        } catch {
            case e: ClassNotFoundException => e.printStackTrace()
            case e: SQLException => e.printStackTrace()
        }
        conn
    }

    def insert(sqlKey: String, fun: (PreparedStatement) => Unit): Int = {
        val conn = getConn()
        val sql = getSql(sqlKey)
        insert(conn, sql, fun)
    }

    def insert(conn: Connection, sql: String, fun: (PreparedStatement) => Unit): Int = {
        var pstmt: PreparedStatement = null
        try {
            pstmt = conn.prepareStatement(sql)
            fun(pstmt)
            val n = pstmt.executeUpdate()
            n
        } catch {
            case e: SQLException => e.printStackTrace(); throw new RuntimeException
        } finally {
            if (pstmt != null) {
                pstmt.close()
            }
            if (conn != null) {
                conn.close()
            }
        }
    }

    def insertBatch(sqlKey: String, fun: (PreparedStatement) => Unit): Int = {
        val conn = getConn()
        val sql = getSql(sqlKey)
        insertBatch(conn, sql, fun)
    }

    def insertBatch(conn: Connection, sql: String, fun: (PreparedStatement) => Unit): Int = {
        var pstmt: PreparedStatement = null
        var n = 0
        try {
            pstmt = conn.prepareStatement(sql)
            fun(pstmt)
            n = pstmt.executeBatch().length
        } catch {
            case e: SQLException => e.printStackTrace(); pstmt.clearBatch()
        } finally {
            if (pstmt != null) {
                pstmt.close()
            }
            if (conn != null) {
                conn.close()
            }
        }
        n
    }

    def insertBatchByIterator[T](sqlKey: String, it: Iterator[T], fun: (PreparedStatement, T) => Unit): Int = {
        var conn: Connection = null
        var pstmt: PreparedStatement = null
        try {
            conn = getConn()
            val sql = getSql(sqlKey)
            pstmt = conn.prepareStatement(sql)
            var n: Int = 0
            var i = 0
            while (it.hasNext) {
                val b = it.next()
                fun(pstmt, b)
                pstmt.addBatch()
                i += 1
                if (i > 1000) {
                    n += pstmt.executeBatch().length
                    pstmt.clearBatch()
                    i = 0
                }
            }
            if (i > 0) {
                n += pstmt.executeBatch().length
                pstmt.clearBatch()
                i = 0
            }
            n
        } catch {
            //case e: SQLException => e.printStackTrace(); throw new RuntimeException
            case e: SQLException => e.printStackTrace(); -1
        } finally {
            if (pstmt != null) {
                pstmt.close()
            }
            if (conn != null) {
                conn.close()
            }
        }
    }

    def read(sqlKey: String, fun: (ResultSet) => Unit, sqlPs: scala.Array[Object] = null): Unit = {
        val conn = getConn()
        val sql = getSql(sqlKey)
        read(conn, sql, fun, sqlPs)
    }

    def read(conn: Connection, sql: String, fun: (ResultSet) => Unit, sqlPs: scala.Array[Object]): Unit = {
        try {
            val pstmt = conn.prepareStatement(sql)
            if (sqlPs != null) {
                var i = 0
                while (i < sqlPs.length) {
                    pstmt.setObject(i + 1, sqlPs(i))
                    i += 1
                }
            }
            read(conn, pstmt, fun)
        } catch {
            case e: SQLException => e.printStackTrace(); throw new RuntimeException
        }
    }

    def read(conn: Connection, pstmt: PreparedStatement, fun: (ResultSet) => Unit): Unit = {
        try {
            val rs: ResultSet = pstmt.executeQuery()
            fun(rs)
        } catch {
            case e: SQLException => e.printStackTrace(); throw new RuntimeException
        } finally {
            if (pstmt != null) {
                pstmt.close()
            }
            if (conn != null) {
                conn.close()
            }
        }
    }


    def insertBatchByIteratorBySql[T](_conn: Connection = null)(sql: String, tableName: String, it: Iterator[T], fun: (PreparedStatement, T) => Unit): Int = {
        var conn: Connection = null
        var pstmt: PreparedStatement = null
        try {
            if (_conn == null)
                conn = getConn()
            else
                conn = _conn
            pstmt = conn.prepareStatement(sql)
            var n: Int = 0
            var i = 0
            while (it.hasNext) {
                val b = it.next()
                fun(pstmt, b)
                pstmt.addBatch()
                i += 1
                if (i > 1000) {
                    n += pstmt.executeBatch().length
                    pstmt.clearBatch()
                    i = 0
                }
            }
            if (i > 0) {
                n += pstmt.executeBatch().length
                pstmt.clearBatch()
                i = 0
            }
            n
        } catch {
            //case e: SQLException => e.printStackTrace(); throw new RuntimeException
            case e: SQLException => e.printStackTrace(); -1
        } finally {
            if (pstmt != null) {
                pstmt.close()
            }
            if (conn != null) {
                conn.close()
            }
        }
    }

}
