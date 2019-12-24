package com.wangjia.bigdata.core.job.common

import java.sql.Timestamp

import com.google.gson.JsonObject
import com.wangjia.bigdata.core.bean.common.{ExUserVisit, UserClue}
import com.wangjia.bigdata.core.bean.info.StrRule
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.job.SparkStreamJob
import com.wangjia.bigdata.core.utils.JDBCBasicUtils
import com.wangjia.es.{EsTableName, ExEsConnection}
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.hbase.{ExHbase, HBaseConst, HBaseTableName}
import com.wangjia.utils.{HBaseUtils, JavaUtils, RedisUtils}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/9/8.
  */
trait JobStreamUserTrack extends SparkStreamJob {

    //访问时间间隔
    protected var VISIT_TIME_INTERVAL = 10 * 60 * 1000

    protected def saveVisitRdd(sourceInfos: Array[StrRule])(visitRdd: RDD[((String, String), ListBuffer[ExUserVisit])]): Unit = {
        visitRdd.foreachPartition(iterator => {
            initHBase()
            val tbConn = new ExTBConnection
            val esConn: ExEsConnection = new ExEsConnection
            val tbVisit = tbConn.getTable(HBaseTableName.UUID_VISIT)
            val tbVisitDes = tbConn.getTable(HBaseTableName.UUID_VISIT_DES)
            while (iterator.hasNext) {
                val visitList = iterator.next()._2
                visitList.foreach(exVisit => {
                    if (exVisit.bChange) {
                        val visit = exVisit.toVisit(sourceInfos)

                        //hbase
                        val visitKey = Bytes.toBytes(ExHbase.getVisitKey(visit.appid, visit.uuid, visit.start))
                        val visitPut = new Put(visitKey)
                        visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_DEVICEID, Bytes.toBytes(visit.deviceId))
                        visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_UUID, Bytes.toBytes(visit.uuid))
                        visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_IP, Bytes.toBytes(visit.ip))
                        visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_REF, Bytes.toBytes(visit.ref))
                        visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_ADDRESS, Bytes.toBytes(visit.add.toJson().toString))
                        visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_START, Bytes.toBytes(visit.start))
                        visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_TIME, Bytes.toBytes(visit.time))
                        visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_NUM, Bytes.toBytes(visit.pReqs.size))
                        visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_ENUM, Bytes.toBytes(visit.pEvents.size))
                        visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_VERSION, Bytes.toBytes(visit.version))
                        visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_NET, Bytes.toBytes(visit.net))
                        visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_PLATFORM, Bytes.toBytes(visit.platform))
                        visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_APPID, Bytes.toBytes(visit.appid))
                        val eventIds = visit.getEventIds
                        visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_EVENTIDS, Bytes.toBytes(eventIds))
                        tbVisit.addPut(visitPut)

                        val desPut = new Put(visitKey)
                        desPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG, Bytes.toBytes(visit.toJson.toString))
                        tbVisitDes.addPut(desPut)

                        //es
                        val json = new JsonObject()
                        json.addProperty("appid", visit.appid)
                        json.addProperty("deviceid", visit.deviceId)
                        json.addProperty("uuid", visit.uuid)
                        json.addProperty("ip", visit.ip)
                        json.addProperty("ref", visit.ref)
                        json.addProperty("time", visit.start)
                        json.addProperty("staytime", visit.time)
                        json.addProperty("platform", visit.platform)
                        json.addProperty("add1", visit.add.country)
                        json.addProperty("add2", visit.add.province)
                        json.addProperty("add3", visit.add.city)
                        json.addProperty("add4", visit.add.area)
                        esConn.add(EsTableName.ES_INDEX_BIGDATA_VISIT, EsTableName.ES_TYPE_VISIT, ExHbase.getVisitKey(visit.appid, visit.uuid, visit.start), json.toString)
                    }
                })
            }
            tbConn.close()
            esConn.close()
        })
    }

    protected def saveUserClueRDD(userClueRdd: RDD[UserClue]): Unit = {
        val clues = userClueRdd.collect()
        if (clues.size > 0) {
            val newDate = new Timestamp(System.currentTimeMillis())
            JDBCBasicUtils.insertBatchByIterator[UserClue]("sql_user_clue_everyday", clues.toIterator, (pstmt, b) => {
                pstmt.setString(1, b.clueId)
                pstmt.setString(2, b.uuid)
                pstmt.setString(3, b.ip)
                pstmt.setString(4, b.appId)
                pstmt.setString(5, b.cookieId)
                pstmt.setString(6, b.deviceId)
                pstmt.setString(7, b.deviceName)
                pstmt.setString(8, b.version)
                pstmt.setTimestamp(9, new Timestamp(b.time))
                pstmt.setString(10, b.name)
                pstmt.setString(11, b.phone)
                pstmt.setString(12, b.cityId)
                pstmt.setString(13, b.houseArea)
                pstmt.setString(14, b.style)
                pstmt.setString(15, b.flatsName)
                pstmt.setDate(16, new java.sql.Date(b.time))
                pstmt.setTimestamp(17, newDate)
            })
        }
    }

    //appid,time
    protected def saveTime(timeRdd: RDD[(String, Long)]): Unit = {
        timeRdd.foreachPartition(iterator => {
            val conn: Jedis = RedisUtils.getPConn(Config.REDIS_URL, Config.REDIS_PORT, Config.REDIS_DBINDEX)
            while (iterator.hasNext) {
                val b = iterator.next()
                val appid = b._1
                val time = System.currentTimeMillis()
                val timeStr = time.toString
                val dayStr = JavaUtils.timeMillis2DayNum(time)
                conn.hset("USER:" + dayStr + "_UT:" + appid + ":TIME", timeStr, b._2.toString)
            }
            RedisUtils.closeConn(conn)
        })
    }
}