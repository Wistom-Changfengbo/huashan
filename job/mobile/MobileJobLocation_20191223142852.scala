package com.wangjia.bigdata.core.job.mobile

import java.sql.Timestamp

import com.alibaba.fastjson.JSON
import com.google.gson.JsonObject
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.JDBCBasicUtils
import com.wangjia.common.LogEventId
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.hbase.{ExHbase, HBaseConst, HBaseTableName}
import com.wangjia.utils.HBaseUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/4/11.
  */
class MobileJobLocation extends EveryDaySparkJob {

    /**
      *
      * @param uuid      唯一ID
      * @param appid     APPID
      * @param platform  平台
      * @param time      时间戳
      * @param longitude 经度
      * @param latitude  纬度
      */
    case class Location(uuid: String, appid: String, platform: String, time: Long, longitude: Double, latitude: Double)

    private def log2Location(line: String): Location = {
        try {
            val bean = BeanHandler.toAppEvent(LogEventId.APP_LOCATION)(line)
            if (bean == null)
                return null
            val data = JSON.parseObject(bean.data)
            val jdu = (data.getDouble("longitude") * 1000000).toInt / 1000000.0
            val wdu = (data.getDouble("latitude") * 1000000).toInt / 1000000.0
            Location(bean.uuid, bean.appid, bean.dStrInfo("platform"), bean.time, jdu, wdu)
        } catch {
            case e: Exception => e.printStackTrace(); null
        }
    }

    /**
      * 过虑掉最小间隔的位置信息
      *
      * @param source      源位置信息
      * @param minInterval 最小间隔 默认是0.0001
      * @return
      */
    private def filterMinInterval(source: List[Location], minInterval: Float = 0.0001f): ListBuffer[Location] = {
        val locs = source.sortWith(_.time < _.time)
        val list = new ListBuffer[Location]
        var upLoc: Location = locs.head
        list += upLoc
        var i = 1
        val max = locs.size
        while (i < max) {
            val loc = locs(i)
            val dL = Math.abs(loc.longitude - upLoc.longitude) + Math.abs(loc.latitude - upLoc.latitude)
            if (dL > minInterval) {
                upLoc = loc
                list += upLoc
            }
            i += 1
        }
        list
    }

    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.UUID_DEVICE_GPS)
        val sc = SparkExtend.ctx

        val linesRdd = sc.textFile(this.inPath)



        //经纬度RDD
        val locationRdd = linesRdd.map(log2Location).filter(_ != null)
        locationRdd.cache()

        //按设备分组经纬度RDD
        val divLocRdd = locationRdd.groupBy(x => (x.uuid, x.appid))

        divLocRdd.take(2).map(x => {
            initHBase()
//            val _conf = HBaseConfiguration.create()
//            _conf.set("hbase.zookeeper.quorum", "ambari.am0.com")
//            _conf.set("hbase.zookeeper.property.clientPort", "2181")
//            HBaseUtils.setConfiguration(_conf)
            val conn = new ExTBConnection
            val tbUuid2devicegps = conn.getTable(HBaseTableName.UUID_DEVICE_GPS)

            val divLoc = x
            val source = filterMinInterval(divLoc._2.toList, 0.0009f)
            val uuid = divLoc._1._1
            val appid = divLoc._1._2
            for (loc <- source) {
                val jsonLoc = new JsonObject
                jsonLoc.addProperty("time", loc.time)
                jsonLoc.addProperty("jd", loc.longitude)
                jsonLoc.addProperty("wd", loc.latitude)
                jsonLoc.addProperty("appid", appid)
                println(ExHbase.getGPSKey(uuid, loc.time))

                val rowkey = Bytes.toBytes(ExHbase.getGPSKey(uuid, loc.time))
                val put = new Put(rowkey)
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG, Bytes.toBytes(jsonLoc.toString))
                tbUuid2devicegps.addPut(put)
            }


        })

        //把每个设备轨迹保存入HBase
        divLocRdd.foreachPartition(iterator => {
            initHBase()
            val conn = new ExTBConnection
            val tbUuid2devicegps = conn.getTable(HBaseTableName.UUID_DEVICE_GPS)

            while (iterator.hasNext) {
                val divLoc = iterator.next()
                val source = filterMinInterval(divLoc._2.toList, 0.0009f)
                val uuid = divLoc._1._1
                val appid = divLoc._1._2
                for (loc <- source) {
                    val jsonLoc = new JsonObject
                    jsonLoc.addProperty("time", loc.time)
                    jsonLoc.addProperty("jd", loc.longitude)
                    jsonLoc.addProperty("wd", loc.latitude)
                    jsonLoc.addProperty("appid", appid)

                    val rowkey = Bytes.toBytes(ExHbase.getGPSKey(uuid, loc.time))
                    val put = new Put(rowkey)
                    put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG, Bytes.toBytes(jsonLoc.toString))
                    tbUuid2devicegps.addPut(put)
                }
            }
            conn.close()
        })

        //统计GPS热力图
        //按APPID 平台 经纬度汇总RDD
        val sumLocRdd = locationRdd.map(x => {
            val jdu = (x.longitude * 100000).toInt / 100000.0
            val wdu = (x.latitude * 100000).toInt / 100000.0
            ((x.appid, x.platform, jdu, wdu), 1)
        }).reduceByKey(_ + _)
//        sumLocRdd.take(10).foreach(println)
        //写入数据库
//        sumLocRdd.foreachPartition(iterator => {
//            val date = new java.sql.Date(logDate.getTime)
//            val newDate = new Timestamp(System.currentTimeMillis())
//            JDBCBasicUtils.insertBatchByIterator[((String, String, Double, Double), Int)]("sql_data_gps_everyday", iterator, (pstmt, b) => {
//                pstmt.setString(1, b._1._1)
//                pstmt.setString(2, b._1._2)
//                pstmt.setDouble(3, b._1._3)
//                pstmt.setDouble(4, b._1._4)
//                pstmt.setLong(5, b._2)
//                pstmt.setDate(6, date)
//                pstmt.setTimestamp(7, newDate)
//            })
//        })
    }
}


object MobileJobLocation {

    def main(args: Array[String]) {
        val job = new MobileJobLocation()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}