package com.wangjia.bigdata.core.job.common

import java.sql.Timestamp

import com.alibaba.fastjson.JSON
import com.google.gson.JsonObject
import com.wangjia.bigdata.core.bean.info.AppInfo
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{JAInfoUtils, JDBCBasicUtils}
import com.wangjia.common.{DeviceType, LogEventId, LogType, Platform}
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.hbase.{ExHbase, HBaseConst, HBaseTableName}
import com.wangjia.utils.{GPSUtils, HBaseUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/4/11.
  */
class ComJobLocation extends EveryDaySparkJob {

    //应用信息
    private var mapAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null

    override protected def init(): Unit = {
        super.init()
        val sc = SparkExtend.ctx
        //加载广播应用信息
        val mapAppInfos = JAInfoUtils.loadAppInfos
        mapAppInfosBroadcast = sc.broadcast(mapAppInfos)
    }

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

    private def loadApp(inpath: String): RDD[Location] = {
        val beanRdd = SparkExtend.ctx.textFile(inpath)
                .map(BeanHandler.toAppEvent(LogEventId.APP_LOCATION))
                .filter(_ != null)
                .map(bean => {
                    try {
                        val data = JSON.parseObject(bean.data)
                        val jdu = (data.getDouble("longitude") * 1000000).toInt / 1000000.0
                        val wdu = (data.getDouble("latitude") * 1000000).toInt / 1000000.0
                        Location(bean.uuid, bean.appid, bean.dStrInfo("platform"), bean.time, jdu, wdu)
                    } catch {
                        case e: Exception => e.printStackTrace(); null
                    }
                })
                .filter(_ != null)
        beanRdd
    }

    private def loadWeiXin(inpath: String): RDD[Location] = {
        val beanRdd = SparkExtend.ctx.textFile(inpath).map(BeanHandler.toWeiXin)
                .filter(x => x != null && x.logtype == LogType.EVENT && x.subtype == LogEventId.APP_LOCATION)
                .map(f = bean => {
                    try {
                        val data = JSON.parseObject(bean.data)
                        val jdu = (data.getDouble("longitude") * 1000000).toInt / 1000000.0
                        val wdu = (data.getDouble("latitude") * 1000000).toInt / 1000000.0
                        val bdGps = GPSUtils.gcj2bd(wdu, jdu)

                        val info = mapAppInfosBroadcast.value.getOrElse(bean.appid, null)
                        if (info != null && info.appType == DeviceType.DEVICE_WEIXIN) {
                            Location(bean.uuid, bean.appid, "weixin", bean.time, bdGps(1), bdGps(0))
                        } else if (info != null && info.appType == DeviceType.DEVICE_ZFB) {
                            Location(bean.uuid, bean.appid, "zfb", bean.time, bdGps(1), bdGps(0))
                        } else
                            null
                    }
                    catch {
                        case e: Exception => e.printStackTrace(); null
                    }
                })
                .filter(_ != null)

        beanRdd
    }


    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.UUID_DEVICE_GPS)
        //平台参数
        val platform: String = this.getParameterOrElse("platform", "null")

        //经纬度RDD
        val locationRdd: RDD[Location] = platform match {
            case Platform.PLATFORM_NAME_MOBILE => loadApp(this.inPath)
            case Platform.PLATFORM_NAME_WEIXIN => loadWeiXin(this.inPath)
            case _ => throw new RuntimeException("no define platform")
        }
        locationRdd.cache()

        //按设备分组经纬度RDD
        val divLocRdd = locationRdd.groupBy(x => (x.uuid, x.appid))
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
        //写入数据库
        sumLocRdd.foreachPartition(iterator => {
            val date = new java.sql.Date(logDate.getTime)
            val newDate = new Timestamp(System.currentTimeMillis())
            JDBCBasicUtils.insertBatchByIterator[((String, String, Double, Double), Int)]("sql_data_gps_everyday", iterator, (pstmt, b) => {
                pstmt.setString(1, b._1._1)
                pstmt.setString(2, b._1._2)
                pstmt.setDouble(3, b._1._3)
                pstmt.setDouble(4, b._1._4)
                pstmt.setLong(5, b._2)
                pstmt.setDate(6, date)
                pstmt.setTimestamp(7, newDate)
            })
        })
    }
}

object ComJobLocation {

    def main(args: Array[String]) {
        val job = new ComJobLocation()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}