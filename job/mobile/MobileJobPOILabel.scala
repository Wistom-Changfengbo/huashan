package com.wangjia.bigdata.core.job.mobile

import java.util

import com.google.gson.{JsonObject, JsonParser}
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.common.LogEventId
import com.wangjia.handler.gps.GPSGraphHandler
import com.wangjia.handler.gps.GPSGraphHandler.GPSGraph
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.hbase.{ExHbase, HBaseConst, HBaseTableName}
import com.wangjia.utils.HBaseUtils
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/4/11.
  */
class MobileJobPOILabel extends EveryDaySparkJob {

    /**
      * 查询范围
      */
    val SEARCH_TIME_RANGE: Long = 1 * 24 * 3600 * 1000

    /**
      * 最大返回条数
      */
    val SEARCH_MAX_RETUREN: Int = 5000

    /**
      * 经纬度
      *
      * @param time 时间
      * @param jd   经度
      * @param wd   纬度
      */
    case class JWBean(time: Long, jd: Double, wd: Double)

    /**
      * 得到GPS
      *
      * @param tb
      * @param uuid
      * @param startTime
      * @return
      */
    def getGpsMsg(tb: Table, uuid: String, startTime: Long): ListBuffer[JWBean] = {
        val beans = new ListBuffer[JWBean]
        val minKey = Bytes.toBytes(ExHbase.getGPSKey(uuid, startTime))
        val maxKey = Bytes.toBytes(ExHbase.getGPSKey(uuid, startTime - SEARCH_TIME_RANGE))
        val scan = new Scan()
        scan.setStartRow(minKey)
        scan.setStopRow(maxKey)
        scan.setBatch(SEARCH_MAX_RETUREN)
        val jsonParser = new JsonParser()

        try {
            val results: ResultScanner = tb.getScanner(scan)
            val iterator: util.Iterator[Result] = results.iterator()
            while (iterator.hasNext) {
                val rs = iterator.next()
                val str = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG))
                val json: JsonObject = jsonParser.parse(str).getAsJsonObject
                val time = json.get("time").getAsLong
                val jd = json.get("jd").getAsDouble
                val wd = json.get("wd").getAsDouble
                beans += JWBean(time, jd, wd)
            }
        } catch {
            case e: Exception => e.printStackTrace()
        }
        beans
    }

    /**
      * 分析GPS图
      */
    def analyzeGPSGraph(uuid: String, pois: ListBuffer[JWBean], graph: String): GPSGraphHandler = {
        val handler = new GPSGraphHandler(graph)
        pois.foreach(jwBean => handler.addGPS(jwBean.time, jwBean.jd, jwBean.wd))
        handler
    }

    /**
      * 图转成标签
      *
      * @param handler
      * @param mapLabel
      * @param graph
      */
    def gPSGraph2Label(handler: GPSGraphHandler, mapLabel: mutable.Map[String, (Long, Int, Double, Double)], graph: (Int, Int, Double, GPSGraph)): Unit = {
        val num = graph._1
        val dayOff = graph._2
        val night = graph._3

        val time = graph._4.getMinTime
        val gps: Array[Double] = handler.getGPS(graph._4)
        //        if (mapLabel.size >= 2 && num < 3)
        //            return
        if (!mapLabel.contains("_sf_gps_home") && night > 0) {
            mapLabel += ("_sf_gps_home" ->(time, num, gps(0), gps(1)))
        } else if (!mapLabel.contains("_sf_gps_work") && dayOff < 0) {
            mapLabel += ("_sf_gps_work" ->(time, num, gps(0), gps(1)))
        } else if (dayOff < 0) {
            mapLabel += ("_sf_gps_business_" + time ->(time, num, gps(0), gps(1)))
        } else if (dayOff > 0) {
            mapLabel += ("_sf_gps_travel_" + time ->(time, num, gps(0), gps(1)))
        }
    }

    /**
      * 分析标签
      *
      * @param handler
      * @return
      */
    def analyzeGPSLabel(handler: GPSGraphHandler): mutable.Map[String, (Long, Int, Double, Double)] = {
        val graphList: util.LinkedList[GPSGraph] = handler.getGraphList
        var i: Int = 0
        val max: Int = graphList.size()
        val list = new ListBuffer[(Int, Int, Double, GPSGraph)]

        while (i < max) {
            val graph: GPSGraph = graphList.get(i)
            val num = graph.getNum
            val dayoff = handler.getDayOffValue(graph)
            val night = handler.getNightValue(graph)
            list += ((num, dayoff, night, graph))
            i += 1
        }

        val label = mutable.Map[String, (Long, Int, Double, Double)]()
        val sortWith: ListBuffer[(Int, Int, Double, GPSGraph)] = list.sortWith(_._1 > _._1)

        sortWith.foreach(x => gPSGraph2Label(handler, label, x))

        label
    }

    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.UUID_DEVICE_GPS)
        HBaseUtils.createTable(HBaseTableName.UUID_LABEL_GPS)

        val sc = SparkExtend.ctx
        val linesRdd = sc.textFile(this.inPath)
        //得到有更新GPS的UUID
        val beanRdd = linesRdd.map(BeanHandler.toAppEvent(LogEventId.APP_LOCATION) _)
                .filter(_ != null)
        val uuidRdd = beanRdd.map(_.uuid)
                .distinct()

//        uuidRdd.take(2).map(uuid=>{
//            val list = mutable.ListBuffer[(String, ListBuffer[JWBean], String)]()
//            val conn = new ExTBConnection
//            val tbGPS = conn.getTable(HBaseTableName.UUID_DEVICE_GPS).getTable
//            val tbLabel = conn.getTable(HBaseTableName.UUID_LABEL_GPS).getTable
//            val startTime = this.logDate.getTime + 24 * 3600 * 1000
//
//            val jwBeans: ListBuffer[JWBean] = getGpsMsg(tbGPS, uuid, startTime)
//
//            val result: Result = tbLabel.get(new Get(Bytes.toBytes(uuid)))
//            val graph = {
//                if (result.containsColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("graph"))) {
//                    Bytes.toString(result.getValue(HBaseConst.BYTES_CF1, Bytes.toBytes("graph")))
//                } else {
//                    null
//                }
//            }
//            if (jwBeans.nonEmpty) {
//                list += ((uuid, jwBeans, graph))
//            }
//        })


        //加载GPS信息
        val uuid2GpsRdd = uuidRdd.mapPartitions(iterator => {
            val list = mutable.ListBuffer[(String, ListBuffer[JWBean], String)]()
            val conn = new ExTBConnection
            val tbGPS = conn.getTable(HBaseTableName.UUID_DEVICE_GPS).getTable
            val tbLabel = conn.getTable(HBaseTableName.UUID_LABEL_GPS).getTable

            val startTime = this.logDate.getTime + 24 * 3600 * 1000
            iterator.foreach(uuid => {
                val jwBeans: ListBuffer[JWBean] = getGpsMsg(tbGPS, uuid, startTime)

                val result: Result = tbLabel.get(new Get(Bytes.toBytes(uuid)))
                val graph = {
                    if (result.containsColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("graph"))) {
                        Bytes.toString(result.getValue(HBaseConst.BYTES_CF1, Bytes.toBytes("graph")))
                    } else {
                        null
                    }
                }
                if (jwBeans.nonEmpty) {
                    list += ((uuid, jwBeans, graph))
                }
            })

            conn.close()
            list.toIterator
        })
        val test =uuid2GpsRdd.take(2)
        test.foreach(println)
        test.map(x =>{
            val conn = new ExTBConnection
            val tb = conn.getTable(HBaseTableName.UUID_LABEL_GPS)
            val handler =analyzeGPSGraph(x._1, x._2, x._3)
            val _3 =analyzeGPSLabel(handler)
            val uuid =x._1


            val json = new JsonObject()
           _3.foreach(b => {
                val gps = new JsonObject()
                gps.addProperty("time", b._2._1)
                gps.addProperty("num", b._2._2)
                gps.addProperty("jd", b._2._3)
                gps.addProperty("wd", b._2._4)
                json.add(b._1, gps)
            })
            val put = new Put(Bytes.toBytes(uuid))
            put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("graph"), Bytes.toBytes(handler.toJSONString))
            put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG, Bytes.toBytes(json.toString))
            tb.addPut(put)
            conn.close()
        }
        )


        val uuid2GPSGraph: RDD[(String, GPSGraphHandler)] = uuid2GpsRdd.map(x => (x._1, analyzeGPSGraph(x._1, x._2, x._3)))

        val uuid2Label = uuid2GPSGraph.map(x => (x._1, x._2, analyzeGPSLabel(x._2)))

        uuid2Label.foreachPartition(iterator => {
            val conn = new ExTBConnection
            val tb = conn.getTable(HBaseTableName.UUID_LABEL_GPS)


            iterator.foreach(x => {
                val uuid = x._1
                val handler = x._2
                val json = new JsonObject()
                x._3.foreach(b => {
                    val gps = new JsonObject()
                    gps.addProperty("time", b._2._1)
                    gps.addProperty("num", b._2._2)
                    gps.addProperty("jd", b._2._3)
                    gps.addProperty("wd", b._2._4)
                    json.add(b._1, gps)
                })
                val put = new Put(Bytes.toBytes(uuid))
                put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("graph"), Bytes.toBytes(handler.toJSONString))
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG, Bytes.toBytes(json.toString))
                tb.addPut(put)

            })
            conn.close()
        })
    }
}


object MobileJobPOILabel {

    def main(args: Array[String]) {

        val job = new MobileJobPOILabel()
        job.run(args)

        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
