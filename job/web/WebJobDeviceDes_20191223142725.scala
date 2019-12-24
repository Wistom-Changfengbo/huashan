package com.wangjia.bigdata.core.job.web

import com.wangjia.bean.DeviceDes
import com.wangjia.bigdata.core.bean.info.AppInfo
import com.wangjia.bigdata.core.bean.web.WebLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.{BeanHandler, DeviceDesHandler}
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.JAInfoUtils
import com.wangjia.builder.DeviceDesBuidler
import com.wangjia.es.{EsTableName, ExEsConnection}
import com.wangjia.hbase.HBaseTableName
import com.wangjia.hbase.conn.{ExTBConnection, ExTable}
import com.wangjia.utils.HBaseUtils
import org.apache.hadoop.hbase.client.{Get, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/4/27.
  */
class WebJobDeviceDes extends EveryDaySparkJob {

    //应用信息
    private var mapAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null

    override protected def init(): Unit = {
        super.init()
        val sc = SparkExtend.ctx

        //加载广播应用信息
        val mapAppInfos = JAInfoUtils.loadAppInfos
        mapAppInfosBroadcast = sc.broadcast(mapAppInfos)
    }

    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.UUID_DEVICE_MSG)

        val sc = SparkExtend.ctx
        val linesRdd = sc.textFile("G:\\LOG\\web\\clean\\2018\\11\\01")

        val beansRdd = linesRdd.map(BeanHandler.toWebAll).filter(_ != null)
        beansRdd.cache()
        beansRdd.checkpoint()

        println(beansRdd.take(2).foreach(println))

        val uuid2beansRdd: RDD[(String, Iterable[WebLogMsg])] = beansRdd.groupBy(_.uuid)
      println(uuid2beansRdd.take(2).foreach(println))
        //加载已有设备信息
        val uuid2DeviceDesRdd: RDD[(String, DeviceDes)] = uuid2beansRdd.map(_._1).mapPartitions(iterator => {
            initHBase()
            val map = mutable.Map[String, DeviceDes]()
            val uuids = new ListBuffer[String]
            val gets = new ListBuffer[Get]

            val conn = new ExTBConnection
            val tb: Table = conn.getTable(HBaseTableName.UUID_DEVICE_MSG).
            getTable
            iterator.foreach(uuid => {
                uuids += uuid
                gets += new Get(Bytes.toBytes(uuid))
                if (uuids.size > Config.BATCH_SIZE) {
                    val results: Array[Result] = tb.get(gets)
                    var i = 0
                    val max = results.length
                    while (i < max) {
                        map += (uuids(i) -> DeviceDesBuidler.build(uuids(i), results(i)))
                        i += 1
                    }
                    uuids.clear()
                    gets.clear()
                }
            })

            if (uuids.nonEmpty) {
                val results: Array[Result] = tb.get(gets)
                var i = 0
                val max = results.length
                while (i < max) {
                    map += (uuids(i) -> DeviceDesBuidler.build(uuids(i), results(i)))
                    i += 1
                }
                uuids.clear()
                gets.clear()
            }

            conn.close()
            map.toIterator
        })
        //更新设备信息
        val uuid2newDesRdd = uuid2DeviceDesRdd.join(uuid2beansRdd).map(bean => {
            val uuid = bean._1
            val des = bean._2._1
            val logMsgs = bean._2._2.toList
            val newDes = DeviceDesHandler.updateWebDeviceDes(uuid, des, logMsgs, mapAppInfosBroadcast.value)
            (uuid, newDes)
        })
      uuid2newDesRdd.take(2).foreach(println)
//      val esConn = new ExEsConnection
//        uuid2newDesRdd.foreachPartition(iterator => {
//            initHBase()
//            val tbConn = new ExTBConnection
//            val esConn = new ExEsConnection
//            val tb: ExTable = tbConn.getTable(HBaseTableName.UUID_DEVICE_MSG)
//            iterator.foreach(b => {
//                tb.addPut(DeviceDesBuidler.deviceDes2Put(b._2))
//                esConn.add(EsTableName.ES_INDEX_BIGDATA_DEVICE, EsTableName.ES_TYPE_DEVICE, b._1, DeviceDesBuidler.deviceDes2EsDoc(b._2))
//            })
//            tbConn.close()
//            esConn.close()
//        })

    }
}

object WebJobDeviceDes {

    def main(args: Array[String]) {
        val job = new WebJobDeviceDes()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
