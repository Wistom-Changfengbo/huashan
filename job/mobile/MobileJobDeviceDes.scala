package com.wangjia.bigdata.core.job.mobile

import com.alibaba.fastjson.JSON
import com.wangjia.bean.DeviceDes
import com.wangjia.bigdata.core.bean.info.AppInfo
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.{BeanHandler, DeviceDesHandler}
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.JAInfoUtils
import com.wangjia.builder.DeviceDesBuidler
//import com.wangjia.es.{EsTableName, ExEsConnection}
import com.wangjia.hbase.HBaseTableName
import com.wangjia.hbase.conn.{ExTBConnection, ExTable}
import com.wangjia.utils.HBaseUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 分析设备信息
  *
  * --inPath    日志输入目录
  * --date      日志日期(yyyy-MM-dd)
  *
  * Created by Administrator on 2017/4/27.
  */
class MobileJobDeviceDes extends EveryDaySparkJob {

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
        initHBase()
        val _conf = HBaseConfiguration.create()
        _conf.set("hbase.zookeeper.quorum", "ambari.am0.com")
        _conf.set("hbase.zookeeper.property.clientPort", "2181")
        HBaseUtils.setConfiguration(_conf)
        HBaseUtils.createTable(HBaseTableName.UUID_DEVICE_MSG)
        val sc = SparkExtend.ctx
        val linesRdd = sc.textFile(this.inPath)
        val beansRdd = linesRdd.map(BeanHandler.toAppAcc).filter(_ != null)
//        beansRdd.cache()
//        beansRdd.checkpoint()

        val uuid2beansRdd: RDD[(String, Iterable[MobileLogMsg])] = beansRdd.groupBy(_.uuid)
        val take: Array[(String, Iterable[MobileLogMsg])] = uuid2beansRdd.sample(true,1).take(1)
        take.map(x=>{

            val map = mutable.Map[String, DeviceDes]()
            val uuids = new ListBuffer[String]
            val gets = new ListBuffer[Get]
            val conn = new ExTBConnection
            val tb: Table = conn.getTable(HBaseTableName.UUID_DEVICE_MSG).getTable
            gets += new Get(Bytes.toBytes(x._1))
            uuids +=x._1
            val results: Array[Result] = tb.get(gets)
            map += (uuids(0) ->
                    DeviceDesBuidler.build(uuids(0), results(0)))
        })
        //加载已有设备信息,没有的设备就加上devicedId，先不管。
        val uuid2DeviceDesRdd: RDD[(String, DeviceDes)] = uuid2beansRdd.map(_._1).mapPartitions(iterator => {
            initHBase()
            val _conf = HBaseConfiguration.create()
            _conf.set("hbase.zookeeper.quorum", "ambari.am0.com")
            _conf.set("hbase.zookeeper.property.clientPort", "2181")
            HBaseUtils.setConfiguration(_conf)
            val map = mutable.Map[String, DeviceDes]()
            val uuids = new ListBuffer[String]
            val gets = new ListBuffer[Get]

            val conn = new ExTBConnection
            val tb: Table = conn.getTable(HBaseTableName.UUID_DEVICE_MSG).getTable

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

        uuid2DeviceDesRdd.sample(true,1).join(uuid2beansRdd).take(1).map(bean=>{
            val uuid = bean._1
            val des = bean._2._1
            val logMsgs = bean._2._2.toList
            val newDes = DeviceDesHandler.updateAppDeviceDes(uuid, des, logMsgs, mapAppInfosBroadcast.value)
            (uuid, newDes)
        })





        //更新设备信息
        val uuid2newDesRdd = uuid2DeviceDesRdd.join(uuid2beansRdd).map(bean => {
            val uuid = bean._1
            val des = bean._2._1
            val logMsgs = bean._2._2.toList
            val newDes = DeviceDesHandler.updateAppDeviceDes(uuid, des, logMsgs, mapAppInfosBroadcast.value)
            (uuid, newDes)
        })

        uuid2newDesRdd.foreachPartition(iterator => {
            val _conf = HBaseConfiguration.create()
            _conf.set("hbase.zookeeper.quorum", "ambari.am0.com")
            _conf.set("hbase.zookeeper.property.clientPort", "2181")
            HBaseUtils.setConfiguration(_conf)
            val tbConn = new ExTBConnection
//            val esConn = new ExEsConnection
            val tb: ExTable = tbConn.getTable(HBaseTableName.UUID_DEVICE_MSG)
            iterator.foreach(b => {
                tb.addPut(DeviceDesBuidler.deviceDes2Put(b._2))
//                esConn.add(EsTableName.ES_INDEX_BIGDATA_DEVICE, EsTableName.ES_TYPE_DEVICE, b._1, DeviceDesBuidler.deviceDes2EsDoc(b._2))

            })
            tbConn.close()
//            esConn.close()
        })
    }
}

object MobileJobDeviceDes {
    def main(args: Array[String]) {
        val job = new MobileJobDeviceDes()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
