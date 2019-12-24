package com.wangjia.bigdata.core.job.tool

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.IPUtils
import com.wangjia.es.{EsTableName, ExEsConnection}
import com.wangjia.hbase.HBaseConst
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HTableDescriptor, TableName}

import scala.collection.JavaConversions._

/**
  * 同步Visit数据到Es
  *
  * Created by Administrator on 2017/4/2.
  */
class Visit2Es extends EveryDaySparkJob {

    override protected def job(args: Array[String]): Unit = {
        val sc = SparkExtend.ctx

        //初始化配置
        val tablename = "v7uuid2visit"
        val conf = initHBase()
        conf.set(TableInputFormat.INPUT_TABLE, tablename)

        // 如果表不存在则创建表
        val admin = new HBaseAdmin(conf)
        if (!admin.isTableAvailable(tablename)) {
            val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
            admin.createTable(tableDesc)
        }

        //读取数据并转化成rdd
        val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])

        hBaseRDD.foreachPartition { case (iterator) => {
            val conn: ExEsConnection = new ExEsConnection
            iterator.foreach { case (_, result) => {
                val key = Bytes.toString(result.getRow)
                val appid = Bytes.toString(result.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_APPID))
                val deviceid = Bytes.toString(result.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_DEVICEID))
                val uuid = Bytes.toString(result.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_UUID))
                val ip = Bytes.toString(result.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_IP))
                val ref = Bytes.toString(result.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_REF))
                val time = Bytes.toLong(result.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_START))
                val staytime = Bytes.toLong(result.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_TIME))
                val platform = Bytes.toInt(result.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_PLATFORM))

                val eventids = Bytes.toString(result.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_EVENTIDS))
                val obj: JSONObject = JSON.parseObject(eventids)
                val events: java.util.List[String] = new util.LinkedList[String]()
                if (obj != null)
                    obj.keySet().foreach(key => events.add(key))

                val map: java.util.Map[java.lang.String, Object] = new java.util.HashMap[java.lang.String, Object]()
                map.put("appid", appid)
                map.put("deviceid", deviceid)
                map.put("uuid", uuid)
                map.put("ip", ip)
                map.put("ref", ref)
                map.put("time", new java.lang.Long(time))
                map.put("staytime",new java.lang.Long(staytime))
                map.put("platform", new java.lang.Integer(platform))
                val add = IPUtils.getAddress(ip)
                map.put("add1", add.country)
                map.put("add2", add.province)
                map.put("add3", add.city)
                map.put("add4", add.area)
                if (events.nonEmpty) {
                    map.put("events", events)
                }
                conn.add(EsTableName.ES_INDEX_BIGDATA_VISIT, EsTableName.ES_TYPE_VISIT, key, map)
            }}
            conn.close()
        }}

        sc.stop()
        admin.close()
    }
}

object Visit2Es {
    def main(args: Array[String]) {
        val job = new Visit2Es()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
