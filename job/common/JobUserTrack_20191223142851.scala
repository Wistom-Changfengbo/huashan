package com.wangjia.bigdata.core.job.common

import java.lang.Long
import java.util

import com.wangjia.bigdata.core.bean.common.{PageEvent, UserVisit}
import com.wangjia.bigdata.core.job.EveryDaySparkJob
import com.wangjia.es.{EsTableName, ExEsConnection}
import com.wangjia.hbase.conn.{ExTBConnection, ExTable}
import com.wangjia.hbase.{ExHbase, HBaseConst, HBaseTableName}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2017/9/8.
  */
trait JobUserTrack extends EveryDaySparkJob {

    protected def saveVisitRdd(visitBeanRdd: RDD[UserVisit]): Unit = {
        //写入HBase
        visitBeanRdd.foreachPartition(iterator => {
//            initHBase()
            val conn: ExTBConnection = null
            val exTbVisit: ExTable = conn.getTable(HBaseTableName.UUID_VISIT)
            val exTbVisitDes: ExTable = conn.getTable(HBaseTableName.UUID_VISIT_DES)
            iterator.foreach(visit => {
                val rowKey = Bytes.toBytes(ExHbase.getVisitKey(visit.appid, visit.uuid, visit.start))
                val visitPut = new Put(rowKey)
                visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_DEVICEID, Bytes.toBytes(visit.deviceId))
                visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_UUID, Bytes.toBytes(visit.uuid))
                visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_IP, Bytes.toBytes(visit.ip))
                visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_REF, Bytes.toBytes(visit.ref))
                visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_ADDRESS, Bytes.toBytes(visit.add.toJson().toString))
                visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_START, Bytes.toBytes(visit.start))
                visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_TIME, Bytes.toBytes(visit.time))
                visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_NUM, Bytes.toBytes(visit.pReqs.length))
                visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_ENUM, Bytes.toBytes(visit.pEvents.length))
                visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_VERSION, Bytes.toBytes(visit.version))
                visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_NET, Bytes.toBytes(visit.net))
                visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_PLATFORM, Bytes.toBytes(visit.platform))
                visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_APPID, Bytes.toBytes(visit.appid))
                val eventIds = visit.getEventIds
                visitPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_EVENTIDS, Bytes.toBytes(eventIds))
                exTbVisit.addPut(visitPut)

                val desPut = new Put(rowKey)
                desPut.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG, Bytes.toBytes(visit.toJson.toString))
                exTbVisitDes.addPut(desPut)
            })
            conn.close()
        })

        //写入es
        visitBeanRdd.foreachPartition(iterator => {
            val conn: ExEsConnection = null
            iterator.foreach(visit => {
                val map: java.util.Map[java.lang.String, Object] = new java.util.HashMap[java.lang.String, Object]()
                map.put("appid", visit.appid)
                map.put("deviceid", visit.deviceId)
                map.put("uuid", visit.uuid)
                map.put("ip", visit.ip)
                map.put("ref", visit.ref)
                map.put("time", new java.lang.Long(visit.start))
                map.put("staytime", new java.lang.Long(visit.time))
                map.put("platform", new java.lang.Integer(visit.platform))
                map.put("add1", visit.add.country)
                map.put("add2", visit.add.province)
                map.put("add3", visit.add.city)
                map.put("add4", visit.add.area)

                val events: java.util.List[java.lang.String] = new util.LinkedList[java.lang.String]()
                val pEvents: Array[PageEvent] = visit.pEvents
                if (pEvents != null && pEvents.nonEmpty) {
                    pEvents.foreach(e => {
                        val id = e.eventType + "#" + e.eventId
                        if (!events.contains(id))
                            events.add(id)
                    })
                    map.put("events", events)
                }
                conn.add(EsTableName.ES_INDEX_BIGDATA_VISIT, EsTableName.ES_TYPE_VISIT, ExHbase.getVisitKey(visit.appid, visit.uuid, visit.start), map)
            })
            conn.close()
        })
    }
}
