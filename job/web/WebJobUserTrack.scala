package com.wangjia.bigdata.core.job.web

import com.wangjia.bigdata.core.bean.info.SourceInfo
import com.wangjia.bigdata.core.handler.{BeanHandler, VisitTrackHandler}
import com.wangjia.bigdata.core.job.SparkExtend
import com.wangjia.bigdata.core.job.common.JobUserTrack
import com.wangjia.bigdata.core.utils.JAInfoUtils
import com.wangjia.es.{EsTableName, ExEsConnection}
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.hbase.{ExHbase, HBaseTableName}
import com.wangjia.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast

class WebJobUserTrack extends JobUserTrack {

    //来源地址映射
    private var sourceInfosBroadcast: Broadcast[Array[SourceInfo]] = null

    override protected def init(): Unit = {
        super.init()
        //加载广播来源地址映射
        val sourceInfos = JAInfoUtils.loadSourceInfos
        sourceInfosBroadcast = SparkExtend.ctx.broadcast(sourceInfos)
    }

    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.UUID_VISIT)
        HBaseUtils.createTable(HBaseTableName.UUID_VISIT_DES)

        val sc = SparkExtend.ctx
        val linesRdd = sc.textFile(this.inPath)
        val beansRdd = linesRdd.map(BeanHandler.toWebAll)
                .filter(_ != null)

        //清除当天数据
        beansRdd.foreachPartition(iterator => {
            initHBase()
            val tbConn = new ExTBConnection
            val esConn = new ExEsConnection
            val tbUuid2visit = tbConn.getTable(HBaseTableName.UUID_VISIT)
            val tbUuid2visitdes = tbConn.getTable(HBaseTableName.UUID_VISIT_DES)
            while (iterator.hasNext) {
                val b = iterator.next()
                val k1 = ExHbase.getVisitKey(b.appid, b.uuid, b.time)
                tbUuid2visit.addDelete(new Delete(Bytes.toBytes(k1)))
                val k2 = ExHbase.getVisitDesKey(b.appid, b.uuid, b.time)
                tbUuid2visitdes.addDelete(new Delete(Bytes.toBytes(k2)))
                esConn.delete(EsTableName.ES_INDEX_BIGDATA_VISIT, EsTableName.ES_TYPE_VISIT, k1)
            }
            tbConn.close()
            esConn.close()
        })

        //计算Visit
        val visitBeanRdd = beansRdd.groupBy(x => (x.appid, x.uuid, x.visitindex))
                .map(x => VisitTrackHandler.countUserVisit(x._2.toList.sortWith(_.time < _.time), sourceInfosBroadcast.value))
                .filter(x => x != null && x.pReqs.length + x.pEvents.length > 0)

        saveVisitRdd(visitBeanRdd)
    }
}

object WebJobUserTrack {
    def main(args: Array[String]) {
        val job = new WebJobUserTrack()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}