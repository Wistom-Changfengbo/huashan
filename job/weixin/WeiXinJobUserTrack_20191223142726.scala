package com.wangjia.bigdata.core.job.weixin

import com.wangjia.bigdata.core.bean.info._
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.bean.weixin.WeixinLogMsg
import com.wangjia.bigdata.core.handler.{BeanHandler, VisitTrackHandler}
import com.wangjia.bigdata.core.job.SparkExtend
import com.wangjia.bigdata.core.job.common.JobUserTrack
import com.wangjia.bigdata.core.utils.JAInfoUtils
import com.wangjia.common.{LogEventId, LogType}
import com.wangjia.es.{EsTableName, ExEsConnection}
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.hbase.{ExHbase, HBaseTableName}
import com.wangjia.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable

class WeiXinJobUserTrack extends JobUserTrack {

    //应用信息
    private var mapAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null

    //页面规则
    private var pageRuleInfosBroadcast: Broadcast[mutable.Map[String, Array[PageInfo]]] = null

    //页面规则Map
    private var mapPageRuleInfosBroadcast: Broadcast[Map[Int, PageInfo]] = null

    override protected def init(): Unit = {
        super.init()
        val sc = SparkExtend.ctx

        //加载广播应用信息
        val mapAppInfos = JAInfoUtils.loadAppInfos
        mapAppInfosBroadcast = sc.broadcast(mapAppInfos)

        //加载广播页面规则
        val apps = JAInfoUtils.loadAppInfos
        val pageRuleInfos = JAInfoUtils.loadPageInfoByAppInfos(apps)
        pageRuleInfosBroadcast = sc.broadcast(pageRuleInfos)

        //加载广播页面规则Map
        val map = pageRuleInfos.flatMap(_._2).map(x => (x.id, x)).toMap
        mapPageRuleInfosBroadcast = sc.broadcast(map)
    }

    private def filterBean(bean: WeixinLogMsg): Boolean = {
        if (bean == null)
            return false
        bean.logtype match {
            case LogType.PAGE => return true
            case LogType.EVENT => {
                //页面点击事件 定位事件 应用列表事件 页面点击事件
                if (bean.subtype == LogEventId.UI_CLICK_POINT
                        || bean.subtype == LogEventId.APP_LOCATION
                        || bean.subtype == LogEventId.APP_APPLIST
                        || bean.subtype == "page_action")
                    return false
                return true
            }
        }
        false
    }

    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.UUID_VISIT)
        HBaseUtils.createTable(HBaseTableName.UUID_VISIT_DES)

        val sc = SparkExtend.ctx

        val linesRdd = sc.textFile(this.inPath)

        val beansRdd = linesRdd.map(BeanHandler.toWeiXin).filter(filterBean)

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
                .map(x => VisitTrackHandler.countUserVisit(x._2.toList.sortWith(_.time < _.time), pageRuleInfosBroadcast.value, mapAppInfosBroadcast.value.getOrElse(x._1._1, null)))
                .filter(x => x != null && x.pReqs.length + x.pEvents.length > 0)

        saveVisitRdd(visitBeanRdd)
    }
}

object WeiXinJobUserTrack {
    def main(args: Array[String]) {
        val job = new WeiXinJobUserTrack()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}