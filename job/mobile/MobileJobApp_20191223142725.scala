package com.wangjia.bigdata.core.job.mobile

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.wangjia.bean.EsAppListDocBean
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.common.LogEventId
import com.wangjia.es.{EsTableName, ExEsConnection}
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.hbase.{HBaseConst, HBaseTableName}
import com.wangjia.utils.{HBaseUtils, JavaUtils}
import org.apache.hadoop.hbase.client.{Result, _}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 分析用户APP，以及安装和卸载
  *
  * --inPath    日志输入目录
  * --date      日志日期(yyyy-MM-dd)
  *
  * Created by Administrator on 2017/3/22.
  */
class MobileJobApp extends EveryDaySparkJob {

    /**
      * 简析APP列表
      *
      * @param log
      * @return
      */
    def decodeLog2AppMap(log: String): mutable.Map[String, String] = {
        val map = mutable.Map[String, String]()
        try {
            val appJosns = JSON.parseObject(log).getJSONArray("app_list")
            var i = 0
            val maxApp = appJosns.size()
            while (i < maxApp) {
                val appNode = appJosns.getJSONObject(i)
                val pkg_name = appNode.getString("pkg_name").trim
                    val app_name = appNode.getString("app_name").trim
                    if (app_name.length >= 1) {
                    map += ((pkg_name, app_name))
                }
                i += 1
            }
        } catch {
            case e: Exception => e.printStackTrace()
        }
        map
    }

    /**
      * 读取APP列表、最后的时间
      *
      * @param rs
      * @return
      */
    def readAppMapAndLastTime(rs: Result): (Long, mutable.Map[String, String]) = {
        if (rs.isEmpty || !rs.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_LASTTIME))
            return (0L, null)

        val lastTime: Long = Bytes.toLong(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_LASTTIME))
        val map = mutable.Map[String, String]()
        try {
            val msg: String = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG))
            val appJosns = JSON.parseObject(msg)
            appJosns.entrySet().foreach(item => {
                map += ((item.getKey, item.getValue.asInstanceOf[String]))
            })
        } catch {
            case e: Exception => e.printStackTrace()
        }

        (lastTime, map)
    }

    /**
      * 计算APP列表变化
      *
      * @param newApp
      * @param oldApp
      * @return (ope,pkgname,appname)
      */
    def computeChange(newApp: mutable.Map[String, String], oldApp: mutable.Map[String, String]): ListBuffer[(String, String, String)] = {
        val operationLog = new ListBuffer[(String, String, String)]
        newApp.foreach(item => {
            if (!oldApp.contains(item._1))
                operationLog += (("add", item._1, item._2))
        })
        oldApp.foreach(item => {
            if (!newApp.contains(item._1))
                operationLog += (("del", item._1, item._2))
        })
        operationLog
    }

    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.UUID_APPLIST)

        val sc = SparkExtend.ctx
        val linesRdd = sc.textFile(this.inPath)
        //得到APPLIST事件日志
        val beanRdd = linesRdd.map(BeanHandler.toAppEvent(LogEventId.APP_APPLIST) _).filter(_ != null)

        //取每个用户最后次APPLIST信息
        val lastAppLogRdd = beanRdd.map(x => (x.uuid, x))
                .reduceByKey((x1, x2) => if (x1.time > x2.time) x1 else x2)
                .map(_._2)

        //简析APP列表
        val appListRdd = lastAppLogRdd.map(x => (x, decodeLog2AppMap(x.data)))
                .filter(_._2.nonEmpty)

        //加载以前的APP列表
        val appRdd = appListRdd.mapPartitions(iterator => {
            initHBase()
            val conn = new ExTBConnection
            val tb = conn.getTable(HBaseTableName.UUID_APPLIST)

            //bean、新的APP列表、最近入库的时间、老的APP列表
            val list = new ListBuffer[(MobileLogMsg, mutable.Map[String, String], Long, mutable.Map[String, String])]
            val cacheBeans = new ListBuffer[(MobileLogMsg, mutable.Map[String, String])]
            val cacheGets = new ListBuffer[Get]
            iterator.foreach(x => {
                cacheBeans += x
                cacheGets += new Get(Bytes.toBytes(x._1.uuid))
                if (cacheGets.length > Config.BATCH_SIZE) {
                    val results: Array[Result] = tb.getTable.get(cacheGets)
                    var i = 0
                    val max = results.length
                    while (i < max) {
                        val oldApp = readAppMapAndLastTime(results(i))
                        val newApp = cacheBeans.get(i)
                        list += ((newApp._1, newApp._2, oldApp._1, oldApp._2))
                        i += 1
                    }
                    cacheBeans.clear()
                    cacheGets.clear()
                }
            })
            if (cacheGets.nonEmpty) {
                val results: Array[Result] = tb.getTable.get(cacheGets)
                var i = 0
                val max = results.length
                while (i < max) {
                    val oldApp = readAppMapAndLastTime(results(i))
                    val newApp = cacheBeans.get(i)
                    list += ((newApp._1, newApp._2, oldApp._1, oldApp._2))
                    i += 1
                }
                cacheBeans.clear()
                cacheGets.clear()
            }
            conn.close()
            list.toIterator
        })

        //保存入库
        appRdd.filter(x => JavaUtils.timeMillis2DayNum(x._1.time) - JavaUtils.timeMillis2DayNum(x._3) > 0)
                .foreachPartition(iterator => {
                    initHBase()
                    val conn = new ExTBConnection
                    val tb = conn.getTable(HBaseTableName.UUID_APPLIST)
                    val esConn: ExEsConnection = new ExEsConnection()

                    iterator.foreach(bean => {
                        val uuid: String = bean._1.uuid
                        val newTime: Long = bean._1.time
                        val newApp: mutable.Map[String, String] = bean._2
                        val oldTime: Long = bean._3
                        val oldApp: mutable.Map[String, String] = bean._4

                        val put = new Put(Bytes.toBytes(uuid))
                        put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_LASTTIME, Bytes.toBytes(newTime))
                        if (oldTime == 0) {
                            put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_FIRSTTIME, Bytes.toBytes(newTime))
                        }
                        val json = new JSONObject()
                        val pkgList = new ListBuffer[String]()
                        val appNameList = new ListBuffer[String]()
                        newApp.foreach(item => {
                            pkgList.add(item._1)
                            appNameList.add(item._2)
                            json.put(item._1, item._2)
                        })

                        //写入操作
                        if (oldApp != null && oldApp.nonEmpty) {
                            val change: ListBuffer[(String, String, String)] = computeChange(newApp, oldApp)
                            if (change.nonEmpty) {
                                val jsonChange = new JSONObject()
                                val opes = new JSONArray()
                                jsonChange.put("opes", opes)
                                jsonChange.put("time", this.logDate.getTime)
                                change.foreach(item => {
                                    val _obj = new JSONObject()
                                    _obj.put("ope", item._1)
                                    _obj.put("pkgname", item._2)
                                    _obj.put("appname", item._3)
                                    opes.add(_obj)
                                })
                                val key = "_ope_" + JavaUtils.timeMillis2DayNum(this.logDate.getTime)
                                put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes(key), Bytes.toBytes(jsonChange.toString))
                            }
                        }
                        val platform: String = bean._1.dStrInfo("platform")
                        val deviceid: String = bean._1.dStrInfo("deviceid")
                        esConn.add(EsTableName.ES_INDEX_BIGDATA_APP, EsTableName.ES_TYPE_APPLIST, uuid, new EsAppListDocBean(pkgList, appNameList, newTime, platform, uuid, deviceid))
                        put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG, Bytes.toBytes(json.toString))
                        tb.addPut(put)
                    })

                    conn.close()
                    esConn.close()
                })
    }
}

object MobileJobApp {
    def main(args: Array[String]) {
        val job = new MobileJobApp()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
