package com.wangjia.bigdata.core.job.mobile

import java.util

import com.alibaba.fastjson.{JSON, JSONArray}
import com.google.gson.Gson
import com.wangjia.bigdata.core.bean.info.AppTagInfo
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.JDBCBasicUtils
import com.wangjia.common.LogEventId
import com.wangjia.es.{EsTableName, ExEsConnection}
import com.wangjia.hbase.conn.{ExTBConnection, ExTable}
import com.wangjia.hbase.{HBaseConst, HBaseTableName}
import com.wangjia.utils.HBaseUtils
import org.apache.commons.httpclient.HttpStatus
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * AppList对应app标签统计
  *
  * Created by Cfb on 2018/1/3.
  */

class MobileJobAppLabel extends EveryDaySparkJob {

    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.UUID_LABEL_APPLIST)
        val sc = SparkExtend.ctx
        val spark = SparkSession.builder().appName(this.jobName).getOrCreate()

        val reader = spark.sqlContext.read.format("jdbc")
        reader.option("url", JDBCBasicUtils.getProperty("info.url"))
        reader.option("dbtable", "ja_info_apptag")
        reader.option("driver", JDBCBasicUtils.getProperty("info.classname"))
        reader.option("user", JDBCBasicUtils.getProperty("info.username"))
        reader.option("password", JDBCBasicUtils.getProperty("info.password"))

        //加载APP标签信息
        val infoRdd: RDD[AppTagInfo] = reader.load().rdd.map(x => {
            val id: Int = x.getInt(0)
            val platform: String = x.getString(1)
            val pkg_name: String = x.getString(2)
            val tag_id: Int = x.getInt(3)
            val tag_name: String = x.getString(4)
            val sex_weight: Double = x.getDouble(5)
            val user_tag_id: Int = x.getInt(6)
            val user_tag_name: String = x.getString(7)
            val user_tag_weight: Double = x.getDouble(8)
            val mutex: String = x.getString(9)
            AppTagInfo(id, platform, pkg_name, tag_id, tag_name, sex_weight, user_tag_id, user_tag_name, user_tag_weight, mutex)
        })

        //得到互斥关系
        val mutexInfo = infoRdd.filter(info => info.user_tag_id != 0 && info.mutex.nonEmpty)
                .map(info => (info.user_tag_id, info.mutex, info.user_tag_weight))
                .distinct()
                .map(x => (x._1, x._2.split(',').map(_.toInt).toList, x._3))
                .collect()
                .sortWith(_._3 > _._3)
                .map(x => (x._1, x._2)).toMap

        //得ID2NAME
        val id2name = infoRdd.filter(info => info.user_tag_id != 0)
                .map(x => (x.user_tag_id, x.user_tag_name))
                .distinct()
                .collect().toMap

        //((platform,pkgName),uuid)
        val appRdd: RDD[((String, String), String)] = sc.textFile(this.inPath)
                .map(BeanHandler.toAppEvent(LogEventId.APP_APPLIST) _)
                .filter(_ != null)
                .map(x => (x.uuid, x))
                .reduceByKey((x1, x2) => if (x1.time > x2.time) x1 else x2)
                .map(_._2)
                .flatMap(getUuidAndApplist)

        appRdd.join(infoRdd.map(info => ((info.platform, info.pkg_name), info)))
                .map(_._2)
                .groupByKey()
                .map { case (uuid, infos) =>
                    var sex: Double = 0
                    val appTag = new java.util.HashMap[String, Int]()
                    val userIdTag = new java.util.HashMap[Int, Int]()
                    infos.foreach(info => {
                        sex += info.sex_weight
                        val tag_name = info.tag_name
                        if (tag_name.nonEmpty) {
                            appTag.put(tag_name, appTag.getOrDefault(tag_name, 0) + 1)
                        }
                        if (info.user_tag_id != 0) {
                            userIdTag.put(info.user_tag_id, userIdTag.getOrDefault(info.user_tag_id, 0) + 1)
                        }
                    })
                    mutexInfo.foreach { case (id, list) =>
                        if (userIdTag.containsKey(id))
                            list.foreach(userIdTag.remove)
                    }
                    val userTag = new java.util.HashMap[String, Int]()
                    import scala.collection.JavaConversions._
                    userIdTag.entrySet().foreach(item => userTag.put(id2name.getOrElse(item.getKey, "-"), item.getValue))

                    val consumenum: Int = {
                        try {
                            val url = "http://192.168.110.2:5000/api/getuserconsume_byuuid?uuid=" + uuid
                            val httpClient = new DefaultHttpClient()
                            val get = new HttpGet(url)
                            val response: CloseableHttpResponse = httpClient.execute(get)
                            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK)
                                JSON.parseObject(EntityUtils.toString(response.getEntity, "UTF-8")).getIntValue("consumenum")
                            else
                                -1
                        } catch {
                            case e: Exception => e.printStackTrace(); -1
                        }
                    }
                    (uuid, sex, consumenum, appTag, userTag, infos.head.platform)
                }
                .foreachPartition(iterator => {
                    initHBase()
                    val conn = new ExTBConnection
                    val tb: ExTable = conn.getTable(HBaseTableName.UUID_LABEL_APPLIST)
                    val esConn: ExEsConnection = new ExEsConnection()
                    val gson: Gson = new Gson()
                    val esMap = new java.util.HashMap[String, Object]()
                    val esAppTagKey = new util.LinkedList[String]()
                    val esAppTagValue = new util.LinkedList[Int]()
                    val esUserTagKey = new util.LinkedList[String]()
                    val esUserTagValue = new util.LinkedList[Int]()


                    iterator.foreach(x => {
                        val put = new Put(Bytes.toBytes(x._1))
                        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("sex"), Bytes.toBytes(x._2))
                        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("consume"), Bytes.toBytes(x._3))
                        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("apptag"), Bytes.toBytes(gson.toJson(x._4)))
                        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("usertag"), Bytes.toBytes(gson.toJson(x._5)))
                        put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_PLATFORM, Bytes.toBytes(x._6))
                        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("updatetime"), Bytes.toBytes(this.logDate.getTime))

                        tb.addPut(put)

                        esMap.clear()
                        esAppTagKey.clear()
                        esAppTagValue.clear()
                        esUserTagKey.clear()
                        esUserTagValue.clear()

                        import scala.collection.JavaConversions._
                        x._4.entrySet().foreach(entry => {
                            esAppTagKey.add(entry.getKey)
                            esAppTagValue.add(entry.getValue)
                        })
                        x._5.entrySet().foreach(entry => {
                            esUserTagKey.add(entry.getKey)
                            esUserTagValue.add(entry.getValue)
                        })
                        esMap.put("sex", new java.lang.Double(x._2))
                        esMap.put("consume", new java.lang.Integer(x._3))
                        esMap.put("apptagkey", esAppTagKey)
                        esMap.put("apptagvalue", esAppTagValue)
                        esMap.put("usertagkey", esUserTagKey)
                        esMap.put("usertagvalue", esUserTagValue)
                        esMap.put("platform", x._6)
                        esMap.put("updatetime",new java.lang.Long(this.logDate.getTime))

                        esConn.add(EsTableName.ES_INDEX_BIGDATA_APP_LABEL, EsTableName.ES_TYPE_APP_LABEL, x._1, gson.toJson(esMap))
                    })
                    conn.close()
                    esConn.close()
                })
    }

    //获取((platform,pkgName),uuid)
    private def getUuidAndApplist(mobileLogSelf: MobileLogMsg): ListBuffer[((String, String), String)] = {
        val data: String = mobileLogSelf.data
        val uuid: String = mobileLogSelf.uuid
        val platform: String = mobileLogSelf.dStrInfo("platform", "-")
        val pkgNames: ListBuffer[((String, String), String)] = new mutable.ListBuffer[((String, String), String)]
        try {
            val array: JSONArray = JSON.parseObject(data).getJSONArray("app_list")
            var i = 0
            val maxApp = array.size()
            while (i < maxApp) {
                val appInfo = array.getJSONObject(i)
                val pkg_name = appInfo.getString("pkg_name").trim
                pkgNames += (((platform, pkg_name), uuid))
                i += 1
            }
        } catch {
            case e: Exception => e.printStackTrace()
        }
        pkgNames
    }
}

object MobileJobAppLabel {
    def main(args: Array[String]) {
        val job = new MobileJobAppLabel()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}

