package com.wangjia.bigdata.core.job.mobile

import java.util

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.JAInfoUtils
import com.wangjia.hbase.conn.{ExTBConnection, ExTable}
import com.wangjia.hbase.{HBaseConst, HBaseTableName}
import com.wangjia.utils.{HBaseUtils, JavaUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ListBuffer
import scala.collection.{JavaConverters, mutable}

/**
  * 分析APP引导标签
  *
  * --inPath    日志输入目录
  * --date      日志日期(yyyy-MM-dd)
  *
  * Created by Administrator on 2017/4/27.
  */
class MobileJobGuideLabel extends EveryDaySparkJob {

    /**
      * 图片属性配置
      */
    private var photoAttriInfoBroadcast: Broadcast[mutable.Map[String, String]] = null

    override protected def init(): Unit = {
        super.init()
        val sc = SparkExtend.ctx
        //加载图片属性
        val photoAttriInfo: mutable.Map[String, String] = JAInfoUtils.loadPhotoAttributeInfo
        photoAttriInfoBroadcast = sc.broadcast(photoAttriInfo)
    }

    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.UUID_LABEL_GUIDE)

        val sc = SparkExtend.ctx
        val lines = sc.textFile(this.inPath)
        //提取引导事件数据
        val logBeanRdd = lines.map(BeanHandler.toAppEvent("app_guide_data") _)
                .filter(_ != null)

        val guideLabelRdd = logBeanRdd.map(bean => {
            val labels = new ListBuffer[String]
            try {
                val jMap: util.Map[String, String] = JavaUtils.parseJson(bean.data, classOf[util.Map[String, String]])
                val sMap: mutable.Map[String, String] = JavaConverters.mapAsScalaMapConverter(jMap).asScala
                val photoAttriInfo: mutable.Map[String, String] = photoAttriInfoBroadcast.value
                sMap.foreach(item => {
                    val key = item._1 + item._2
                    if (photoAttriInfo.contains(key)) {
                        labels += photoAttriInfo.getOrElse(key, "")
                    }
                })
            } catch {
                case e: Exception => e.printStackTrace(); println(bean.uuid, bean.data)
            }
            (bean.uuid, labels)
        }).filter(_._2.nonEmpty)

        //写入HBase
        guideLabelRdd.foreachPartition(iterator => {
            initHBase()
            val conn: ExTBConnection = new ExTBConnection
            val tb: ExTable = conn.getTable(HBaseTableName.UUID_LABEL_GUIDE)
            iterator.foreach(item => {
                val json = new JSONObject()
                json.put("time", this.logDate.getTime)
                val labelArray = new JSONArray()
                item._2.foreach(l => {
                    val obj = new JSONObject()
                    obj.put("key", l)
                    obj.put("value", 5.0)
                    labelArray.add(obj)
                })
                json.put("labels", labelArray)
                val put = new Put(Bytes.toBytes(item._1))
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG, Bytes.toBytes(json.toString))
                tb.addPut(put)
            })
            conn.close()
        })
    }
}

object MobileJobGuideLabel {
    def main(args: Array[String]) {
        val job = new MobileJobGuideLabel()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
