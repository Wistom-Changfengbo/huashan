package com.wangjia.bigdata.core.job.mobile

import com.google.gson.JsonParser
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.common.LogType
import com.wangjia.hbase.conn.{ExTBConnection, ExTable}
import com.wangjia.hbase.{HBaseConst, HBaseTableName}
import com.wangjia.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.StringBuilder


/**
  * 分析用户操作的物品
  *
  * --inPath    日志输入目录
  * --date      日志日期(yyyy-MM-dd)
  * --output    日志输出目录
  *
  * Created by Administrator on 2017/4/27.
  */
class MobileJobItem extends EveryDaySparkJob {

    /**
      * @param uuid      用户唯一ID
      * @param appid     APPID
      * @param eventType 事件类型
      * @param itemType  1是图片，2是案例
      * @param itemId    物品ID
      * @param time      时间戳
      * @param baseValue 基础值
      * @param stayTime  停留时间
      *
      */
    case class ItemLabel(uuid: String, appid: String, eventType: String, itemType: Int, itemId: Int, time: Long, baseValue: Float, stayTime: Long)

    private def logBean2Item(bean: MobileLogMsg): ItemLabel = {
        try {
            val uuid = bean.uuid
            val appId = bean.appid
            var eventType: String = ""
            var itemType: Int = 0

            var baseValue: Float = 0
            var stayTime: Long = 0

            if (bean.logtype == LogType.PAGE) {
                //画廊（大图浏览）
                if (bean.pageid == "details_gallery") {
                    eventType = LogType.PAGE + "#" + bean.pageid
                    itemType = 1
                    baseValue = 0
                    stayTime = bean.staytime
                    //案例详情
                } else if (bean.pageid == "details_case") {
                    eventType = LogType.PAGE + "#" + bean.pageid
                    itemType = 2
                    baseValue = 0
                    stayTime = bean.staytime
                } else {
                    return null
                }
            }
            else if (bean.logtype == LogType.EVENT) {
                //分享图片 保存图片 图片收藏
                if (bean.subtype == "share_photo"
                        || bean.subtype == "save_photo"
                        || bean.subtype == "collection_photo") {
                    eventType = LogType.EVENT + "#" + bean.subtype
                    itemType = 1
                    baseValue = 5.0F
                    stayTime = 0

                    //分享案例 案例收藏
                } else if (bean.subtype == "share_case"
                        || bean.subtype == "collection_case") {
                    eventType = LogType.EVENT + "#" + bean.subtype
                    itemType = 2
                    baseValue = 5.0F
                    stayTime = 0
                } else {
                    return null
                }
            }

            if (itemType == 0 || baseValue + stayTime == 0)
                return null

            //设计师详情
            //if (bean.logtype == LogType.PAGE && bean.subtype == "details_designer")

            if (bean.data == null || bean.data.length == 0)
                return null
            val data = new JsonParser().parse(bean.data).getAsJsonObject
            val itemId: Int = {
                val _obj = data.get("id")
                if (_obj == null)
                    return null
                if (_obj.isJsonObject)
                    _obj.getAsJsonObject.get("id").getAsInt
                else
                    _obj.getAsInt
            }

            return ItemLabel(uuid, appId, eventType, itemType, itemId, bean.time, baseValue, stayTime)
        } catch {
            case e: Exception =>
                e.printStackTrace()
                println(bean)
        }
        null
    }

    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.UUID_ITEM_STAR)

        val sc = SparkExtend.ctx
        val linesRdd = sc.textFile(this.inPath)
        val beanRdd = linesRdd.map(BeanHandler.toAppAcc).filter(_ != null)
        val ss =beanRdd.filter(x=>( x.subtype == "share_photo" || x.subtype == "save_photo" || x.subtype == "collection_photo" || x.subtype == "share_case" || x.subtype == "collection_case")).take(10)
        ss.map(logBean2Item)
        val itemLabelRdd = beanRdd.map(logBean2Item)
                .filter(x => x != null && x.itemId > 0 && x.uuid.length > 0)

        //写入HBase
        itemLabelRdd.foreachPartition(iterator => {
            initHBase()
            val conn: ExTBConnection = new ExTBConnection
            val tb: ExTable = conn.getTable(HBaseTableName.UUID_ITEM_STAR)
            iterator.foreach(item => {
                val put = new Put(Bytes.toBytes(item.uuid))
                put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes(item.itemType + "#" + item.itemId), Bytes.toBytes(item.stayTime))
                tb.addPut(put)
            })
            conn.close()
        })

        val resultRdd = itemLabelRdd.map(item => {
            val sb: StringBuilder = new StringBuilder
            sb.append(item.uuid).append(',')
            sb.append(item.appid).append(',')
            sb.append(item.eventType).append(',')
            sb.append(item.itemType).append(',')
            sb.append(item.itemId).append(',')
            sb.append(item.time).append(',')
            sb.append(item.baseValue).append(',')
            sb.append(item.stayTime)
            sb.toString
        })

        resultRdd.foreach(println)
        resultRdd.repartition(1).saveAsTextFile(this.outPath)
    }
}

object MobileJobItem {
    def main(args: Array[String]): Unit = {
        val job = new MobileJobItem()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}