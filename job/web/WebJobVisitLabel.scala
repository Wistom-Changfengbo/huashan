package com.wangjia.bigdata.core.job.web

import com.google.gson.{JsonArray, JsonObject, JsonParser}
import com.wangjia.bigdata.core.bean.info.{AppInfo, UserLabel}
import com.wangjia.bigdata.core.bean.web.WebLogMsg
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.common.ConfigWebLabel
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.JAInfoUtils
import com.wangjia.common.LogType
import com.wangjia.hbase.conn.{ExTBConnection, ExTable}
import com.wangjia.hbase.{HBaseConst, HBaseTableName}
import com.wangjia.math.ExMath
import com.wangjia.utils.{HBaseUtils, JavaUtils, LabelUtils}
import org.apache.hadoop.hbase.client.{Delete, Get, Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class WebJobVisitLabel extends EveryDaySparkJob {

    //用户标签配置信息
    private var userLabelInfosBroadcast: Broadcast[mutable.Map[Int, ListBuffer[UserLabel]]] = null

    //应用信息
    private var mapAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null

    /**
      * 标签
      *
      * @param uuid        设备号
      * @param labelName   标签名
      * @param sumTimes    总次数
      * @param sumStayTime 总停留时间
      * @param weight      权重
      */
    case class LabelDes(uuid: String, labelName: String, sumTimes: Int, sumStayTime: Long, weight: Double)

    override protected def init(): Unit = {
        super.init()
        val sc = SparkExtend.ctx

        //加载广播用户标签配置信息
        val userLabelInfos = JAInfoUtils.loadUserLabelInfo
        userLabelInfosBroadcast = sc.broadcast(userLabelInfos)

        //加载广播应用信息
        val mapAppInfos = JAInfoUtils.loadAppInfos
        mapAppInfosBroadcast = sc.broadcast(mapAppInfos)
    }

    /**
      * 访问页面转化成标签
      *
      * @param uuid
      * @param companyId
      * @param beans
      * @return
      */
    private def pageBeans2Labels(uuid: String, companyId: Int, beans: List[WebLogMsg]): List[LabelDes] = {
        val map = mutable.Map[String, (Int, Long)]()
        val labelList = new ListBuffer[LabelDes]
        val userLabelInfos = userLabelInfosBroadcast.value.getOrElse(companyId, null)
        if (userLabelInfos == null)
            return labelList.toList

        beans.foreach(b => {
            val dt: Long = b.staytime
            val title = b.title
            userLabelInfos.foreach(x => {
                if (x.isMeet(title)) {
                    val ct: (Int, Long) = map.getOrElse(x.name, (0, 0L))
                    map.put(x.name, (ct._1 + 1, ct._2 + dt))
                }
            })
            //用户活跃度
            val ct: (Int, Long) = map.getOrElse(ConfigWebLabel.SELF_DEFINITION_LABEL_LIVENESS, (0, 0L))
            map.put(ConfigWebLabel.SELF_DEFINITION_LABEL_LIVENESS, (ct._1 + 1, ct._2 + dt))
        })

        for (kv <- map) {
            labelList += LabelDes(uuid, kv._1, kv._2._1, kv._2._2, ConfigWebLabel.EVENT_PAGE_WEIGHT)
        }
        labelList.toList
    }

    /**
      * 特殊事件转化成标签
      *
      * @param uuid
      * @param companyId
      * @param beans
      * @return
      */
    private def eventBeans2Labels(uuid: String, companyId: Int, beans: List[WebLogMsg]): List[LabelDes] = {
        val map = mutable.Map[String, (Int, Long)]()
        val labelList = new ListBuffer[LabelDes]
        val userLabelInfos = userLabelInfosBroadcast.value.getOrElse(companyId, null)
        if (userLabelInfos == null)
            return labelList.toList

        beans.foreach(b => {
            //收藏
            if (b.subtype == "favorite") {
                val title = b.title
                userLabelInfos.foreach(x => {
                    if (x.isMeet(title)) {
                        val ct: (Int, Long) = map.getOrElse(x.name, (0, 0L))
                        map.put(x.name, (ct._1, ct._2 + ConfigWebLabel.PAGE_DEFAULT_TIME))
                    }
                })
                //用户活跃度
                val ct: (Int, Long) = map.getOrElse(ConfigWebLabel.SELF_DEFINITION_LABEL_LIVENESS, (0, 0L))
                map.put(ConfigWebLabel.SELF_DEFINITION_LABEL_LIVENESS, (ct._1 + 1, ct._2 + ConfigWebLabel.PAGE_DEFAULT_TIME))
                //用户搜索
            } else if (b.subtype == "user_search") {
                try {
                    val jsonObj = new JsonParser().parse(b.data).getAsJsonObject
                    val _type = jsonObj.get("type").getAsString
                    val _kw = jsonObj.get("keywords").getAsString
                    if (_type.length > 0 && _kw.length > 0) {
                        val key = "_kw_" + _type + "_" + _kw
                        val ct: (Int, Long) = map.getOrElse(key, (0, 0L))
                        map.put(key, (ct._1 + 1, ct._2 + ConfigWebLabel.PAGE_DEFAULT_TIME))
                    }

                    //用户活跃度
                    val ct: (Int, Long) = map.getOrElse(ConfigWebLabel.SELF_DEFINITION_LABEL_LIVENESS, (0, 0L))
                    map.put(ConfigWebLabel.SELF_DEFINITION_LABEL_LIVENESS, (ct._1 + 1, ct._2 + ConfigWebLabel.PAGE_DEFAULT_TIME))
                } catch {
                    case e: Exception => e.printStackTrace();
                }
            }
        })

        for (kv <- map) {
            labelList += LabelDes(uuid, kv._1, kv._2._1, kv._2._2, ConfigWebLabel.EVENT_IMPORTANCE_SELF_WEIGHT)
        }
        labelList.toList
    }

    /**
      * 计算基础权重
      *
      * @param uuid
      * @param companyId
      * @param beans
      * @return
      */
    private def countBaseWeight(uuid: String, companyId: Int, beans: List[WebLogMsg]): Double = {
        var dBaseWeight: Double = 0.0
        val userLabelInfos = userLabelInfosBroadcast.value.getOrElse(companyId, null)
        if (userLabelInfos == null)
            return dBaseWeight

        val pageCount = {
            val size = beans.size
            if (size > 200)
                200
            else
                size
        }
        dBaseWeight += pageCount / 200.0

        val eventSubTyes = beans.map(_.subtype).distinct
        val eventCount = {
            val size = eventSubTyes.size
            if (size > 10)
                10
            else
                size
        }
        dBaseWeight += 0.5 * eventCount / 10.0

        if (eventSubTyes.contains("favorite")) {
            dBaseWeight += 0.4F
        }
        if (eventSubTyes.contains("user_search")) {
            dBaseWeight += 0.4F
        }

        dBaseWeight
    }

    /**
      * 聚合标签集合
      *
      * @param ls1
      * @param ls2
      * @return
      */
    private def reduceLebelDess(ls1: List[LabelDes], ls2: List[LabelDes]): List[LabelDes] = {
        //[(UUID, LabelName, Weight), (SumTimes, SumStayTime)]
        val map = mutable.Map[(String, String, Double), (Int, Long)]()
        val funAddLabelDes = (label: LabelDes) => {
            val key = (label.uuid, label.labelName, label.weight)
            val lc = map.getOrElse(key, (0, 0L))
            map.put(key, (lc._1 + label.sumTimes, lc._2 + label.sumStayTime))
        }
        ls1.foreach(funAddLabelDes)
        ls2.foreach(funAddLabelDes)
        map.toList.map(x => LabelDes(x._1._1, x._1._2, x._2._1, x._2._2, x._1._3))
    }

    /**
      *
      * @param result
      * @param uuid
      * @param companyId
      * @return
      */
    private def countSumLabel(result: Result, uuid: String, companyId: Int): mutable.Map[String, Double] = {
        val dayNum = JavaUtils.timeMillis2DayNum(logDate.getTime)
        val map = mutable.Map[String, Double]()
        try {
            var num = 0
            while (num < 60) {
                val day = dayNum - num
                val key = Bytes.toBytes(day.toString + "#" + companyId)
                if (result.containsColumn(HBaseConst.BYTES_CF1, key)) {
                    val str = Bytes.toString(result.getValue(HBaseConst.BYTES_CF1, key))
                    val json = new JsonParser().parse(str).getAsJsonObject
                    val baseWeight = json.get("weight").getAsFloat
                    val labelArrayJson = json.get("labels").getAsJsonArray
                    val size = labelArrayJson.size()
                    var i = 0
                    while (i < size) {
                        val obj = labelArrayJson.get(i).getAsJsonObject
                        val name = obj.get("name").getAsString
                        val times = obj.get("times").getAsInt
                        val stayTime = obj.get("stayTime").getAsLong
                        val weight = obj.get("weight").getAsFloat
                        val value = ExMath.attenuation(LabelUtils.getWebLabelValue(times, stayTime, weight, baseWeight), num)
                        map.put(name, value + map.getOrElse(name, 0.0))
                        i += 1
                    }
                }
                num += 1
            }
        } catch {
            case e: Exception => e.printStackTrace()
        }
        import com.wangjia.bigdata.core.utils.ExMap.map2ExMap
        map.remove((k: String, v: Double) => v <= 0.001)
    }

    /**
      * 聚合新老数据
      *
      * @param uuidAndCompanys （UUID,COMPANY）List 集合
      * @param rs
      * @return
      */
    private def reduceOldLebel(uuidAndCompanys: ListBuffer[(String, Int)], rs: Array[Result]): ListBuffer[Put] = {
        val puts = new ListBuffer[Put]()
        var i = 0
        val max = uuidAndCompanys.size

        while (i < max) {
            val uuidAndCompany = uuidAndCompanys.get(i)
            val map = countSumLabel(rs(i), uuidAndCompany._1, uuidAndCompany._2)
            if (map.nonEmpty) {
                val json = new JsonObject()
                json.addProperty("time", this.logDate.getTime)
                val labelArray = new JsonArray()
                map.foreach(l => {
                    val obj = new JsonObject()
                    obj.addProperty("key", l._1)
                    obj.addProperty("value", l._2)
                    labelArray.add(obj)
                })
                json.add("labels", labelArray)

                val put = new Put(Bytes.toBytes(uuidAndCompany._1))
                put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("company#" + uuidAndCompany._2), Bytes.toBytes(json.toString))

                puts += put
            }
            i += 1
        }
        puts
    }

    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.UUID_LABEL_DES)
        HBaseUtils.createTable(HBaseTableName.UUID_LABEL_SUM)

        val sc = SparkExtend.ctx
        val lines = sc.textFile(this.inPath)
        val logBeanRdd = lines.map(BeanHandler.toWebAll).filter(_ != null)

        //按UUID和APPID分组
        val uuidAppid2BeansRdd = logBeanRdd.groupBy(x => (x.uuid, x.appid))

        //计算每个UUID和APPID分组的集合
        val uuid2LabelsRdd = uuidAppid2BeansRdd.map(x => {
            val uuid = x._1._1
            val appid = x._1._2
            val allbs = x._2.toList

            val companyId: Int = {
                val info: AppInfo = mapAppInfosBroadcast.value.getOrElse(appid, null)
                if (info != null)
                    info.companyId
                else
                    -1
            }

            //计算基础附加权重
            val dBaseWeight = countBaseWeight(uuid, companyId, allbs)
            //计算页面标签
            val pagebs = allbs.filter(_.logtype == LogType.PAGE)
            val pageLabelList = pageBeans2Labels(uuid, companyId, pagebs)
            //计算事件标签
            val eventbs = allbs.filter(_.logtype == LogType.EVENT)
            val eventLabelList = eventBeans2Labels(uuid, companyId, eventbs)
            ((uuid, companyId), dBaseWeight, pageLabelList ++ eventLabelList)
        })
                .filter(x => x._2 > 0 && x._3.nonEmpty)
                .map(x => (x._1, (x._2, x._3)))
                .reduceByKey((x1, x2) => (x1._1 + x2._1, reduceLebelDess(x1._2, x2._2)))

        //详情写入HBase
        val dayNum = JavaUtils.timeMillis2DayNum(logDate.getTime)
        uuid2LabelsRdd.foreachPartition(iterator => {
            initHBase()
            val conn: ExTBConnection = new ExTBConnection
            val tb: ExTable = conn.getTable(HBaseTableName.UUID_LABEL_DES)
            iterator.foreach(bean => {
                val uuid = bean._1._1
                val companyId = bean._1._2
                val weight = bean._2._1
                val put = new Put(Bytes.toBytes(uuid))
                val json = new JsonObject()
                json.addProperty("weight", weight)
                json.addProperty("time", logDate.getTime)
                val labelArray = new JsonArray()
                bean._2._2.foreach(label => {
                    val obj = new JsonObject()
                    obj.addProperty("name", label.labelName)
                    obj.addProperty("times", label.sumTimes)
                    obj.addProperty("stayTime", label.sumStayTime)
                    obj.addProperty("weight", label.weight)
                    labelArray.add(obj)
                })
                json.add("labels", labelArray)
                put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes(dayNum.toString + "#" + companyId), Bytes.toBytes(json.toString))
                tb.addPut(put)
            })
            conn.close()
        })

        //清除数据
        uuid2LabelsRdd.foreachPartition(iterator => {
            initHBase()
            val conn: ExTBConnection = new ExTBConnection
            val tbSum: ExTable = conn.getTable(HBaseTableName.UUID_LABEL_SUM)
            iterator.foreach(x => {
                val del = new Delete(Bytes.toBytes(x._1._1))
                del.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("company#" + x._1._2))
                tbSum.addDelete(del)
            })
            conn.close()
        })

        //计算总标签
        uuid2LabelsRdd.foreachPartition(iterator => {
            initHBase()
            val conn: ExTBConnection = new ExTBConnection
            val tbDes: ExTable = conn.getTable(HBaseTableName.UUID_LABEL_DES)
            val tbSum: ExTable = conn.getTable(HBaseTableName.UUID_LABEL_SUM)
            val uuidAndCompanys = new ListBuffer[(String, Int)]()
            val gets = new ListBuffer[Get]()
            iterator.foreach(bean => {
                uuidAndCompanys += bean._1
                gets += new Get(Bytes.toBytes(bean._1._1))
                if (uuidAndCompanys.size > 500) {
                    val rs: Array[Result] = tbDes.getTable.get(gets)
                    tbSum.addPut(reduceOldLebel(uuidAndCompanys, rs))
                    uuidAndCompanys.clear()
                    gets.clear()
                }
            })
            if (uuidAndCompanys.nonEmpty) {
                val rs: Array[Result] = tbDes.getTable.get(gets)
                tbSum.addPut(reduceOldLebel(uuidAndCompanys, rs))

                uuidAndCompanys.clear()
                gets.clear()
            }
            conn.close()
        })
    }
}

object WebJobVisitLabel {

    def main(args: Array[String]) {
        val job = new WebJobVisitLabel()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
