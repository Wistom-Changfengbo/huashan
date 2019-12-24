package com.wangjia.bigdata.core.job.mobile

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.wangjia.bean.EsAppListDocBean
import com.wangjia.bigdata.core.bean.info.{AppTagInfo, IpAddress}
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{IPUtils, JDBCBasicUtils}
import com.wangjia.common.{LogEventId, LogType}
import com.wangjia.es.{EsTableName, ExEsConnection}
import com.wangjia.hbase.conn.{ComTBConnection, ExTBConnection}
import com.wangjia.hbase.{HBaseConst, HBaseTableName}
import com.wangjia.utils.{HBaseUtils, JavaUtils}
import org.apache.commons.httpclient.HttpStatus
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Get, Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Zhoulei on 2018/5/11.
  */
class MobileJobUserImage extends EveryDaySparkJob {

    private case class AppLabel(sex: Double,
                                consumenum: Int,
                                appTag: java.util.HashMap[String, Int],
                                userTag: java.util.HashMap[String, Int])

    private case class HabitLabl(activeDayNum: Int,
                                 pageNum: Int,
                                 stayTimeSum: Long,
                                 recent7ActiveDayNum: Int,
                                 bClue: Boolean,
                                 actionTime: Int,
                                 days: ListBuffer[Int],
                                 eventids: mutable.Map[String, Int],
                                 actionTimes: mutable.Map[String, Int],
                                 ips: mutable.Map[String, Int])

    private case class UserImage(uuid: String,
                                 platform: String,
                                 deviceName: String,
                                 deviceId: String,
                                 firstTime: Long,
                                 lastTime: Long,
                                 address: IpAddress,
                                 appids: List[String],
                                 appLabel: AppLabel,
                                 appList: mutable.Map[String, String],
                                 userLabel: mutable.Map[String, Double],
                                 habitLabl: HabitLabl)

    /**
      * 加载APPTAG配置信息
      *
      * @param spark
      * @return
      */
    private def loadAppTagInfo(spark: SparkSession): (RDD[AppTagInfo], Map[Int, List[Int]], Map[Int, String]) = {
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
        val mutexInfo: Map[Int, List[Int]] = infoRdd.filter(info => info.user_tag_id != 0 && info.mutex.nonEmpty)
                .map(info => (info.user_tag_id, info.mutex, info.user_tag_weight))
                .distinct()
                .map(x => (x._1, x._2.split(',').map(_.toInt).toList, x._3))
                .collect()
                .sortWith(_._3 > _._3)
                .map(x => (x._1, x._2)).toMap

        //得ID2NAME
        val id2name: Map[Int, String] = infoRdd.filter(info => info.user_tag_id != 0)
                .map(x => (x.user_tag_id, x.user_tag_name))
                .distinct()
                .collect().toMap

        (infoRdd, mutexInfo, id2name)
    }

    /**
      * 获取((platform,pkgName),uuid)
      *
      * @param bean
      * @return
      */
    private def getUuidAndApplist(bean: MobileLogMsg): ListBuffer[((String, String), String)] = {
        val data: String = bean.data
        val uuid: String = bean.uuid
        val platform: String = bean.dStrInfo("platform", "-")
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

    /**
      * 分析APP标签
      *
      * @param beanRdd
      */
    private def analyzeAppLabel(beanRdd: RDD[MobileLogMsg], spark: SparkSession): RDD[(String, AppLabel)] = {
        val appTagInfos = loadAppTagInfo(spark)
        //加载APP标签信息
        val infoRdd: RDD[AppTagInfo] = appTagInfos._1
        //得到互斥关系
        val mutexInfo: Map[Int, List[Int]] = appTagInfos._2
        //得ID2NAME
        val id2name: Map[Int, String] = appTagInfos._3

        //((platform,pkgName),uuid)
        val appRdd: RDD[((String, String), String)] = beanRdd.filter(x => x.logtype == LogType.EVENT && x.subtype == LogEventId.APP_APPLIST)
                .map(x => (x.uuid, x))
                .reduceByKey((x1, _) => x1)
                .flatMap(x => getUuidAndApplist(x._2))

        val mapInfoRdd: RDD[((String, String), AppTagInfo)] = infoRdd.map(info => ((info.platform, info.pkg_name), info))
        val appLabel: RDD[(String, AppLabel)] = appRdd.join(mapInfoRdd)
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
                            val httpClient = HttpClientBuilder.create().build()
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
                    (uuid, AppLabel(sex, consumenum, appTag, userTag))
                }
        appLabel
    }

    private def getUserLabel(rsLabel: Result): mutable.Map[String, Double] = {
        var i: Int = 0
        var max: Int = 0
        val map = mutable.Map[String, Double]()
        if (rsLabel.isEmpty)
            return map
        rsLabel.listCells().foreach(cell => {
            try {
                val str = Bytes.toString(CellUtil.cloneValue(cell))
                val obj = JSON.parseObject(str)
                val jArray = obj.getJSONArray("labels")
                max = jArray.size()
                i = 0
                while (i < max) {
                    val o = jArray.getJSONObject(i)
                    val key = o.getString("key")
                    val value: Double = o.getDoubleValue("value") + map.getOrDefault(key, 0f)
                    map.put(key, value)
                    i += 1
                }
            } catch {
                case e: Exception => e.printStackTrace()
            }
        })
        map
    }

    private def getDeviceMsg(rsDevice: Result): (Long, Long, ListBuffer[String]) = {
        if (rsDevice.isEmpty)
            return null
        val firstTime: Long = Bytes.toLong(rsDevice.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_FIRSTTIME))
        val lastTime: Long = Bytes.toLong(rsDevice.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_LASTTIME))

        val appids = new ListBuffer[String]
        rsDevice.listCells().foreach(cell => {
            val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
            if (qualifier.startsWith("_appid#"))
                appids += qualifier.substring(7)
        })
        (firstTime, lastTime, appids)
    }

    private def getHabitMsg(rsHabit: Result): HabitLabl = {
        if (rsHabit.isEmpty)
            return null
        var activeDayNum: Int = 0
        var pageNum: Int = 0
        var stayTimeSum: Long = 0
        var recent7ActiveDayNum: Int = 0
        var bClue: Boolean = false
        var days: ListBuffer[Int] = new ListBuffer[Int]
        val eventids: mutable.Map[String, Int] = mutable.Map[String, Int]()
        val actionTimes: mutable.Map[String, Int] = mutable.Map[String, Int]()
        val ips: mutable.Map[String, Int] = mutable.Map[String, Int]()

        rsHabit.listCells().foreach(cell => {
            try {
                val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
                if (qualifier.endsWith("msg")) {
                    val qs = qualifier.split(':')
                    if (!qs(1).equals("habit")) {
                        days += qs(1).toInt
                    } else {
                        val str = Bytes.toString(CellUtil.cloneValue(cell))
                        val json = JSON.parseObject(str)
                        val jTimes = json.getJSONObject("times")
                        jTimes.keySet().foreach(key => {
                            actionTimes.put(key, actionTimes.getOrElse(key, 0) + jTimes.getIntValue(key))
                        })
                        val appid = json.getString("appid")
                        val jEventids = json.getJSONObject("events")
                        jEventids.keySet().foreach(key => {
                            val _key = s"""$appid#$key"""
                            eventids.put(_key, eventids.getOrElse(_key, 0) + jEventids.getIntValue(key))
                        })
                        val jIps = json.getJSONObject("ips")
                        jIps.keySet().foreach(key => {
                            ips.put(key, ips.getOrElse(key, 0) + jIps.getIntValue(key))
                        })
                    }
                }
                else if (qualifier.endsWith("bClue")) {
                    bClue = bClue | Bytes.toBoolean(CellUtil.cloneValue(cell))
                } else if (qualifier.endsWith("stayTime")) {
                    stayTimeSum += Bytes.toLong(CellUtil.cloneValue(cell))
                } else if (qualifier.endsWith("page")) {
                    pageNum += Bytes.toInt(CellUtil.cloneValue(cell))
                }
            }
            catch {
                case e: Exception => e.printStackTrace()
            }
        })

        days = days.distinct.sortWith(_ > _)
        activeDayNum = days.size

        val newDay = JavaUtils.timeMillis2DayNum(System.currentTimeMillis())
        var i = 1
        while (i < 8) {
            if (days.contains(newDay - i))
                recent7ActiveDayNum += 1
            i += 1
        }

        val actionTime: Int = actionTimes.toList.sortWith(_._2 > _._2).head._1.toInt

        HabitLabl(activeDayNum,
            pageNum,
            stayTimeSum,
            recent7ActiveDayNum,
            bClue,
            actionTime,
            days,
            eventids,
            actionTimes,
            ips)
    }

    private def analyzeUserImage(bean: MobileLogMsg,
                                 appLabel: AppLabel,
                                 rsDevice: Result,
                                 rsHabit: Result,
                                 rsLabel: Result,
                                 rsAppList: Result): UserImage = {
        try {
            val uuid: String = bean.uuid
            val platform: String = bean.dStrInfo("platform", "-")
            val deviceName: String = bean.dStrInfo("model", "-")
            val deviceId: String = bean.dStrInfo("deviceid", "-")

            val deviceMsg = getDeviceMsg(rsDevice)
            if (deviceMsg == null)
                return null

            val habitLabl = getHabitMsg(rsHabit)
            if (habitLabl == null)
                return null

            val userLabel: mutable.Map[String, Double] = getUserLabel(rsLabel)

            val maxIp: String = habitLabl.ips.toList.sortWith(_._2 > _._2).head._1
            val address: IpAddress = IPUtils.getAddress(maxIp)
            val firstTime: Long = deviceMsg._1
            val lastTime: Long = deviceMsg._2
            val appids: List[String] = deviceMsg._3.toList

            val appList = mutable.Map[String, String]()
            if (rsAppList.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG)) {
                val str: String = Bytes.toString(rsAppList.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG))
                val josn: JSONObject = JSON.parseObject(str)
                josn.entrySet().foreach(item => {
                    appList.put(item.getKey, item.getValue.asInstanceOf[String])
                })
            }

            UserImage(uuid,
                platform,
                deviceName,
                deviceId,
                firstTime,
                lastTime,
                address,
                appids,
                appLabel,
                appList,
                userLabel,
                habitLabl)
        } catch {
            case e: Exception => e.printStackTrace(); null
        }
    }

    override def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.UUID_USER_IMAGE)
        val sc = SparkExtend.ctx
        val spark = SparkSession.builder().appName(this.jobName).getOrCreate()

        val beanRdd: RDD[MobileLogMsg] = sc.textFile(this.inPath)
                .map(BeanHandler.toAppAcc)
                .filter(_ != null)

        val appLabelRdd: RDD[(String, AppLabel)] = analyzeAppLabel(beanRdd, spark)
        //        val appLabelRdd: RDD[(String, AppLabel)] = sc.makeRDD(new ListBuffer[(String, AppLabel)])
        val userImageRdd: RDD[UserImage] = beanRdd.groupBy(_.uuid)
                .leftOuterJoin(appLabelRdd)
                .mapPartitions(iterator => {
                    initHBase()
                    val list = new ListBuffer[UserImage]
                    val conn = new ComTBConnection
                    val tbDevice = conn.getTable(HBaseTableName.UUID_DEVICE_MSG)
                    val tbHabit = conn.getTable(HBaseTableName.UUID_HABIT)
                    val tbLabelSum = conn.getTable(HBaseTableName.UUID_LABEL_SUM)
                    val tbAppList = conn.getTable(HBaseTableName.UUID_APPLIST)
                    iterator.foreach(item => {
                        val bean = item._2._1.toList.head
                        val appLabel = item._2._2.getOrElse(null)
                        val get = new Get(Bytes.toBytes(bean.uuid))
                        val rsDevice = tbDevice.get(get)
                        val rsHabit = tbHabit.get(get)
                        val rsLabel = tbLabelSum.get(get)
                        val rsAppList = tbAppList.get(get)
                        val userImage = analyzeUserImage(bean, appLabel, rsDevice, rsHabit, rsLabel, rsAppList)
                        if (userImage != null)
                            list += userImage
                    })
                    conn.close()
                    list.toIterator
                })

        userImageRdd.foreachPartition(iterator => {
            initHBase()
            val conn = new ExTBConnection
            val tb = conn.getTable(HBaseTableName.UUID_USER_IMAGE)
            val esConn: ExEsConnection = new ExEsConnection()
            iterator.foreach(userImage => {
                val put = new Put(Bytes.toBytes(userImage.uuid))
                val json = new JSONObject()
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_PLATFORM, Bytes.toBytes(userImage.platform))
                json.put("platform", userImage.platform)
                put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("devicename"), Bytes.toBytes(userImage.deviceName))
                json.put("devicename", userImage.deviceName)
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_DEVICEID, Bytes.toBytes(userImage.deviceId))
                json.put("deviceid", userImage.deviceId)
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_FIRSTTIME, Bytes.toBytes(userImage.firstTime))
                json.put("firsttime", userImage.firstTime)
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_LASTTIME, Bytes.toBytes(userImage.lastTime))
                json.put("lasttime", userImage.lastTime)
                if (userImage.address != null) {
                    val jAddress = new JSONArray()
                    jAddress.add(userImage.address.country)
                    jAddress.add(userImage.address.province)
                    jAddress.add(userImage.address.city)
                    jAddress.add(userImage.address.area)
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("address"), Bytes.toBytes(jAddress.toJSONString))
                    json.put("address", jAddress)
                }
                if (userImage.appids != null && userImage.appids.nonEmpty) {
                    val jAppids = new JSONArray()
                    userImage.appids.foreach(x => jAppids.add(x))
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("appids"), Bytes.toBytes(jAppids.toJSONString))
                    json.put("appids", jAppids)
                }

                val appLabel = userImage.appLabel
                if (appLabel != null) {
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("sex"), Bytes.toBytes(appLabel.sex))
                    json.put("sex", appLabel.sex)
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("consumenum"), Bytes.toBytes(appLabel.consumenum))
                    json.put("consumenum", appLabel.consumenum)

                    val jApptagkey = new JSONArray()
                    val jApptagvalue = new JSONArray()
                    appLabel.appTag.toList.sortWith(_._2 > _._2).foreach(x => {
                        jApptagkey.add(x._1)
                        jApptagvalue.add(x._2)
                    })
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("apptagkey"), Bytes.toBytes(jApptagkey.toJSONString))
                    json.put("apptagkey", jApptagkey)
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("apptagvalue"), Bytes.toBytes(jApptagvalue.toJSONString))
                    json.put("apptagvalue", jApptagvalue)

                    val jUsertagkey = new JSONArray()
                    val jUsertagvalue = new JSONArray()
                    appLabel.userTag.toList.sortWith(_._2 > _._2).foreach(x => {
                        jUsertagkey.add(x._1)
                        jUsertagvalue.add(x._2)
                    })
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("usertagkey"), Bytes.toBytes(jUsertagkey.toJSONString))
                    json.put("usertagkey", jUsertagkey)
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("usertagvalue"), Bytes.toBytes(jUsertagvalue.toJSONString))
                    json.put("usertagvalue", jUsertagvalue)
                }

                val appList = userImage.appList
                if (appList != null && appList.nonEmpty) {
                    val jAppPkgList = new JSONArray()
                    val jAppNameList = new JSONArray()
                    appList.foreach(x => {
                        jAppPkgList.add(x._1)
                        jAppNameList.add(x._2)
                    })
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("apppkglist"), Bytes.toBytes(jAppPkgList.toJSONString))
                    json.put("apppkglist", jAppPkgList)
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("appnamelist"), Bytes.toBytes(jAppNameList.toJSONString))
                    json.put("appnamelist", jAppNameList)
                }

                val userLabel = userImage.userLabel
                if (userLabel != null && userLabel.nonEmpty) {
                    val jUserLabelkey = new JSONArray()
                    val jUserLabelvalue = new JSONArray()
                    userLabel.toList.sortWith(_._2 > _._2).foreach(x => {
                        jUserLabelkey.add(x._1)
                        jUserLabelvalue.add(x._2)
                    })
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("userlabelkey"), Bytes.toBytes(jUserLabelkey.toJSONString))
                    json.put("userlabelkey", jUserLabelkey)
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("userlabelvalue"), Bytes.toBytes(jUserLabelvalue.toJSONString))
                    json.put("userlabelvalue", jUserLabelvalue)

                    if (userLabel.contains("_sf_liveness")) {
                        val liveness = userLabel.getOrDefault("_sf_liveness", 0)
                        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("liveness"), Bytes.toBytes(liveness))
                        json.put("liveness", liveness)
                    }
                }

                val habitLabl = userImage.habitLabl
                if (habitLabl != null) {
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("activedaynum"), Bytes.toBytes(habitLabl.activeDayNum))
                    json.put("activedaynum", habitLabl.activeDayNum)
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("pagenum"), Bytes.toBytes(habitLabl.pageNum))
                    json.put("pagenum", habitLabl.pageNum)
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("staytimesum"), Bytes.toBytes(habitLabl.stayTimeSum))
                  json.put("staytimesum", habitLabl.stayTimeSum)
                  put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("r7activedaynum"), Bytes.toBytes(habitLabl.recent7ActiveDayNum))
                  json.put("r7activedaynum", habitLabl.recent7ActiveDayNum)
                  put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("bclue"), Bytes.toBytes(habitLabl.bClue))
                    json.put("bclue", habitLabl.bClue)
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("actiontime"), Bytes.toBytes(habitLabl.actionTime))
                    json.put("actiontime", habitLabl.actionTime)

                    val jDays = new JSONArray()
                    habitLabl.days.sortWith(_ > _).foreach(x => jDays.add(x))
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("days"), Bytes.toBytes(jDays.toJSONString))
                    json.put("days", jDays)

                    val jEventidkey = new JSONArray()
                    val jEventidvalue = new JSONArray()
                    habitLabl.eventids.toList.sortWith(_._2 > _._2).foreach(x => {
                        jEventidkey.add(x._1)
                        jEventidvalue.add(x._2)
                    })
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("eventidkey"), Bytes.toBytes(jEventidkey.toJSONString))
                    json.put("eventidkey", jEventidkey)
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("eventidvalue"), Bytes.toBytes(jEventidvalue.toJSONString))
                    json.put("eventidvalue", jEventidvalue)

                    val jIps = new JSONArray()
                    var i: Int = 0
                    val iter = habitLabl.ips.toList.sortWith(_._2 > _._2).toIterator
                    while (iter.hasNext && i < 10) {
                        jIps.add(iter.next()._1)
                        i += 1
                    }
                    put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("ips"), Bytes.toBytes(jIps.toJSONString))
                    json.put("ips", jIps)
                }
                tb.addPut(put)
                esConn.add(EsTableName.ES_INDEX_BIGDATA_USER_IMAGE,
                    EsTableName.ES_TYPE_USER_IMAGE,
                    userImage.uuid,
                    json.toJSONString)
            })
            conn.close()
            esConn.close()
        })
    }
}

object MobileJobUserImage {
    def main(args: Array[String]) {
        val job = new MobileJobUserImage()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
