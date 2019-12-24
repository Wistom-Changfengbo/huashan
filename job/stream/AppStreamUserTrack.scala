package com.wangjia.bigdata.core.job.stream

import com.wangjia.bean.DeviceDes
import com.wangjia.bigdata.core.bean.common.ExUserVisit
import com.wangjia.bigdata.core.bean.info._
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.{BeanHandler, _}
import com.wangjia.bigdata.core.job.common.{ConfigMobileLabel, JobStreamUserTrack}
import com.wangjia.bigdata.core.utils.JAInfoUtils
import com.wangjia.builder.DeviceDesBuidler
import com.wangjia.common.{LogEventId, LogType}
import com.wangjia.es.{EsTableName, ExEsConnection}
import com.wangjia.hbase.HBaseTableName
import com.wangjia.hbase.conn.{ExTBConnection, ExTable}
import com.wangjia.utils.{JavaUtils, LabelUtils, RedisUtils}
import org.apache.hadoop.hbase.client.{Get, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * APP端实时轨迹、实时标签、实时线索
  *
  * Created by Administrator on 2018/3/5.
  */
class AppStreamUserTrack extends JobStreamUserTrack {
    //一个批次时间(S)
    BATCH_DURATION_SECONDS = 60L

    //加载广播应用信息
    private val mapAppInfos = new LoadDataHandler[mutable.Map[String, AppInfo]](Config.UPDATE_INFO_INTERVAL_TIME, JAInfoUtils.loadAppInfos _)

    //加载应用信息
    private val mapIdAppInfos = new LoadDataHandler[mutable.Map[String, AppInfo]](Config.UPDATE_INFO_INTERVAL_TIME, () => {
        val _mapAppInfos = JAInfoUtils.loadAppInfos
        val _mapIdAppInfos = mutable.Map[String, AppInfo]()
        _mapAppInfos.foreach(i => {
            if (i._2.identifier != "")
                _mapIdAppInfos.put(i._2.identifier, i._2)
        })
        _mapIdAppInfos
    })

    //加载广播帐号信息
    private val mapAccountInfos = new LoadDataHandler[mutable.Map[Int, AccountInfo]](Config.UPDATE_INFO_INTERVAL_TIME, JAInfoUtils.loadAccountInfos _)

    //加载页面规则
    private val pageRuleInfos = new LoadDataHandler[mutable.Map[String, Array[PageInfo]]](Config.UPDATE_INFO_INTERVAL_TIME, () => {
        val _mapAppInfos = JAInfoUtils.loadAppInfos
        JAInfoUtils.loadPageInfoByAppInfos(_mapAppInfos)
    })

    //来源地址映射
    private val sourceInfos = new LoadDataHandler[Array[StrRule]](Config.UPDATE_INFO_INTERVAL_TIME, () => {
        JAInfoUtils.loadSourceInfos.asInstanceOf[Array[StrRule]]
    })

    //用户标签配置信息
    private val userLabelInfos = new LoadDataHandler[mutable.Map[Int, ListBuffer[UserLabel]]](Config.UPDATE_INFO_INTERVAL_TIME, JAInfoUtils.loadUserLabelInfo _)

    //更新新老数据
    private val updateFunc = (iter: Iterator[((String, String), Seq[MobileLogMsg], Option[ListBuffer[ExUserVisit]])]) => {
        val list = mutable.ListBuffer[((String, String), ListBuffer[ExUserVisit])]()
        iter.foreach(b => list += ((b._1, reduceFunc(b._2, b._3))))
        list.toIterator
    }

    //聚合新老数据
    private def reduceFunc(lnew: Seq[MobileLogMsg], of: Option[ListBuffer[ExUserVisit]]): ListBuffer[ExUserVisit] = {
        val list = of.getOrElse(ListBuffer[ExUserVisit]())
        list.foreach(_.bChange = false)
        val beans = lnew.sortWith(_.time < _.time)
        val timeMillis: Long = System.currentTimeMillis()

        //构建访问轨迹
        var i: Int = 0
        var visit: ExUserVisit = null
        val max = beans.size
        while (i < max) {
            val b = beans(i)
            if (visit == null) {
                if (list.nonEmpty)
                    visit = list.last
                else {
                    visit = ExUserVisit(b, timeMillis)
                    list += visit
                }
            }
            //超过一次访问间隔
            if (b.time - visit.lastTime > VISIT_TIME_INTERVAL) {
                visit = ExUserVisit(b, timeMillis)
                list += visit
            }
            visit.addTrack(b, timeMillis, pageRuleInfos)
            i += 1
        }

        //去掉过时数据
        if (list.nonEmpty) {
            val lastVisit = list.last
            if (!lastVisit.bChange && timeMillis - lastVisit.updateTime > VISIT_TIME_INTERVAL) {
                list.clear()
            }
        }
        i = 0
        while (i < list.size - 1) {
            val visit = list(i)
            if (!visit.bChange) {
                list.remove(i)
                i -= 1
            }
            i += 1
        }

        list
    }

    //分析保存用户线索
    private def analyzeUserClue(ds: DStream[MobileLogMsg]): Unit = {
        ds.filter(x => x.logtype == LogType.EVENT && x.subtype == LogEventId.USER_COMMIT_CLUE)
                .map(x => BeanHandler.toUserClue(x))
                .filter(_ != null)
                .foreachRDD(saveUserClueRDD _)
    }

    //分析用户标签
    private def analyzeUserLabel(ds: DStream[((String, String), MobileLogMsg)]): Unit = {
        val beanGroupDs: DStream[((String, String), Iterable[MobileLogMsg])] = ds.groupByKey()

        val uuid2Labels = beanGroupDs.flatMap(it => {
            val appid = it._1._1
            val uuid = it._1._2
            val companyId: Int = {
                val info: AppInfo = mapAppInfos.value().getOrElse(appid, null)
                if (info != null)
                    info.companyId
                else
                    -1
            }

            val labelInfos = userLabelInfos.value().getOrElse(companyId, null)
            val map = mutable.Map[String, Float]()

            if (labelInfos != null) {
                val beans = it._2.toList
                var i = 0
                var max = 0

                //计算页面标签
                val pagebs = beans.filter(_.logtype == LogType.PAGE)
                        .sortWith(_.time < _.time)
                i = 0
                max = pagebs.size

                //计算时间差
                val countDt = {
                    if (beans.head.ja_version.startsWith("2.")) {
                        val _countDtV2 = (b: MobileLogMsg) => {
                            var _dt: Long = 0
                            if (i > 0)
                                _dt = b.time - pagebs(i - 1).time
                            if (_dt <= 0 || _dt > VISIT_TIME_INTERVAL)
                                _dt = ConfigMobileLabel.PAGE_DEFAULT_TIME
                            _dt
                        }
                        _countDtV2
                    } else {
                        val _countDtVDef = (b: MobileLogMsg) => {
                            var _dt: Long = 0
                            if (i + 1 < max)
                                _dt = pagebs(i + 1).time - b.time
                            if (_dt <= 0 || _dt > VISIT_TIME_INTERVAL)
                                _dt = ConfigMobileLabel.PAGE_DEFAULT_TIME
                            _dt
                        }
                        _countDtVDef
                    }
                }
                while (i < max) {
                    val b = pagebs(i)
                    val dt: Long = countDt(b)
                    val lvalue = LabelUtils.getMobileLabelValue(1, dt, ConfigMobileLabel.EVENT_PAGE_WEIGHT, 0F)
                    val title = b.title
                    if (title != "") {
                        labelInfos.foreach(x => {
                            if (x.isMeet(title)) {
                                val ct: Float = map.getOrElse(x.name, 0F)
                                map.put(x.name, ct + lvalue)
                            }
                        })
                    }
                    //用户活跃度
                    val ct: Float = map.getOrElse(ConfigMobileLabel.SELF_DEFINITION_LABEL_LIVENESS, 0F)
                    map.put(ConfigMobileLabel.SELF_DEFINITION_LABEL_LIVENESS, ct + lvalue)
                    i += 1
                }

                //计算事件标签
                val eventbs = beans.filter(_.logtype == LogType.EVENT)
                i = 0
                max = eventbs.size
                while (i < max) {
                    val b = eventbs(i)
                    val lvalue = LabelUtils.getMobileLabelValue(1, ConfigMobileLabel.PAGE_DEFAULT_TIME, ConfigMobileLabel.EVENT_IMPORTANCE_SELF_WEIGHT, 0F)
                    val subType = b.subtype
                    val title = b.title
                    if (subType == "search_case" || subType == "search_photo") {
                        if (title != "") {
                            val key = "_kw_" + title
                            val ct: Float = map.getOrElse(key, 0F)
                            map.put(key, ct + lvalue)
                        }
                        //用户活跃度
                        val ct: Float = map.getOrElse(ConfigMobileLabel.SELF_DEFINITION_LABEL_LIVENESS, 0F)
                        map.put(ConfigMobileLabel.SELF_DEFINITION_LABEL_LIVENESS, ct + lvalue)
                    }
                    //案例收藏 案例分享 图片分享 图片收藏 图片保存
                    else if (subType == "collection_case"
                            || subType == "share_case"
                            || subType == "share_photo"
                            || subType == "collection_photo"
                            || subType == "save_photo") {
                        if (title != "") {
                            labelInfos.foreach(x => {
                                if (x.isMeet(title)) {
                                    val ct: Float = map.getOrElse(x.name, 0F)
                                    map.put(x.name, ct + lvalue)
                                }
                            })
                        }
                        //用户活跃度
                        val ct: Float = map.getOrElse(ConfigMobileLabel.SELF_DEFINITION_LABEL_LIVENESS, 0F)
                        map.put(ConfigMobileLabel.SELF_DEFINITION_LABEL_LIVENESS, ct + lvalue)
                    }
                    i += 1
                }
            }
            map.map(b => ((uuid, companyId, b._1), b._2))
        }).reduceByKey(_ + _)

        uuid2Labels.foreachRDD(_.foreachPartition(iterator => {
            val conn: Jedis = RedisUtils.getPConn(Config.REDIS_URL, Config.REDIS_PORT, Config.REDIS_DBINDEX)
            val time = System.currentTimeMillis()
            val dayStr = JavaUtils.timeMillis2DayNum(time)
            val keyHead = "LABELTEMP:" + dayStr + ":"
            while (iterator.hasNext) {
                val b = iterator.next()
                //(UUID,companyId,labelName),Value
                conn.hincrByFloat(keyHead + b._1._2 + ":" + b._1._1, b._1._3, b._2)
            }
            RedisUtils.closeConn(conn)
        }))
    }

    //分析设备信息
    private def analyzeDeviceDes(ds: DStream[MobileLogMsg]): Unit = {
        val uuid2logBeansDS: DStream[(String, Iterable[MobileLogMsg])] = ds.map(x => (x.uuid, x)).groupByKey()
        val uuid2DeviceDesDS: DStream[(String, DeviceDes)] = uuid2logBeansDS.map(_._1).mapPartitions(iterator => {
            initHBase()
            val map = mutable.Map[String, DeviceDes]()
            val uuids = new ListBuffer[String]
            val gets = new ListBuffer[Get]

            val conn = new ExTBConnection
            val tb: Table = conn.getTable(HBaseTableName.UUID_DEVICE_MSG).getTable

            iterator.foreach(uuid => {
                uuids += uuid
                gets += new Get(Bytes.toBytes(uuid))
                if (uuids.size > Config.BATCH_SIZE) {
                    val results: Array[Result] = tb.get(gets)
                    var i = 0
                    val max = results.length
                    while (i < max) {
                        map += (uuids(i) -> DeviceDesBuidler.build(uuids(i), results(i)))
                        i += 1
                    }
                    uuids.clear()
                    gets.clear()
                }
            })

            if (uuids.nonEmpty) {
                val results: Array[Result] = tb.get(gets)
                var i = 0
                val max = results.length
                while (i < max) {
                    map += (uuids(i) -> DeviceDesBuidler.build(uuids(i), results(i)))
                    i += 1
                }
                uuids.clear()
                gets.clear()
            }

            conn.close()
            map.toIterator
        })
        val uuid2newDesDS = uuid2DeviceDesDS.join(uuid2logBeansDS).map(bean => {
            val uuid = bean._1
            val des = bean._2._1
            val logMsgs = bean._2._2.toList
            val newDes = DeviceDesHandler.updateAppDeviceDes(uuid, des, logMsgs, mapAppInfos)
            (uuid, newDes)
        })
        uuid2newDesDS.foreachRDD(_.foreachPartition(iterator => {
            initHBase()
            val tbConn = new ExTBConnection
            val esConn = new ExEsConnection
            val tb: ExTable = tbConn.getTable(HBaseTableName.UUID_DEVICE_MSG)
            iterator.foreach(b => {
                tb.addPut(DeviceDesBuidler.deviceDes2Put(b._2))
                esConn.add(EsTableName.ES_INDEX_BIGDATA_DEVICE, EsTableName.ES_TYPE_DEVICE, b._1, DeviceDesBuidler.deviceDes2EsDoc(b._2))
            })
            tbConn.close()
            esConn.close()
        }))

    }

    private def filterBean(bean: MobileLogMsg): Boolean = {
        if (bean == null)
            return false
        val timeMillis = System.currentTimeMillis()
        if (bean.time > timeMillis)
            return false
        if (!JavaUtils.isSameDate(bean.time, timeMillis))
            return false
        //        if (!bean.deviceId.contains("864368033676681"))
        //            return false
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
        val kafkaParams = Map[String, Object](
            "session.timeout.ms" -> "70000",
            "request.timeout.ms" -> "80000",
            "group.id" -> "wj_app_user_track_20180611"
        )

        val topics = Array("app20180611")

        val lines = readLineByKafka(ssc, topics, kafkaParams)
        val beanDs: DStream[MobileLogMsg] = lines.map(x => LogClearHandler.clearMobileLog(x, mapAppInfos, mapIdAppInfos, mapAccountInfos))
                .filter(_._1 == LogClearStatus.SUCCESS)
                .flatMap(_._2.asInstanceOf[ListBuffer[MobileLogMsg]])
                .filter(filterBean)

        //保存时间
        beanDs.map(x => (x.appid, x.time))
                .reduceByKey((t1, t2) => math.max(t1, t2))
                .foreachRDD(saveTime _)

        //分析线索
        analyzeUserClue(beanDs)

        val rightIdBeanRdd = beanDs.filter(x => x.uuid != null && x.uuid.length == 32)
        //分析设备信息
        analyzeDeviceDes(rightIdBeanRdd)

        val logBeanDS: DStream[((String, String), MobileLogMsg)] = rightIdBeanRdd.map(x => ((x.appid, x.uuid), x))
        //分析标签
        analyzeUserLabel(logBeanDS)

        val results: DStream[((String, String), ListBuffer[ExUserVisit])] = logBeanDS.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
        //保存数据
        results.foreachRDD(saveVisitRdd(sourceInfos) _)
    }
}

object AppStreamUserTrack {
    def main(args: Array[String]) {
        val job = new AppStreamUserTrack
        job.run(args)
    }
}