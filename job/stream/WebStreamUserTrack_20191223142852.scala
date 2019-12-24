package com.wangjia.bigdata.core.job.stream

import com.alibaba.fastjson.JSON
import com.wangjia.bean.DeviceDes
import com.wangjia.bigdata.core.bean.common.ExUserVisit
import com.wangjia.bigdata.core.bean.info._
import com.wangjia.bigdata.core.bean.web.WebLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.{BeanHandler, _}
import com.wangjia.bigdata.core.job.common.{ConfigWebLabel, JobStreamUserTrack}
import com.wangjia.bigdata.core.utils.JAInfoUtils
import com.wangjia.builder.DeviceDesBuidler
import com.wangjia.common.{LogEventId, LogType}
import com.wangjia.es.{EsTableName, ExEsConnection}
import com.wangjia.hbase.HBaseTableName
import com.wangjia.hbase.conn.{ExTBConnection, ExTable}
import com.wangjia.utils._
import org.apache.hadoop.hbase.client.{Get, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * WEB端实时轨迹、实时标签、实时线索
  *
  * Created by Administrator on 2017/7/5.
  */
class WebStreamUserTrack extends JobStreamUserTrack {
    //一个批次时间(S)
    BATCH_DURATION_SECONDS = 60L

    //来源地址映射
    private val sourceInfos = new LoadDataHandler[Array[StrRule]](Config.UPDATE_INFO_INTERVAL_TIME, () => JAInfoUtils.loadSourceInfos.asInstanceOf[Array[StrRule]])

    //加载广播帐号信息
    private val mapAccountInfos = new LoadDataHandler[mutable.Map[Int, AccountInfo]](Config.UPDATE_INFO_INTERVAL_TIME, JAInfoUtils.loadAccountInfos _)

    //加载广播应用信息
    private val mapAppInfos = new LoadDataHandler[mutable.Map[String, AppInfo]](Config.UPDATE_INFO_INTERVAL_TIME, JAInfoUtils.loadAppInfos _)

    //用户标签配置信息
    private val userLabelInfos = new LoadDataHandler[mutable.Map[Int, ListBuffer[UserLabel]]](Config.UPDATE_INFO_INTERVAL_TIME, JAInfoUtils.loadUserLabelInfo _)

    //聚合新老数据
    private def reduceFunc(lnew: Seq[WebLogMsg], of: Option[ListBuffer[ExUserVisit]]): ListBuffer[ExUserVisit] = {
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
            visit.addTrack(b, timeMillis)
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

    //更新新老数据
    private val updateFunc = (iter: Iterator[((String, String), Seq[WebLogMsg], Option[ListBuffer[ExUserVisit]])]) => {
        val list = mutable.ListBuffer[((String, String), ListBuffer[ExUserVisit])]()
        iter.foreach(b => list += ((b._1, reduceFunc(b._2, b._3))))
        list.toIterator
    }

    //分析保存用户线索
    private def analyzeUserClue(ds: DStream[WebLogMsg]): Unit = {
        ds.filter(x => x.logtype == LogType.EVENT && x.subtype == LogEventId.USER_COMMIT_CLUE)
                .map(BeanHandler.toUserClue)
                .filter(_ != null)
                .foreachRDD(saveUserClueRDD _)
    }

    //分析用户标签
    private def analyzeUserLabel(ds: DStream[((String, String), WebLogMsg)]): Unit = {
        val beanGroupDs: DStream[((String, String), Iterable[WebLogMsg])] = ds.groupByKey()
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
                val beans: List[WebLogMsg] = it._2.toList
                var i: Int = 0
                var max: Int = 0

                //计算页面标签
                val pageBs = beans.filter(_.logtype == LogType.PAGE)
                        .sortWith(_.time < _.time)
                i = 0
                max = pageBs.size
                while (i < max) {
                    val b = pageBs(i)
                    val dt: Long = {
                        var _dt: Long = 0
                        if (i + 1 < max)
                            _dt = beans(i + 1).time - b.time
                        if (_dt <= 0 || _dt > VISIT_TIME_INTERVAL)
                            _dt = ConfigWebLabel.PAGE_DEFAULT_TIME
                        _dt
                    }
                    val title = b.title
                    val lvalue = LabelUtils.getWebLabelValue(1, dt, ConfigWebLabel.EVENT_PAGE_WEIGHT, 0F)
                    labelInfos.foreach(x => {
                        if (x.isMeet(title)) {
                            val ct: Float = map.getOrElse(x.name, 0F)
                            map.put(x.name, ct + lvalue)
                        }
                    })
                    //用户活跃度
                    val ct: Float = map.getOrElse(ConfigWebLabel.SELF_DEFINITION_LABEL_LIVENESS, 0F)
                    map.put(ConfigWebLabel.SELF_DEFINITION_LABEL_LIVENESS, ct + lvalue)
                    i += 1
                }

                //计算事件标签
                val events = beans.filter(_.logtype == LogType.EVENT)
                i = 0
                max = events.size
                while (i < max) {
                    val b = events(i)
                    val lvalue = LabelUtils.getWebLabelValue(1, ConfigWebLabel.PAGE_DEFAULT_TIME, ConfigWebLabel.EVENT_IMPORTANCE_SELF_WEIGHT, 0F)
                    //收藏
                    if (b.subtype == "favorite") {
                        val title = b.title
                        labelInfos.foreach(x => {
                            if (x.isMeet(title)) {
                                val ct: Float = map.getOrElse(x.name, 0F)
                                map.put(x.name, ct + lvalue)
                            }
                        })
                        //用户活跃度
                        val ct: Float = map.getOrElse(ConfigWebLabel.SELF_DEFINITION_LABEL_LIVENESS, 0F)
                        map.put(ConfigWebLabel.SELF_DEFINITION_LABEL_LIVENESS, ct + lvalue)
                        //用户搜索
                    } else if (b.subtype == "user_search") {
                        try {
                            val jsonObj = JSON.parseObject(b.data)
                            val _type = jsonObj.getString("type")
                            val _kw = jsonObj.getString("keywords")
                            if (_type.length > 0 && _kw.length > 0) {
                                val key = "_kw_" + _type + "_" + _kw
                                val ct: Float = map.getOrElse(key, 0F)
                                map.put(key, ct + lvalue)
                            }
                            //用户活跃度
                            val ct: Float = map.getOrElse(ConfigWebLabel.SELF_DEFINITION_LABEL_LIVENESS, 0F)
                            map.put(ConfigWebLabel.SELF_DEFINITION_LABEL_LIVENESS, ct + lvalue)
                        } catch {
                            case e: Exception => e.printStackTrace();
                        }
                    }
                    i += 1
                }
            }

            map.map(l => ((uuid, companyId, l._1), l._2))
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
    private def analyzeDeviceDes(ds: DStream[WebLogMsg]): Unit = {
        val uuid2logBeansDS: DStream[(String, Iterable[WebLogMsg])] = ds.map(x => (x.uuid, x)).groupByKey()
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
            val newDes = DeviceDesHandler.updateWebDeviceDes(uuid, des, logMsgs, mapAppInfos)
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

    //过滤日志
    private def filterBean(bean: WebLogMsg): Boolean = {
        if (bean == null)
            return false
        val timeMillis = System.currentTimeMillis()
        if (bean.time > timeMillis)
            return false
        if (!JavaUtils.isSameDate(bean.time, timeMillis))
            return false
        true
    }

    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.UUID_VISIT)
        HBaseUtils.createTable(HBaseTableName.UUID_VISIT_DES)

        val kafkaParams = Map[String, Object](
            "session.timeout.ms" -> "40000",
            "request.timeout.ms" -> "50000",
            "group.id" -> "wj_web_user_track_20180611"
        )

        val topics = Array("web20180611")
        val lines = readLineByKafka(ssc, topics, kafkaParams)

        //简析Json日志
        val beanDS: DStream[WebLogMsg] = lines.map(str => LogClearHandler.clearWebLog(str, mapAppInfos, mapAccountInfos))
                .filter(_._1 == LogClearStatus.SUCCESS)
                .map(_._2.asInstanceOf[WebLogMsg])
                .filter(filterBean)

        //保存时间
        beanDS.map(x => (x.appid, x.time))
                .reduceByKey((t1, t2) => math.max(t1, t2))
                .foreachRDD(saveTime _)

        //分析线索
        analyzeUserClue(beanDS)

        //分析设备信息
        analyzeDeviceDes(beanDS)

        val logBeanDS: DStream[((String, String), WebLogMsg)] = beanDS.map(x => ((x.appid, x.uuid), x))

        //分析标签
        analyzeUserLabel(logBeanDS)

        val results: DStream[((String, String), ListBuffer[ExUserVisit])] = logBeanDS.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

        //保存数据
        results.foreachRDD(saveVisitRdd(sourceInfos) _)
    }
}

object WebStreamUserTrack {
    def main(args: Array[String]) {
        val job = new WebStreamUserTrack
        job.run(args)
    }
}