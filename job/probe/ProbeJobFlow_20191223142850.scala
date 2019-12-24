package com.wangjia.bigdata.core.job.probe

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat

import com.wangjia.bigdata.core.bean.info.ProbeInfo
import com.wangjia.bigdata.core.bean.probe.ProbeLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.{LogClearHandler, LogClearStatus}
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{JAInfoUtils, JDBCBasicUtils}
import com.wangjia.handler.probe.{MacProbeHandler, ProbeDto}
import com.wangjia.hbase.conn.{ExTBConnection, ExTable}
import com.wangjia.hbase.{HBaseConst, HBaseTableName}
import com.wangjia.utils.{HBaseUtils, JavaUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

//mac停留时间
case class VisitMac(venueId: Int, mac: String, startTime: Long, stayTime: Long)

/**
  * Created by Cfb on 2017/9/14.
  */
class ProbeJobFlow extends EveryDaySparkJob {

    //最大间隔时间,超过此时间视为离开
    private val MAX_INTERVAL_TIME = 2 * 3600 * 1000

    //最小停留时间，停留小于此时间视为误差数据
    private val MIN_STAY_TIME = 60 * 1000

    //广播厂家信息
    private var brandInfosBroadcast: Broadcast[mutable.Map[String, String]] = null

    //广播探针信息
    private var mapProbeInfosBroadcast: Broadcast[mutable.Map[String, ProbeInfo]] = null

    override protected def init(): Unit = {
        super.init()
        //广播厂家信息
        val brand: mutable.Map[String, String] = JAInfoUtils.loadMac2PhoneBrand
        brandInfosBroadcast = SparkExtend.ctx.broadcast(brand)

        //广播探针信息
        val info: mutable.Map[String, ProbeInfo] = JAInfoUtils.loadProbeInfo
        mapProbeInfosBroadcast = SparkExtend.ctx.broadcast(info)
    }

    val brandFunc = (x: String) => {
        if (x.length > 6) {
            val macMatch = x.substring(0, 6).toUpperCase
            brandInfosBroadcast.value.getOrElse(macMatch, "")
        } else {
            ""
        }
    }

    /**
      * 过滤原始日志
      *
      * @param iterable
      * @return
      */
    private def filterProbes(iterable: Iterable[ProbeLogMsg]): List[ProbeLogMsg] = {
        val size = iterable.size
        if (size < 3)
            return null
        if (size > 5760)
            return null
        val list = iterable.toList.sortWith(_.time < _.time)
        var startTime: Long = list.head.time
        list.foreach(x => {
            if (x.time - startTime > MAX_INTERVAL_TIME)
                startTime = x.time
            if (x.time - startTime > MIN_STAY_TIME)
                return list
        })
        null
    }

    private def makeUpVisitBeans(beans: List[ProbeLogMsg]): List[List[ProbeLogMsg]] = {
        var time = beans.head.time
        val endPoints = new ListBuffer[Long]
        val list = new ListBuffer[List[ProbeLogMsg]]
        var endPointBeans = new ListBuffer[ProbeLogMsg]
        beans.foreach(x => {
            if (x.time - time > MAX_INTERVAL_TIME) {
                endPoints += time
            }
            time = x.time
        })
        endPoints += Long.MaxValue
        val size = beans.size
        var end = 0
        var sesssionEndTime = endPoints(end)
        var i = 0
        while (i < size) {
            val bean = beans(i)
            if (bean.time < sesssionEndTime) {
                endPointBeans += bean
            } else {
                end += 1
                sesssionEndTime = endPoints(end)
                list += endPointBeans.toList
                endPointBeans.clear()
                endPointBeans += bean
            }
            i += 1
        }
        if (endPointBeans.nonEmpty) {
            list += endPointBeans.toList
        }
        list.filter(_.nonEmpty).toList
    }

    /**
      * 把一个设备的访问数据转换成 VisitMacList
      *
      * @param beans 设备的访问记录
      * @return
      */
    private def beans2Visit(beans: List[ProbeLogMsg]): ListBuffer[VisitMac] = {
        val visitList = new ListBuffer[VisitMac]()
        try {
            val visitBeans = makeUpVisitBeans(beans)
            visitBeans.foreach(bs => {
                val visit = trans2IndependentVisit(bs)
                if (visit != null)
                    visitList += visit
            })
        } catch {
            case e: Exception => e.printStackTrace()
        }
        visitList
    }

    private def trans2IndependentVisit(list: List[ProbeLogMsg]): VisitMac = {
        val headBean = list.head
        val stayTime: Long = list.last.time - headBean.time
        if (stayTime < MIN_STAY_TIME)
            return null
        VisitMac(headBean.venueId, headBean.mac, headBean.time, stayTime)
    }

    /**
      * 分析MAC列表
      *
      * @param groupBeansRdd
      */
    private def analyzeMacList(groupBeansRdd: RDD[((Int, String), List[ProbeLogMsg])]): Unit = {
        //MAC列表写入Mysql
        groupBeansRdd.map(x => (x._1, x._2.last.time))
                .foreachPartition(iterator => {
                    val date = new java.sql.Date(this.logDate.getTime)
                    val newDate = new java.sql.Date(System.currentTimeMillis())
                    JDBCBasicUtils.insertBatchByIterator[((Int, String), Long)]("ja_data_mac", iterator, (pstmt, b) => {
                        pstmt.setInt(1, b._1._1)
                        pstmt.setString(2, b._1._2)
                        pstmt.setLong(3, b._2)
                        pstmt.setString(4, brandFunc(b._1._2))
                        pstmt.setDate(5, date)
                        pstmt.setDate(6, newDate)
                    })
                })
        groupBeansRdd.map(x => (x._1, x._2.last.time))
                .foreachPartition(iterator => {
                    val date = new java.sql.Date(this.logDate.getTime)
                    val newDate = new java.sql.Date(System.currentTimeMillis())
                    JDBCBasicUtils.insertBatchByIterator[((Int, String), Long)]("ja_data_mac_venue_everyday", iterator, (pstmt, b) => {
                        pstmt.setInt(1, b._1._1)
                        pstmt.setString(2, b._1._2)
                        pstmt.setString(3, brandFunc(b._1._2))
                        pstmt.setDate(4, date)
                        pstmt.setDate(5, newDate)
                    })
                })
    }

    /**
      * 分析每小时流量
      *
      * @param groupBeansRdd
      */
    private def analyzeMinuteFlew(groupBeansRdd: RDD[((Int, String), List[ProbeLogMsg])], minuteRDD: RDD[((Int, Long), Int)]): Unit = {

        val sdf = new SimpleDateFormat("yyyyMMdd")
        val format: String = sdf.format(this.logDate)

        val url = JDBCBasicUtils.getProperty("probeMinute.url")
        val username = JDBCBasicUtils.getProperty("probeMinute.username")
        val password = JDBCBasicUtils.getProperty("probeMinute.password")
        val conn: Connection = JDBCBasicUtils.getConn(url, username, password)

        val tableNameMinute = s"ja_data_probe_minute_everyday_$format"
        val createSqlMinute =
            s"""
               |CREATE TABLE IF NOT EXISTS `$tableNameMinute`(
               |`id` int(11) NOT NULL AUTO_INCREMENT,
               |`venueId` int NOT NULL,
               |`minute` bigint NOT NULL,
               |`count` int NOT NULL,
               |`day` date,
               |`addtime` date,
               |PRIMARY KEY (`id`),
               |index `idx_time`(`venueId`,`minute`,`day`))
               |ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
            """.stripMargin
        //一个月创建一次表
        val psMinute: PreparedStatement = conn.prepareStatement(createSqlMinute)
        psMinute.executeUpdate(createSqlMinute)
        psMinute.close()
        conn.close()

        //        ja_data_probe_minute_everyday
        val orginMinute = this.logDate.getTime / (60 * 1000)
        //场馆探针按分钟统计
        val countByMinutes: RDD[((Int, Long), Int)] = groupBeansRdd.flatMap(x => {

            val venueId = x._1._1
            val mac = x._1._2
            val set = mutable.Set[((Int, Long), String)]()
            x._2.foreach(bean => {
                set += (((venueId, (bean.time / (60 * 1000)) - orginMinute), mac))
            })
            set
        }).map(x => (x._1, 1)).union(minuteRDD).reduceByKey(_ + _)

        val sql = JDBCBasicUtils.getProperty("ja_data_probe_minute_everyday").replace("${tableName}", tableNameMinute)
        //写入Mysql
        countByMinutes.foreachPartition(iterator => {
            val url = JDBCBasicUtils.getProperty("probeMinute.url")
            val username = JDBCBasicUtils.getProperty("probeMinute.username")
            val password = JDBCBasicUtils.getProperty("probeMinute.password")
            val conn: Connection = JDBCBasicUtils.getConn(url, username, password)

            val date = new java.sql.Date(this.logDate.getTime)
            //日志当天最初的小时

            val newDate = new java.sql.Date(System.currentTimeMillis())
            JDBCBasicUtils.insertBatchByIteratorBySql[((Int, Long), Int)](conn)(sql, tableNameMinute, iterator, (pstmt, b) => {
                pstmt.setInt(1, b._1._1)
                pstmt.setLong(2, b._1._2)
                pstmt.setInt(3, b._2)
                pstmt.setDate(4, date)
                pstmt.setDate(5, newDate)
            })
        })
    }


    /**
      * 分析每小时流量
      *
      * @param groupBeansRdd
      */
    private def analyzeHourFlew(groupBeansRdd: RDD[((Int, String), List[ProbeLogMsg])], hourRDD: RDD[((Int, Long), Int)]): Unit = {
        val orginHour = this.logDate.getTime / (60 * 60 * 1000)
        //场馆探针按小时统计
        val countByHours: RDD[((Int, Long), Int)] = groupBeansRdd.flatMap(x => {
            val venueId = x._1._1
            val mac = x._1._2
            val set = mutable.Set[((Int, Long), String)]()
            x._2.foreach(bean => {
                set += (((venueId, (bean.time / (60 * 60 * 1000)) - orginHour), mac))
            })
            set
        }).map(x => (x._1, 1)).union(hourRDD).reduceByKey(_ + _)

        //写入Mysql
        countByHours.foreachPartition(iterator => {
            val date = new java.sql.Date(this.logDate.getTime)
            //日志当天最初的小时

            val newDate = new java.sql.Date(System.currentTimeMillis())
            JDBCBasicUtils.insertBatchByIterator[((Int, Long), Int)]("ja_data_probe_hour_everyday", iterator, (pstmt, b) => {
                pstmt.setInt(1, b._1._1)
                pstmt.setLong(2, b._1._2)
                pstmt.setInt(3, b._2)
                pstmt.setDate(4, date)
                pstmt.setDate(5, newDate)
            })
        })
    }

    /**
      * 任务入口方法
      *
      * @param args
      */
    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.MAC_PROBE)
        HBaseUtils.createTable(HBaseTableName.MAC_PROBE_VISIT)

        val dayNum = JavaUtils.timeMillis2DayNum(this.logDate.getTime)
        val sc = SparkExtend.ctx

        val beanRdd = sc.textFile(this.inPath)
                .map(x => LogClearHandler.clearProbeLog(x, mapProbeInfosBroadcast.value))
                .filter(_._1 == LogClearStatus.SUCCESS)
                .map(_._2.asInstanceOf[ProbeLogMsg])
                .filter(x => dayNum == JavaUtils.timeMillis2DayNum(x.time))

        val groupBeansRdd: RDD[((Int, String), List[ProbeLogMsg])] = beanRdd
                .map(x => ((x.venueId, x.mac, x.time / 60 * 1000), x))
                .reduceByKey((x1, x2) => if (x1.time > x2.time) x1 else x2)
                .map(_._2)
                .groupBy(x => (x.venueId, x.mac))
                .map(x => {
                    val key = x._1
                    val beans = filterProbes(x._2)
                    if (beans == null)
                        null
                    else
                        (key, beans)
                }).filter(_ != null)
        groupBeansRdd.cache()
        groupBeansRdd.checkpoint()

        //得到总数和新增
        val newAddRdd = groupBeansRdd.mapPartitions(iterable => {
            initHBase()
            val gets = ListBuffer[ProbeDto]()
            val beans = ListBuffer[ProbeDto]()
            val handler = new MacProbeHandler()
            iterable.foreach(x => {
                gets += new ProbeDto(x._1._1, x._1._2, x._2.head.time)
                if (gets.size > Config.BATCH_SIZE) {
                    beans ++= handler.findMac(gets)
                    gets.clear()
                }
            })
            if (gets.nonEmpty) {
                beans ++= handler.findMac(gets)
                gets.clear()
            }
            beans.toIterator
        }).filter(_.getFirstTime != -1).map(bean => {
            val acc = if (brandFunc(bean.getMac) != "") 1 else 0
            val day = JavaUtils.timeMillis2DayNum(bean.getTime) - JavaUtils.timeMillis2DayNum(bean.getFirstTime)
            if (day == 0)
                ((bean.getVenueId, acc), (1, 1))
            else
                ((bean.getVenueId, acc), (1, 0))
        }).reduceByKey((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2))

        //得到访问轨迹
        val visitRdd: RDD[VisitMac] = groupBeansRdd.flatMap(x => beans2Visit(x._2))

        //求访问时间和访问次数
        val timesRdd = visitRdd.map(x => {
            val acc = if (brandFunc(x.mac) != "") 1 else 0
            ((x.venueId, acc), (x.stayTime, 1))
        })
                .reduceByKey((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2))

        //venueId、acc、totalNum、newArrival、totalStayTime、totalVisits
        val result: RDD[((Int, Int), ((Int, Int), (Long, Int)))] = newAddRdd.join(timesRdd)
        //写入数据库
        result.foreachPartition(iterator => {
            val date = new java.sql.Date(this.logDate.getTime)
            val newDate = new java.sql.Date(System.currentTimeMillis())
            JDBCBasicUtils.insertBatchByIterator[((Int, Int), ((Int, Int), (Long, Int)))]("ja_data_probe_mac_count_everyday", iterator, (pstmt, b) => {
                pstmt.setInt(1, b._1._1)
                pstmt.setInt(2, b._1._2)
                pstmt.setInt(3, b._2._1._1)
                pstmt.setLong(4, b._2._1._2)
                pstmt.setLong(5, b._2._2._1)
                pstmt.setLong(6, b._2._2._2)
                pstmt.setDate(7, date)
                pstmt.setDate(8, newDate)
            })
        })

        //保存访问轨迹
        visitRdd.foreachPartition(iterator => {
            initHBase()
            val conn: ExTBConnection = new ExTBConnection()
            val tb: ExTable = conn.getTable(HBaseTableName.MAC_PROBE_VISIT)
            iterator.foreach(visit => {
                val put: Put = new Put(Bytes.toBytes(visit.mac))
                put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes(visit.venueId + "|" + visit.startTime), Bytes.toBytes(visit.stayTime))
                tb.addPut(put)
            })
            conn.close()
        })

        //分析MAC列表
        analyzeMacList(groupBeansRdd)

        val probeInfo: mutable.Map[String, ProbeInfo] = mapProbeInfosBroadcast.value

        val list = probeInfo.filter((k) => {
            k._2.active == 1
        }).map(_._2.venueId).toList

        val rangeMinute: Array[Long] = Array.range(0, 1440).map(x => x.toLong)
        val rangeHour: Array[Long] = Array.range(0, 24).map(x => x.toLong)

        val minuteRDD = sc.makeRDD(rangeMinute.flatMap(x => {
            val tuples = new ListBuffer[(Int, Long)]
            for (venueId: Int <- list) {
                tuples += ((venueId, x))
            }
            tuples
        })).map(x => (x, 0))

        //把每个小时跟appid拼到一起，并做成key，value形式去join
        val hourRDD = sc.makeRDD(rangeHour.flatMap(x => {
            val tuples = new ListBuffer[(Int, Long)]
            for (venueId: Int <- list) {
                tuples += ((venueId, x))
            }
            tuples
        })).map(x => (x, 0))

        //分析每小时流量
        analyzeHourFlew(groupBeansRdd, hourRDD)
        //分析每分钟流量
        analyzeMinuteFlew(groupBeansRdd, minuteRDD)
    }
}

object ProbeJobFlow {
    def main(args: Array[String]) {
        val job = new ProbeJobFlow()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
