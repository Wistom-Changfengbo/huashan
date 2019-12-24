package com.wangjia.bigdata.core.job.stream

import com.wangjia.bigdata.core.bean.info.ProbeInfo
import com.wangjia.bigdata.core.bean.probe.ProbeLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.{LoadDataHandler, LogClearHandler, LogClearStatus}
import com.wangjia.bigdata.core.job.SparkStreamJob
import com.wangjia.bigdata.core.utils.ExListBuffer.map2ExListBuffer
import com.wangjia.bigdata.core.utils.ExMap.map2ExMap
import com.wangjia.bigdata.core.utils.{JAInfoUtils, StreamUtils}
import com.wangjia.utils.{JavaUtils, RedisUtils}
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * 探针实时统计在店用户数
  *
  * Created by Administrator on 2018/3/5.
  */
class ProbeStreamCount extends SparkStreamJob {
    //一个批次时间(S)
    BATCH_DURATION_SECONDS = 20L

    /**
      * 探针统计间隔时间
      */
    private val PROBE_STATISTICS_INTERVAL_TIME: Long = 60L

    /**
      *
      * @param searchId 探针ID
      * @param pv       PV数
      * @param uv       用户MAC 集合
      * @param olUser   在线用户集合(时间、信号强度)
      * @param maxTime  日志最大时间
      * @param endTime  结束时间
      */
    private case class CountValue(searchId: String,
                                  pv: ListBuffer[Long],
                                  uv: mutable.Map[String, Long],
                                  olUser: mutable.Map[String, (Long, Int)],
                                  var maxTime: Long,
                                  var endTime: Long)

    //规则信息
    private val mapProbeInfos = new LoadDataHandler[mutable.Map[String, ProbeInfo]](Config.UPDATE_INFO_INTERVAL_TIME, JAInfoUtils.loadProbeInfo _)


    private def reduceCountValue(bUpdateOLUser2Redis: Boolean)(nValue: CountValue, oldVale: CountValue): CountValue = {
        val id = nValue.searchId
        val uv = nValue.uv.addValue(oldVale.uv, (t1, t2) => t1 < t2)

        var olUser: mutable.Map[String, (Long, Int)] = null
        if (!bUpdateOLUser2Redis) {
            olUser = nValue.olUser.addValue(oldVale.olUser, (t1, t2) => t1._1 > t2._1)
        } else {
            //离线
            val lineOff = ListBuffer[String]()
            for ((k, v) <- oldVale.olUser)
                if (!nValue.olUser.contains(k))
                    lineOff += k
            //上线
            val newLineOn = ListBuffer[String]()
            for ((k, v) <- nValue.olUser)
                if (!oldVale.olUser.contains(k))
                    newLineOn += k
            //写入Redis
            if (lineOff.size + newLineOn.size > 0) {
                val conn: Jedis = RedisUtils.getPConn(Config.REDIS_URL, Config.REDIS_PORT, Config.REDIS_DBINDEX)
                val time = System.currentTimeMillis()
                val dayStr = JavaUtils.timeMillis2DayNum(time)

                //val handlerKey = "HANDLER:" + dayStr + ":" + id + ":OL"
                val olKey = "PROBE:" + dayStr + ":" + id + ":OL"
                lineOff.foreach(x => {
                    //conn.hset(handlerKey, x, "del")
                    conn.hdel(olKey, x)
                })
                newLineOn.foreach(x => {
                    //conn.hset(handlerKey, x, "add")
                    val b = nValue.olUser.getOrElse(x, (0L, 0))
                    conn.hset(olKey, x, b._1 + "|" + b._2)
                })
                RedisUtils.closeConn(conn)
            }

            olUser = nValue.olUser
            for ((k, v) <- oldVale.olUser)
                if (olUser.contains(k)) {
                    olUser.put(k, v)
                }
        }

        val maxTime: Long = math.max(nValue.maxTime, oldVale.maxTime)
        val endTime: Long = {
            if (maxTime % PROBE_STATISTICS_INTERVAL_TIME == 0)
                maxTime
            else
                (maxTime / PROBE_STATISTICS_INTERVAL_TIME + 1) * PROBE_STATISTICS_INTERVAL_TIME
        }

        CountValue(id, nValue.pv ++ oldVale.pv, uv, olUser, maxTime, endTime)
    }

    //聚合新老数据
    private def reduceFunc(id: String, lnew: Seq[CountValue], of: Option[CountValue]): CountValue = {
        val newValue: CountValue = {
            if (lnew.size > 0)
                lnew.reduce(reduceCountValue(false) _)
            else
                CountValue(id, ListBuffer[Long](), mutable.Map[String, Long](), mutable.Map[String, (Long, Int)](), 0L, 0L)
        }
        val oldValue = of.getOrElse(CountValue(id, ListBuffer[Long](), mutable.Map[String, Long](), mutable.Map[String, (Long, Int)](), 0L, 0L))
        val result = reduceCountValue(true)(newValue, oldValue)

        //删除过时数据
        result.pv.remove((x: Long) => result.endTime - x > PROBE_STATISTICS_INTERVAL_TIME * 1000)
        result.uv.remove((k: String, v: Long) => result.endTime - v > PROBE_STATISTICS_INTERVAL_TIME * 1000)

        result
    }

    private val updateFunc = (iter: Iterator[(String, Seq[CountValue], Option[CountValue])]) => {
        val list = mutable.ListBuffer[(String, CountValue)]()
        while (iter.hasNext) {
            val b = iter.next()
            val id = b._1
            list += ((id, reduceFunc(id, b._2, b._3)))
        }
        list.toIterator
    }

    override protected def job(args: Array[String]): Unit = {
        val kafkaParams = Map[String, Object](
            "fetch.message.max.bytes" -> "10485760",
            "group.id" -> "wj_probe_flew_20180611"
        )
        val topics = Array("probe20180611")

        val lineDs = readLineByKafka(ssc, topics, kafkaParams)

        val beanDS: DStream[ProbeLogMsg] = lineDs.map(x => LogClearHandler.clearProbeLog(x, mapProbeInfos))
                .filter(_._1 == LogClearStatus.SUCCESS)
                .map(_._2.asInstanceOf[ProbeLogMsg])

        StreamUtils.saveLogLastTime(beanDS.map(_.time), "MONITOR:PROBE:LOG:LASTTIME")

        val result = beanDS.map(x => (x.id, x)).groupByKey().map(x => {
            //探针ID
            val id = x._1
            val iter: Iterator[ProbeLogMsg] = x._2.toIterator
            var pv = ListBuffer[Long]()
            val uv = mutable.Map[String, Long]()
            val olUser = mutable.Map[String, (Long, Int)]()
            var lastTime: Long = 0L
            while (iter.hasNext) {
                val bean = iter.next()
                pv += bean.time
                //取最大时间
                uv.addValue(bean.mac, bean.time, (t1, t2) => t1 < t2)
                //取最小时间
                olUser.addValue(bean.mac, (bean.time, bean.signal), (t1, t2) => t1._1 > t2._1)

                lastTime = math.max(lastTime, bean.time)
            }
            (id, CountValue(id, pv, uv, olUser, lastTime, 0))
        })

        val results = result.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

        val size: Int = (PROBE_STATISTICS_INTERVAL_TIME / BATCH_DURATION_SECONDS).toInt
        var runTimes: Int = 0
        results.foreachRDD(rdd => {
            runTimes += 1
            if (runTimes % size == 0) {
                rdd.foreachPartition(iterator => {
                    val monitorKey = "MONITOR:LASTTIME:PROBE"
                    val conn: Jedis = RedisUtils.getPConn(Config.REDIS_URL, Config.REDIS_PORT, Config.REDIS_DBINDEX)
                    while (iterator.hasNext) {
                        val b = iterator.next()._2
                        val id = b.searchId
                        val pv = b.pv.size
                        val uv = b.uv.size
                        val key = b.endTime.toString
                        val dayStr = JavaUtils.timeMillis2DayNum(b.endTime)

                        val keyHead = "PROBE:" + dayStr + ":" + id
                        conn.hset(keyHead + ":PV", key, pv.toString)
                        conn.hset(keyHead + ":UV", key, uv.toString)
                        conn.hset(keyHead + ":TIME", System.currentTimeMillis().toString, b.maxTime.toString)

                        conn.hset(monitorKey, id, b.maxTime.toString)
                    }
                    RedisUtils.closeConn(conn)
                })
            }
        })
    }
}

object ProbeStreamCount {
    def main(args: Array[String]) {
        val job = new ProbeStreamCount
        job.run(args)
    }
}