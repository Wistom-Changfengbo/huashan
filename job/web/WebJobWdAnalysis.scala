package com.wangjia.bigdata.core.job.web

import java.net.URLDecoder
import java.util

import com.wangjia.bigdata.core.bean.info.{SourceInfo, StrRule}
import com.wangjia.bigdata.core.bean.web.WebLogMsg
import com.wangjia.bigdata.core.handler.{BeanHandler, KeyWordHandler, RuleHandler}
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{JAInfoUtils, JDBCBasicUtils}
import com.wangjia.common.{LogEventId, LogType}
import com.wangjia.utils.JavaUtils
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ListBuffer

class WebJobWdAnalysis extends EveryDaySparkJob {

    private case class KWBean(appid: String,
                              uuid: String,
                              sourceId: Int,
                              kw: String,
                              pageNum: Int,
                              bUserId: Boolean,
                              bClue: Boolean,
                              ref: String)

    //来源地址映射
    private var sourceInfosBroadcast: Broadcast[Array[SourceInfo]] = null

    override protected def init(): Unit = {
        super.init()
        //加载广播来源地址映射
        val sourceInfos = JAInfoUtils.loadSourceInfos
        sourceInfosBroadcast = SparkExtend.ctx.broadcast(sourceInfos)
    }


    override protected def job(args: Array[String]): Unit = {
        val sc = SparkExtend.ctx
        val lines = sc.textFile(this.inPath)

        val logBeanRdd = lines.map(BeanHandler.toWebAll)
                .filter(_ != null)

        logBeanRdd.filter(x => x.appid == "1001" && x.ref.indexOf("baidu") != -1)
                .filter(_.url.indexOf("m.jiajuol.com/special/81") != -1)
                .map(_.ref)
                .map(x=>URLDecoder.decode(x,"UTF-8"))
                .foreach(println)

        /*
        val sourceVisitRdd = logBeanRdd.groupBy(x => (x.uuid, x.appid))
                .flatMap { case ((uuid, appid), iterable) =>
                    val beans = iterable.toList.sortWith(_.time < _.time)
                    val list = new ListBuffer[(Int, ListBuffer[WebLogMsg])]
                    var l: ListBuffer[WebLogMsg] = null
                    beans.foreach(b => {
                        val info = RuleHandler.getMeetRule(sourceInfosBroadcast.value.asInstanceOf[Array[StrRule]], b.ref)
                        val sourceid = if (info != null) info.asInstanceOf[SourceInfo].id else 0
                        if (sourceid > 1) {
                            l = null
                            list.foreach(x => if (x._2.head.ref.equals(b.ref)) l = x._2)
                            if (l == null) {
                                l = new ListBuffer[WebLogMsg]
                                list += ((sourceid, l))
                            }
                            l += b
                        } else if (l != null) {
                            l += b
                        }
                    })
                    list.toIterator
                }

        val wkBeanRdd = sourceVisitRdd.map(f = x => {
            var pageNum: Int = 0
            var bClue: Boolean = false
            var bUserId: Boolean = false
            x._2.foreach(b => {
                if (b.logtype == LogType.PAGE)
                    pageNum += 1
                else if (b.logtype == LogType.EVENT && b.subtype == LogEventId.USER_COMMIT_CLUE)
                    bClue = true
                if (b.userid.nonEmpty)
                    bUserId = true
            })
            val bean = x._2.head
            val refUrl = x._2.head.ref
            val sourceId = x._1
            val kw = KeyWordHandler.url2KeyWord(sourceId, refUrl)

            KWBean(bean.appid, bean.uuid, x._1, if (kw == null) "" else kw._2, pageNum, bUserId, bClue, bean.ref)
        }).filter(x => x.kw != "" && !JavaUtils.isMessyCode(x.kw))

        wkBeanRdd.foreach(println)
        */

        //        val len = logBeanRdd.map(_.ref.length).max()
        //        println(len)

        //        wkBeanRdd.map(wk => ((wk.sourceId, wk.appid, wk.kw), (1, wk.pageNum, if (wk.bClue) 1 else 0, if (wk.bUserId) 1 else 0)))
        //                .reduceByKey((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2, x1._3 + x2._3, x1._4 + x2._4))
        //                .foreachPartition(iter => {
        //                    val date = new java.sql.Date(logDate.getTime)
        //                    val newDate = new java.sql.Date(System.currentTimeMillis())
        //                    val sql = "insert into ja_data_keyword(appid,sourceid,kw,num,page_num,clue_num,userid_num,day,addtime) values(?,?,?,?,?,?,?,?,?)"
        //                    JDBCBasicUtils.insertBatchByIteratorBySql[((Int, String, String), (Int, Int, Int, Int))](JDBCBasicUtils.getConn())(sql, "ja_data_keyword", iter, (pstmt, bean) => {
        //                        pstmt.setString(1, bean._1._2)
        //                        pstmt.setInt(2, bean._1._1)
        //                        pstmt.setString(3, bean._1._3)
        //                        pstmt.setInt(4, bean._2._1)
        //                        pstmt.setInt(5, bean._2._2)
        //                        pstmt.setInt(6, bean._2._3)
        //                        pstmt.setInt(7, bean._2._4)
        //                        pstmt.setDate(8, date)
        //                        pstmt.setDate(9, newDate)
        //                    })
        //                })

        //        logBeanRdd.map(b => {
        //            val info = RuleHandler.getMeetRule(sourceInfosBroadcast.value.asInstanceOf[Array[StrRule]], b.ref)
        //            val sourceid = if (info != null) info.asInstanceOf[SourceInfo].id else 0
        //            (sourceid, b)
        //        }).filter(_._1 == 3).map(_._2.ref).foreach(println)


        //        logBeanRdd.filter(_.ref.indexOf("baidu.com") > 0)
        //                .map(_.ref)
        //                .flatMap(url => JavaUtils.urlRequest(url))
        //                .map(x => (x._1, 1))
        //                .reduceByKey(_ + _)
        //                .filter(_._1.length < 200)
        //                .repartition(1)
        //                .sortBy(_._2)
        //                .foreach(println)

        //        logBeanRdd.filter(_.ref.indexOf("m.sm.cn") > 0)
        //                .map(_.ref)
        //                .flatMap(url => JavaUtils.urlRequest(url))
        //                .map(x => (x._1, 1))
        //                .reduceByKey(_ + _)
        //                .filter(_._1.length < 200)
        //                .repartition(1)
        //                .sortBy(_._2)
        //                .foreach(println)


    }
}

object WebJobWdAnalysis {

    def main(args: Array[String]) {
        val job = new WebJobWdAnalysis()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
