package com.wangjia.bigdata.core.job.web

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.baidu.drapi.autosdk.sms.service.RealTimeQueryResultType
import com.wangjia.bean.sem.{SemKeyWordInfo, SemKeyWordRelateInfo}
import com.wangjia.bigdata.core.bean.info.SemUserInfo
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{JAInfoUtils, JDBCBasicUtils}
import com.wangjia.sem.ReportService
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * Created by Cfb on 2018/5/16.
  */
class WebJobSem extends EveryDaySparkJob {
  /**
    * 任务入口方法
    *
    * @param args
    */
  override protected def job(args: Array[String]): Unit = {
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
    val begin = sdf1.format(this.logDate) + " 00:00:00"
    val end = sdf1.format(this.logDate) + " 23:59:59"

    val sc: SparkContext = SparkExtend.ctx

    val semUserInfoList: ListBuffer[SemUserInfo] = JAInfoUtils.loadSemUserInfo()

    val keywordsRDD = sc.makeRDD(semUserInfoList.map(x => {
      val reportService = new ReportService(x.sourceid, x.appid, x.username, x.password, x.token, x.target)
      val l1: java.util.List[SemKeyWordInfo] = reportService.getRealTimePairData(null, 12, 5, 15, begin, end)
      val l2: java.util.List[SemKeyWordRelateInfo] = reportService.getRealTimeQueryData(null, 0, 12, 5, 6, begin, end)
      (l1, l2)
    }).filter(x => x._1 != null && x._2 != null))

    val keywords = keywordsRDD.flatMap(_._1)
    
    val keywordsRelate = keywordsRDD.flatMap(_._2)

    keywords.foreachPartition(iter => {
      val date = new java.sql.Date(logDate.getTime)
      val addTime = new Timestamp(System.currentTimeMillis())
      JDBCBasicUtils.insertBatchByIterator[SemKeyWordInfo]("sql_insert_sem_keyword_everyday", iter, (pstmt, bean) => {
        pstmt.setString(1, bean.getAppid)
        pstmt.setString(2, bean.getSourceid)
        pstmt.setLong(3, bean.getKeywordId)
        pstmt.setLong(4, bean.getCreativeId)
        pstmt.setString(5, bean.getUsername)
        pstmt.setString(6, bean.getCampaignName)
        pstmt.setString(7, bean.getAdgroup)
        pstmt.setString(8, bean.getKeyword)
        pstmt.setString(9, bean.getCreativeTitle)
        pstmt.setString(10, bean.getCreativeDesc1)
        pstmt.setString(11, bean.getCreativeDesc2)
        pstmt.setString(12, bean.getCreativeUrl)
        pstmt.setInt(13, bean.getDisplay)
        pstmt.setInt(14, bean.getClick)
        pstmt.setFloat(15, bean.getCost)
        pstmt.setFloat(16, bean.getCostAvg)
        pstmt.setFloat(17, bean.getClickRate)
        pstmt.setFloat(18, bean.getCpm)
        pstmt.setFloat(19, bean.getConversion)
        pstmt.setDate(20, new java.sql.Date(sdf1.parse(bean.getTimeStamp).getTime))
        pstmt.setDate(21, date)
        pstmt.setTimestamp(22, addTime)

      })
    })
    keywordsRelate.foreachPartition(iter => {
      val date = new java.sql.Date(logDate.getTime)
      val addTime = new Timestamp(System.currentTimeMillis())
      JDBCBasicUtils.insertBatchByIterator[SemKeyWordRelateInfo]("sql_insert_sem_keyword_relate_everyday", iter, (pstmt, bean) => {
        pstmt.setString(1, bean.getAppid)
        pstmt.setString(2, bean.getSourceid)
        pstmt.setString(3, bean.getQuery)
        pstmt.setLong(4, bean.getKeywordId)
        pstmt.setLong(5, bean.getCreativeId)
        pstmt.setInt(6, bean.getDisplay)
        pstmt.setInt(7, bean.getClick)
        pstmt.setFloat(8, bean.getCost)
        pstmt.setFloat(9, bean.getClickRate)
        pstmt.setDate(10, new java.sql.Date(sdf1.parse(bean.getTimeStamp).getTime))
        pstmt.setDate(11, date)
        pstmt.setTimestamp(12, addTime)
      })
    })
  }
}

object WebJobSem {
  def main(args: Array[String]) {
    val job = new WebJobSem()
    job.run(args)
    println(System.currentTimeMillis() - job.jobStartTime)
  }
}
