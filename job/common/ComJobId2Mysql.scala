package com.wangjia.bigdata.core.job.common

import java.sql.Timestamp

import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.JDBCBasicUtils
import com.wangjia.common.Platform
import org.apache.spark.rdd.RDD

/**
  * 统计各个平台的UUID,UserId,UserId2UUID信息
  *
  * --inPath    日志输入目录
  * --date      日志日期(yyyy-MM-dd)
  * --platform  平台:web,app，weixin
  *
  * Created by Cfb on 2018/2/26.
  */

case class ComId2MysqlBean(appid: String, deviceid: String, userid: String, acc: Int, time: Long)

class ComJobId2Mysql extends EveryDaySparkJob {

    private def loadWeb(inpath: String): RDD[ComId2MysqlBean] = {
        val beanRdd = SparkExtend.ctx.textFile(inpath).map(BeanHandler.toWebAll)
                .filter(_ != null)
                .map(x => ComId2MysqlBean(x.appid, x.dStrInfo("ja_uuid"), x.userid, x.acc, x.time))
        beanRdd
    }

    private def loadApp(inpath: String): RDD[ComId2MysqlBean] = {
        val beanRdd = SparkExtend.ctx.textFile(inpath).map(BeanHandler.toAppAll)
                .filter(_ != null)
                .map(x => ComId2MysqlBean(x.appid, x.dStrInfo("deviceid"), x.userid, 1, x.time))
        beanRdd
    }

    private def loadWeiXin(inpath: String): RDD[ComId2MysqlBean] = {
        val beanRdd = SparkExtend.ctx.textFile(inpath).map(BeanHandler.toWeiXin)
                .filter(_ != null)
                .map(x => ComId2MysqlBean(x.appid, x.dStrInfo("ja_uuid"), x.userid, 1, x.time))
        beanRdd
    }

    /**
      * 分析UUID 保存入MySql
      *
      * @param beans RDD[BriefBaen]
      */
    private def analyzeUUID(beans: RDD[ComId2MysqlBean]): Unit = {
        val uuidRdd = beans.groupBy(x => (x.appid, x.deviceid)).map(x => {
            val list = x._2.toList.sortWith(_.time < _.time)
            val b = list.head
            (x._1._1, x._1._2, b.acc, b.time)
        })

        uuidRdd.foreachPartition(iterator => {
            val newDate = new Timestamp(System.currentTimeMillis())
            var i = 0
            JDBCBasicUtils.insertBatch("sql_data_uuid", pstmt => {
                while (iterator.hasNext) {
                    val b = iterator.next()

                    pstmt.setString(1, b._1)
                    pstmt.setString(2, b._2)
                    pstmt.setInt(3, b._3)
                    pstmt.setLong(4, b._4)
                    pstmt.setTimestamp(5, newDate)
                    pstmt.setInt(6, b._3)
                    pstmt.addBatch()

                    i += 1
                    if (i > Config.BATCH_SIZE) {
                        pstmt.executeBatch()
                        pstmt.clearBatch()
                        i = 0
                    }
                }

            })
        })
    }

    /**
      * 分析UserId 保存入MySql
      *
      * @param beans
      */
    private def analyzeUserId(beans: RDD[ComId2MysqlBean]): Unit = {
        val uuidRdd: RDD[(String, String, Long)] = beans.filter(_.userid != "").groupBy(x => (x.appid, x.userid)).map(x => {
            val list = x._2.toList.sortWith(_.time < _.time)
            val b = list.head
            (x._1._1, x._1._2, b.time)
        })
        uuidRdd.foreachPartition(iterator => {
            val newDate = new Timestamp(System.currentTimeMillis())
            var i = 0
            JDBCBasicUtils.insertBatch("sql_data_userid", pstmt => {
                while (iterator.hasNext) {
                    val b = iterator.next()
                    pstmt.setString(1, b._1)
                    pstmt.setString(2, b._2)
                    pstmt.setLong(3, b._3)
                    pstmt.setTimestamp(4, newDate)

                    pstmt.addBatch()

                    i += 1
                    if (i > Config.BATCH_SIZE) {
                        pstmt.executeBatch()
                        pstmt.clearBatch()
                        i = 0
                    }
                }
            })
        })
    }

    /**
      * 分析UserId2UUID 保存入MySql
      *
      * @param beans
      */
    private def analyzeUserId2UUID(beans: RDD[ComId2MysqlBean]): Unit = {
        val uuidRdd = beans.filter(_.userid != "").groupBy(x => (x.appid, x.userid, x.deviceid)).map(x => {
            val list = x._2.toList.sortWith(_.time < _.time)
            val b = list.head
            (x._1._1, x._1._2, x._1._3, b.acc, b.time)
        })

        uuidRdd.foreachPartition(iterator => {
            val newDate = new Timestamp(System.currentTimeMillis())
            var i = 0
            JDBCBasicUtils.insertBatch("sql_data_uuid_userid", pstmt => {
                while (iterator.hasNext) {
                    val b = iterator.next()

                    pstmt.setString(1, b._1)
                    pstmt.setString(2, b._2)
                    pstmt.setString(3, b._3)
                    pstmt.setInt(4, b._4)
                    pstmt.setLong(5, b._5)
                    pstmt.setTimestamp(6, newDate)
                    pstmt.setInt(7, b._4)
                    pstmt.addBatch()

                    i += 1
                    if (i > Config.BATCH_SIZE) {
                        pstmt.executeBatch()
                        pstmt.clearBatch()
                        i = 0
                    }
                }
            })
        })
    }

    /**
      * 任务入口方法
      *
      * @param args
      */
    override protected def job(args: Array[String]): Unit = {
        val sc = SparkExtend.ctx
        val linesRdd = sc.textFile(this.inPath)

        //平台参数
        val platform = this.getParameterOrElse("platform", "null")

        val beanRdd: RDD[ComId2MysqlBean] = platform match {
            case Platform.PLATFORM_NAME_WEB => loadWeb(this.inPath)
            case Platform.PLATFORM_NAME_MOBILE => loadApp(this.inPath)
            case Platform.PLATFORM_NAME_WEIXIN => loadWeiXin(this.inPath)
            case _ => throw new RuntimeException("no define platform")
        }
        beanRdd.cache()
        beanRdd.checkpoint()

        //分析UUID 保存入MySql
        analyzeUUID(beanRdd)

        //分析UserId 保存入MySql
        analyzeUserId(beanRdd)

        //分析UserId2UUID 保存入MySql
        analyzeUserId2UUID(beanRdd)

    }


}

object ComJobId2Mysql {
    def main(args: Array[String]) {
        val job = new ComJobId2Mysql()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}

