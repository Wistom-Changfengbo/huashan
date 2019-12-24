package com.wangjia.bigdata.core.job.mobile

import java.text.SimpleDateFormat
import java.util.Date

import com.google.gson.JsonParser
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.JDBCBasicUtils
import com.wangjia.common.LogType
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Cfb on 2018/1/19.
  */
class MobileJobRecommandStatisticCount extends EveryDaySparkJob {
    private var todayAm: Long = 0
    private var todayPm: Long = 0
    private val oneDayTime: Long = 1000 * 60 * 60 * 24
    private val photoSql = "select uuid  from ja_data_recommend_photo_$time group by uuid"
    private val caseSql = "select uuid  from ja_data_recommend_case_$time group by uuid"
    private val sdf = new SimpleDateFormat("yyyyMMdd")
    private var makeupUrl: String = null
    private var makeupUsername: String = null
    private var makeupPassword: String = null
    private var makeupUnit: String = null

    def load(): ListBuffer[(String, Int)] = {
        val list = mutable.ListBuffer[(String, Int)]()
        val date: Date = new Date()
        date.setTime(todayAm)
        val format: String = sdf.format(date)
        val conn = JDBCBasicUtils.getConn(makeupUrl, makeupUsername, makeupPassword)
        val replace: String = photoSql.replace("$time", format)
        println(s"++++$replace")
        JDBCBasicUtils.read(conn, conn.prepareStatement(replace), rs => {
            while (rs.next()) {
                val uuid = rs.getString("uuid")
                list += ((uuid, 1))
            }
        })
        list
    }


    def loadCase(): ListBuffer[(String, Int)] = {
        val list = mutable.ListBuffer[(String, Int)]()
        val date: Date = new Date()
        date.setTime(todayAm)
        val format: String = sdf.format(date)
        val conn = JDBCBasicUtils.getConn(makeupUrl, makeupUsername, makeupPassword)
        val replace = caseSql.replace("$time", format)
        println(s"++++$replace")
        JDBCBasicUtils.read(conn, conn.prepareStatement(replace), rs => {
            while (rs.next()) {
                val uuid = rs.getString("uuid")
                list += ((uuid, 1))
            }
        })
        list
    }

    override protected def init(): Unit = {
        super.init()
        val sc = SparkExtend.ctx
        todayPm = this.logDate.getTime + oneDayTime
        todayAm = this.logDate.getTime
        makeupUrl = JDBCBasicUtils.getProperty("makeup.url")
        makeupUsername = JDBCBasicUtils.getProperty("makeup.username")
        makeupPassword = JDBCBasicUtils.getProperty("makeup.password")
    }


    //
    override protected def job(args: Array[String]): Unit = {
        val sc = SparkExtend.ctx
        val lineRDD: RDD[String] = sc.textFile(this.inPath)
        val baseRDD = lineRDD.map(BeanHandler.toAppAcc).filter(x=> x!= null && x.logtype==LogType.PAGE && x.appid == "1200" )
        baseRDD.cache()


        //总的用户数
        val mainTotalF = baseRDD.filter(_.pageid == "main_tab_photo_list").map(x => (x.uuid, 1)).distinct()
        //photorecommand的数据
        val photoReco: RDD[(String, Int)] = sc.makeRDD(load())
        //有推荐的用户
        val recommandPhotoF = mainTotalF.join(photoReco)

        //无推荐用户
        val recommandPhotoNoF = mainTotalF.leftOuterJoin(photoReco).filter(x => x._2._2 == None)
        //无推荐用户去设备库找，拿到 默认的用户总数
        //点击用户
        val hitTotalF: RDD[(String, Int)] = baseRDD.groupBy(_.uuid).map(x => (x._1, x._2.toList.sortWith(_.time < _.time)))
                .map(x => {
                    var sb = new StringBuilder
                    x._2.foreach(b => sb.append(b.pageid))
                    (x._1, sb.toString())
                }).filter(_._2.contains("main_tab_photo_listdetails_gallery")).map(x => (x._1, 1))


        //默认用户点击数 newArrivalPhotoF 去找两个挨着的
        val hitDefault = recommandPhotoNoF.join(hitTotalF)

        //推荐用户点击数recommandPhotoF 去找两个挨着的
        //       val join: RDD[(String, ((Int, Int), Int))] = recommandPhotoF.join(hitTotalF)
        //        println(join.count())

        val hitRecom = baseRDD.filter(_.pageid == "details_gallery").map(x => {
            val data = x.data
            val parser = new JsonParser
            val asJsonObject = parser.parse(data).getAsJsonObject
            try {
                if (asJsonObject != null) {
                    val algorithmic = asJsonObject.get("algorithmic").getAsString
                    (x.uuid, algorithmic)
                } else {
                    null
                }
            } catch {
                case e: Exception => null
            }
        }).filter(_ != null).filter(_._2 == "lfm_v1").distinct().groupBy(_._2).map(x => (x._1, x._2.size))

        //图片点击统计
        val hitRecomOrginData: RDD[((String, String, String, Int), Int)] = baseRDD.filter(_.pageid == "details_gallery")
                .filter(_.data.toString.contains("cfi_v1")).map(x => {
            val data = x.data
            val parser = new JsonParser
            val asJsonObject = parser.parse(data).getAsJsonObject
            try {
                if (asJsonObject != null) {
                    val algorithmic = asJsonObject.get("algorithmic").getAsString
                    val origin_type = asJsonObject.get("origin_item_type").getAsString
                    val origin_id = asJsonObject.get("origin_item_id").getAsString
                    val hit_id = asJsonObject.get("id").getAsString
                    val hit_type = 1
                    ((origin_type, origin_id, hit_id, hit_type), 1)
                } else {
                    null
                }
            } catch {
                case e: Exception => null
            }
        }).filter(_ != null).reduceByKey(_ + _)

        //案例
        //总的用户数
        val mainTotalFC = baseRDD.filter(_.pageid == "main_tab_case_list").map(x => (x.uuid, 1)).distinct()

        //photorecommand的数据
        val caseReco: RDD[(String, Int)] = sc.makeRDD(loadCase())
        //有推荐的用户
        val recommandCaseF = mainTotalFC.join(caseReco)
        //无推荐用户
        val recommandCaseNoF = mainTotalFC.leftOuterJoin(caseReco).filter(x => x._2._2 == None)

        //点击用户
        val hitTotalFC: RDD[(String, Int)] = baseRDD.groupBy(_.uuid).map(x => (x._1, x._2.toList.sortWith(_.time < _.time)))
                .map(x => {
                    var sb = new StringBuilder
                    x._2.foreach(b => sb.append(b.pageid))
                    (x._1, sb.toString())
                }).filter(_._2.contains("main_tab_case_listdetails_case")).map(x => (x._1, 1))

        //推荐用户点击数recommandPhotoF 去找两个挨着的
        val hitRecomC = baseRDD.filter(_.pageid == "details_case").map(x => {
            val data = x.data
            val parser = new JsonParser
            val asJsonObject = parser.parse(data).getAsJsonObject
            try {
                if (asJsonObject != null) {
                    val algorithmic = asJsonObject.get("algorithmic").getAsString
                    (x.uuid, algorithmic)
                } else {
                    null
                }
            } catch {
                case e: Exception => null
            }
        }).filter(_ != null).filter(_._2 == "lfm_v1").distinct().groupBy(_._2).map(x => (x._1, x._2.size))
        //默认用户点击数 newArrivalPhotoF 去找两个挨着的
        val hitDefaultC = recommandCaseNoF.join(hitTotalFC)

        val logtime = new java.sql.Date(todayAm)
        val addtime = new java.sql.Date(todayPm)
        var lfm_v1 = 0
        val photoRe = hitRecom.collect()
        for (re <- photoRe) {
            if ("lfm_v1" == re._1) {
                lfm_v1 = re._2
            }
        }
        var lfm_v1C = 0
        val photoReC = hitRecomC.collect()
        for (reC <- photoReC) {
            if ("lfm_v1" == reC._1) {
                lfm_v1C = reC._2
            }
        }




        JDBCBasicUtils.insert("sql_insert_recommand_everyday", psmt => {
            psmt.setLong(1, recommandPhotoF.count())
            psmt.setLong(2, recommandPhotoNoF.count())
            psmt.setLong(3, lfm_v1)
            psmt.setLong(4, hitDefault.count())
            psmt.setLong(5, recommandCaseF.count())
            psmt.setLong(6, recommandCaseNoF.count())
            psmt.setLong(7, lfm_v1C)
            psmt.setLong(8, hitDefaultC.count())
            psmt.setLong(9, hitTotalF.count())
            psmt.setString(9, "1200")
            psmt.setDate(10, logtime)
            psmt.setDate(11, addtime)
        })


        hitRecomOrginData.foreachPartition(iter => {
            JDBCBasicUtils.insertBatchByIterator[((String, String, String, Int), Int)]("sql_insert_orgin_hit_everyday", iter, (pstmt, b) => {
                pstmt.setString(1, b._1._1)
                pstmt.setString(2, b._1._2)
                pstmt.setString(3, b._1._4.toString)
                pstmt.setString(4, b._1._3)
                pstmt.setLong(5, b._2)
                pstmt.setDate(6, logtime)
                pstmt.setDate(7, addtime)
            })
        })

    }


}

object MobileJobRecommandStatisticCount {

    def main(args: Array[String]) {

        val job = new MobileJobRecommandStatisticCount()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
