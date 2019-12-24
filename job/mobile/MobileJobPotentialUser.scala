package com.wangjia.bigdata.core.job.mobile

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import com.google.gson.{JsonArray, JsonObject, JsonParser}
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{JAInfoUtils, JDBCBasicUtils}
import com.wangjia.es.{EsPageBean, EsPageQuery, EsTableName}
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.hbase.{HBaseConst, HBaseTableName}
import org.apache.hadoop.hbase.client.{Get, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.search.SearchHit

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Cfb on 2018/1/10.
  */
class MobileJobPotentialUser extends EveryDaySparkJob {
    private val sdf = new SimpleDateFormat("yyyyMMdd")
    private val oneDayTime: Long = 1000 * 60 * 60 * 24
    private val logPeriod = 92
    private val hoursPeriod: List[Int] = Array(0, 1, 2, 3, 4, 5, 6, 9, 10, 11, 14, 15, 16, 17, 23).toList
    private var firstStartTime: Long = 0
    private var clueRDD: RDD[(String, Int)] = null
    private var filterOldDevice: Broadcast[ListBuffer[String]] = null
    private val COMMA = ","
    private var addDayTime: Long = 0

    override protected def init(): Unit = {
        super.init()
        val sc = SparkExtend.ctx
        val broadcast = JAInfoUtils.loadUserClueDeviceIdList(this.logDate).filter(_._1.size < 30).map(_._1)
        filterOldDevice = sc.broadcast(broadcast)
        clueRDD = sc.makeRDD(JAInfoUtils.loadUserClueDeviceIdList(this.logDate).filter(_._1.size >= 30))
        addDayTime = this.logDate.getTime + oneDayTime
    }

    def filterClueUser(filterOldDevice: Broadcast[ListBuffer[String]])(mid: (String, Array[Long])): (String, Array[Long]) = {
        val deviceId = mid._1
        val value: ListBuffer[String] = filterOldDevice.value
        if (value.contains(deviceId) || value.contains(deviceId.split("#")(0))) {
            null
        } else {
            (deviceId, mid._2)
        }

    }


    //获取特定时间段
    def getHourAndDay(startTime: Long, cal: Calendar): (Boolean, Int) = {

        cal.setTimeInMillis(startTime)
        val hour: Int = cal.get(Calendar.HOUR)
        val flag = hoursPeriod.contains(hour)
        (flag, cal.get(Calendar.DAY_OF_YEAR))
    }

    //获取日志路径
    private def getLogPath(cal: Calendar): String = {
        val year = cal.get(Calendar.YEAR).toString
        var month = (cal.get(Calendar.MONTH) + 1).toString
        if (month.size == 1) {
            month = s"0$month"
        }
        var day = cal.get(Calendar.DAY_OF_MONTH).toString
        if (day.size == 1) {
            day = s"0$day"
        }
        val dataPath = s"pyear=$year/pmonth=$month/pdate=$day"
        dataPath
    }

    //获取92天的日志
    def getTotalRDD(sc: SparkContext): RDD[String] = {
        val callendar = Calendar.getInstance()
        callendar.setTimeInMillis(addDayTime)
        var totalRdd: RDD[String] = null
        //往后移动92天
        var total = logPeriod
        val sb = new StringBuilder
        while (total != 0) {
            total = total - 1
            callendar.add(Calendar.DATE, -1)
            val dataPath = getLogPath(callendar)
            sb.append(this.inPath.concat(dataPath))
            if (total != 0) {
                sb.append(COMMA)
            }
        }
        if (totalRdd == null) {
            totalRdd = sc.textFile(sb.toString())
        }
        firstStartTime = callendar.getTimeInMillis
        totalRdd
    }

    //獲取註冊用戶的日誌文件
    def getRegistRDD(sc: SparkContext): RDD[(String, String)] = {
        val regestRdd = sc.textFile(this.getParameterOrElse("regest", "")).map(x => (x, "1"))
        regestRdd
    }


    //判斷是否是潛客
    private def potentialJudge(result: RDD[(String, Array[Long])]): RDD[(String, Int)] = {
        val judeResult = result.map(x => {
            //筛选
            val firstVisitTime: Long = x._2(10)
            val collection: Long = x._2(0)
            val search: Long = x._2(1)
            val save: Long = x._2(2)
            val filter: Long = x._2(3)
            val share: Long = x._2(4)
            val register: Long = x._2(5)
            val pages: Long = x._2(6)
            val speciPage: Long = x._2(7)
            val visitDay: Long = x._2(8)
            val stayTime: Long = x._2(9)
            //第一次访问与跑批时间的间隔
            var interval: Long = (this.logDate.getTime - firstVisitTime) / oneDayTime
            interval = if (interval < 0) 0 else interval
            val sum = filter + search + save + collection + share + register
            val judge: Int = {
                if (interval <= 7) {
                    1
                } else if (interval > 7 && interval <= 31) {
                    if ((pages > 153 || speciPage >= 44 || ((stayTime * 1.0) >= 3)) && sum != 0 && visitDay >= 4) {
                        1
                    } else {
                        0
                    }
                } else if (interval > 31 && interval <= 92) {
                    if ((pages > 172 || speciPage >= 50 || ((stayTime * 1.0) / 1000 >= 4.2)) && sum != 0 && visitDay >= 5) {
                        1
                    } else {
                        0
                    }
                } else {
                    0
                }
            }
            //            (x._1, judge)
            (x._1, judge)
        })
        judeResult
    }

    //是否收藏，查询，保存，筛选，分享
    def map2Array(x: MobileLogMsg): (String, Array[Long]) = {
        val array = Array[Long](0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        if (x.subtype == "collection_case" || x.subtype == "collection_photo") {
            array(0) = 1
        } else if (x.subtype == "search_case" || x.subtype == "search_photo") {
            array(1) = 1
        } else if (x.subtype == "save_photo") {
            array(2) = 1
        } else if (x.subtype == "filter_photo_list" || x.subtype == "filter_case_list") {
            array(3) = 1
        } else if (x.subtype == "share_app" || x.subtype == "share_photo" || x.subtype == "share_case" || x.subtype == "share_post") {
            array(4) = 1
        }
        (x.dStrInfo("ja_uuid"), array)
    }

    def distinctDevice(x1: Array[Long], x2: Array[Long]): Array[Long] = {
        //            收藏
        val v1 = x2(0)
        //            //            搜索
        val v2 = x2(1)
        //            //            保存
        val v3 = x2(2)
        //            //            筛选
        val v4 = x2(3)
        //            //            分享
        val v5 = x2(4)
        if (v1 == 1) {
            x1(0) = v1
        }
        if (v2 == 1) {
            x1(1) = v2
        }
        if (v3 == 1) {
            x1(2) = v3
        }
        if (v4 == 1) {
            x1(3) = v4
        }
        if (v5 == 1) {
            x1(4) = v5
        }
        x1
    }

    override protected def job(args: Array[String]): Unit = {
        val sc = SparkExtend.ctx

        //获取了当前的时间
        val totalRdd = getTotalRDD(sc)
        //        val totalRdd: RDD[String] = sc.textFile("G:\\LOG\\wj-app\\2018\\01\\15")
        //device  andorid只有IMEI
        val regestRdd = getRegistRDD(sc)
        //        val regestRdd: RDD[(String,String)] = sc.textFile("D:\\login_user_deviceid_list").map(x => (x, "1"))
        //去重后的，已经赋值过array一遍的
        val reduceRDD = totalRdd.map(BeanHandler.toAppAcc).filter(_ != null).map(map2Array)
                .reduceByKey(distinctDevice)

        val midRDD = reduceRDD.leftOuterJoin(clueRDD).filter(_._2._2 == None).map(x => (x._1, x._2._1))
                .map(filterClueUser(filterOldDevice)).filter(_ != null)
        //        RDD[(String, Array[Int])]   clueRDD:RDD[(String, Int)]
        midRDD.persist(StorageLevel.MEMORY_AND_DISK)
        //因为注册用户中Android只有IMEI，所以拆开去匹配。
        val separRdd = midRDD.map(x => {
            if (x._1.contains("#")) {
                (x._1.split("#")(0), x)
            } else {
                (x._1, x)
            }
        })
        // 获取用户注册与否
        val joinRdd = separRdd.leftOuterJoin(regestRdd).map(x => if (x._2._2 != None) {
            x._2._1._2(5) = 1
            (x._2._1._1, x._2._1._2)
        } else {
            x._2._1._2(5) = 0
            (x._2._1._1, x._2._1._2)
        }
        )
        val lessRdd =joinRdd.repartition(200)

        //        joinRdd
        //        //从Es中根据deviceId拿到所有会话的id，然后去v5uuid2visit中拿到会话信息，获取访问页面，访问天数。
        //        //        val result: RDD[(String, mutable.Map[String, Long])] = midRDD.mapPartitions[(String, mutable.Map[String, Long])]
        val result = lessRdd.mapPartitions(iter => {
            val cal: Calendar = Calendar.getInstance()
            initHBase()
            val tbConn = new ExTBConnection
            val tbDes = tbConn.getTable(HBaseTableName.UUID_VISIT_DES).getTable
            val source: Array[String] = Array[String]("userids")
            val query: EsPageQuery[String] = new EsPageQuery[String] {
                override def builderObject(searchHit: SearchHit): String = {
                    searchHit.getId
                }
            }
            //每一个deviceId()  (FE3CE6DE-3E17-458C-A1FB-C4DA03910C3D,Map(筛选 -> 5, 保存 -> 3,页面q->2,))
            //x是一个iterator
            val buffer = new ListBuffer[(String, Array[Long])]()
            iter.foreach(x => {

                //                特定时间段内的访问页面数
                //                申请免费设计页（服务页）的访问时长
                //                访问天数
                val value = new util.HashMap[String, Object]()
                value.put("deviceid", x._1)
                val bean: EsPageBean[String] = query.getPageData(EsTableName.ES_INDEX_BIGDATA_VISIT, EsTableName.ES_TYPE_VISIT, 0, 1000, source, value)
                //deviceid对应的会话ids
                val ids = bean.getDatas.toArray

                val gets = new ListBuffer[Get]
                //服务页面访问停留时间
                var stayTimeSum: Long = 0
                var speciPage = 0
                var sumPages = 0

                //hbase gets
                ids.foreach(id => {
                    gets += (new Get(Bytes.toBytes(id.toString)))
                })
                //resultsDes
                val resultDes: Array[Result] = tbDes.get(gets)
                //resultArray的迭代index
                var j = 0
                val max = resultDes.length
                //每个Device拿到的 访问页面数，和特定时间段访问的页面数
                //获取访问天数
                val value1 = mutable.Set[Long]()
                //第一次访问的时间
                var firstVisitTime: Long = Long.MaxValue
                //一个deviceId的一个会话的开始
                while (j < max) {
                    val rs: Result = resultDes(j)
                    j += 1
                    //访问的页面数
                    //一个会话信息
                    val msg = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG))
                    //                                        println(x._1+Bytes.toString(rs.getRow)+"====="+msg)
                    try {
                        if (msg != null) {
                            val parser: JsonParser = new JsonParser()
                            val msgJson: JsonObject = parser.parse(msg).getAsJsonObject
                            val pReqs: JsonArray = msgJson.get("pReqs").getAsJsonArray
                            //访问页面数
                            val pages: Int = pReqs.size()
                            var i = 0
                            sumPages += pages
                            while (i < pages) {
                                val pageInfo: JsonObject = pReqs.get(i).getAsJsonObject
                                i += 1
                                if (pageInfo.get("title").getAsString == "服务页" || pageInfo.get("title").getAsString == "申请免费设计页") {
                                    val time: Long = pageInfo.get("time").getAsLong
                                    stayTimeSum += time
                                }
                                //页面打开时间
                                val startTime: Long = pageInfo.get("start").getAsLong

                                if (startTime >= firstStartTime && startTime < firstVisitTime) {
                                    firstVisitTime = startTime
                                }
                                val flag = getHourAndDay(startTime, cal)._1
                                val day = getHourAndDay(startTime, cal)._2
                                if (flag) {
                                    speciPage += 1
                                }
                                value1.add(day)
                            }
                        }
                    } catch {
                        case e: Exception => {
                            e.printStackTrace()
                        }
                    }
                }


                //                页面
                x._2(6) = sumPages
                //                特定页面
                x._2(7) = speciPage
                //                访问天数
                x._2(8) = value1.size
                //                服务页时长
                x._2(9) = stayTimeSum
                x._2(10) = firstVisitTime

                buffer += ((x._1, x._2))
            })
//            tbConn.close()
            //            query.close()
            buffer.toIterator

        })
        val judge: RDD[(String, Int)] = potentialJudge(result)
        judge.persist(StorageLevel.MEMORY_AND_DISK)

        judge.foreachPartition(iterator => {
            val date = new java.sql.Date(addDayTime)
            val logtime = new java.sql.Date(logDate.getTime)
            JDBCBasicUtils.insertBatchByIterator[(String, Int)]("sql_insert_potential_user", iterator, (pstmt, x) => {
                pstmt.setString(1, x._1)
                pstmt.setInt(2, x._2)
                pstmt.setDate(3, date)
                pstmt.setDate(4, logtime)
            })
        })
        midRDD.unpersist()
        judge.unpersist()

    }


}

object MobileJobPotentialUser {

    def main(args: Array[String]) {

        val job = new MobileJobPotentialUser()
        job.run(args)

        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
