package com.wangjia.bigdata.core.job.mobile

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Properties

import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.JDBCBasicUtils
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.hbase.{HBaseConst, HBaseTableName}
import com.wangjia.utils.RedisUtils
import org.apache.hadoop.hbase.client.{Get, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, _}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

case class RecommendItem(uuid: String, itemId: Int, itemType: Int, score: Float)

/**
  * Created by Administrator on 2017/4/26.
  */
class MobileJobRecommend extends EveryDaySparkJob {

    //图片topn
    val RECOMMEND_PHOTO_TOP_N: Int = 250
    //案例topn
    val RECOMMEND_CASE_TOP_N: Int = 125
    val IS_OUTPUT_ONLINE: Boolean = false

    //推送日期范围
    val PUSH_DATE_RANGE: Int = 7

    /**
      * 计算推荐
      *
      * @param uuid
      * @param iter
      * @param rs
      * @return
      */
    def computeRecommendItem(uuid: String, iter: Iterable[RecommendItem], rs: Result): (String, ListBuffer[RecommendItem], ListBuffer[RecommendItem], Int) = {
        val result1 = new ListBuffer[RecommendItem]
        val result2 = new ListBuffer[RecommendItem]
        var sumScore1: Float = 0F
        var sumScore2: Float = 0F
        var num1: Int = 0
        var num2: Int = 0
        var bContinue: Boolean = true
        val list = new ListBuffer[RecommendItem]
        list.addAll(iter.toList.filter(_.score > 0).sortWith(_.score > _.score))

        while (list.nonEmpty && bContinue) {
            val item: RecommendItem = list.remove(0)
            val key: Array[Byte] = Bytes.toBytes(item.itemType + "#" + item.itemId)
            //取TOPN,去除有特殊事件的Item
            if (item.itemType == 1) {
                if (result1.length < RECOMMEND_PHOTO_TOP_N &&
                        (!rs.containsColumn(HBaseConst.BYTES_CF1, key)
                                || Bytes.toLong(rs.getValue(HBaseConst.BYTES_CF1, key)) == 0)) {
                    sumScore1 += item.score
                    num1 += 1
                    result1 += item
                }
            } else if (item.itemType == 2) {
                if (result2.length < RECOMMEND_CASE_TOP_N &&
                        (!rs.containsColumn(HBaseConst.BYTES_CF1, key)
                                || Bytes.toLong(rs.getValue(HBaseConst.BYTES_CF1, key)) == 0)) {
                    sumScore2 += item.score
                    num2 += 1
                    result2 += item
                }
            }
            if (result1.length >= RECOMMEND_PHOTO_TOP_N && result2.length >= RECOMMEND_CASE_TOP_N)
                bContinue = false
        }

        val score1 = if (num1 == 0) 0F else sumScore1 / num1
        val score2 = if (num2 == 0) 0F else sumScore2 / num2
        if (score1 > score2)
            (uuid, result1, result2, 1)
        else
            (uuid, result1, result2, 2)
    }

    /**
      * 写入Mysql
      *
      * @param url
      * @param username
      * @param password
      * @param tableName
      * @param tableSql
      * @param dataFrame
      * @return
      */
    def saveToMysql(url: String, username: String, password: String, tableName: String, tableSql: String, dataFrame: DataFrame): Unit = {
        try {
            //创建表
            val conn: Connection = JDBCBasicUtils.getConn(url, username, password)
            val ps: PreparedStatement = conn.prepareStatement(tableSql)
            ps.executeUpdate(tableSql)
            ps.close()
            conn.close()

            //写入数据
            val prop = new Properties
            prop.setProperty("user", username)
            prop.setProperty("password", password)
            dataFrame.write.mode(SaveMode.Append).jdbc(url, tableName, prop)
        } catch {
            case e: Exception => e.printStackTrace();
        }
    }

    /**
      *
      * @param url
      * @param username
      * @param password
      * @param tableName
      * @return
      */
    def getTableCount(url: String, username: String, password: String, tableName: String): Int = {
        var conn: Connection = null
        var ps: PreparedStatement = null
        val sql: String = "select count(*) from " + tableName
        try {
            conn = JDBCBasicUtils.getConn(url, username, password)
            ps = conn.prepareStatement(sql)
            val rs: ResultSet = ps.executeQuery()
            if (rs.next()) {
                return rs.getInt(1)
            }
        } catch {
            case e: Exception => e.printStackTrace(); return -1
        } finally {
            if (ps != null)
                ps.close()
            if (conn != null)
                conn.close()
        }
        0
    }

    override protected def job(args: Array[String]): Unit = {
        val sc = SparkExtend.ctx
        val linesRdd = sc.textFile(this.inPath)

        val beanRdd = linesRdd.map(line => {
            try {
                val fields: Array[String] = line.split('\t')
                RecommendItem(fields(0), fields(1).toInt, fields(2).toInt, fields(3).toFloat)
            } catch {
                case e: Exception => e.printStackTrace(); null
            }
        }).filter(_ != null)

        val uuid2BeanRdd = beanRdd.groupBy(_.uuid)

        val resultRdd = uuid2BeanRdd.mapPartitions(iteraor => {
            initHBase()
            val conn = new ExTBConnection
            val tbStar = conn.getTable(HBaseTableName.UUID_ITEM_STAR).getTable
            val list = new ListBuffer[(String, ListBuffer[RecommendItem], ListBuffer[RecommendItem], Int)]
            val cache = new ListBuffer[(String, Iterable[RecommendItem])]
            val gets = new ListBuffer[Get]

            iteraor.foreach(x => {
                cache += x
                gets += new Get(Bytes.toBytes(x._1))
                if (gets.length > Config.BATCH_SIZE) {
                    val rsStar: Array[Result] = tbStar.get(gets)
                    var i = 0
                    val max = rsStar.length
                    while (i < max) {
                        val bean = cache(i)
                        list += computeRecommendItem(bean._1, bean._2, rsStar(i))
                        i += 1
                    }
                    gets.clear()
                    cache.clear()
                }
            })

            if (gets.nonEmpty) {
                val rsStar: Array[Result] = tbStar.get(gets)
                var i = 0
                val max = rsStar.length
                while (i < max) {
                    val bean = cache(i)
                    list += computeRecommendItem(bean._1, bean._2, rsStar(i))
                    i += 1
                }
                gets.clear()
                cache.clear()
            }

            conn.close()
            list.toIterator
        })

        val sdf = new SimpleDateFormat("yyyyMMdd")
        val format: String = sdf.format(this.logDate)
        val tableNameRecommendType = s"ja_data_recommend_type_$format"

        //推荐类型
        val createSqlRecommendType =
            s"""
               |CREATE TABLE IF NOT EXISTS `$tableNameRecommendType` (
               |  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'ID',
               |  `uuid` char(32) DEFAULT NULL,
               |  `retype` int(11) DEFAULT NULL,
               |  PRIMARY KEY (`id`),
               |  KEY `uuid` (`uuid`)
               |) ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;
            """.stripMargin
        //推荐图片
        val tableNameRecommendItemPhoto = s"ja_data_recommend_photo_$format"
        val createSqlRecommendItemPhoto =
            s"""
               |CREATE TABLE IF NOT EXISTS `$tableNameRecommendItemPhoto` (
               |  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
               |  `uuid` char(32) NOT NULL,
               |  `photo_id` int(11) NOT NULL COMMENT '图片ID',
               |  `score` int(10) NOT NULL DEFAULT '0' COMMENT '分数',
               |  PRIMARY KEY (`id`),
               |  KEY `uuid` (`uuid`,`score`)
               |) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='推荐图片';
            """.stripMargin
        //推荐案例
        val tableNameRecommendItemCase = s"ja_data_recommend_case_$format"
        val createSqlRecommendItemCase =
            s"""
               |CREATE TABLE IF NOT EXISTS `$tableNameRecommendItemCase` (
               |  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
               |  `uuid` char(32) NOT NULL,
               |  `case_id` bigint(20) NOT NULL COMMENT '案例ID',
               |  `score` int(10) NOT NULL DEFAULT '0' COMMENT '分数',
               |  PRIMARY KEY (`id`),
               |  KEY `uuid` (`uuid`,`score`)
               |) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='推荐案例';
            """.stripMargin

        val spark = SparkSession.builder().appName(this.jobName).getOrCreate()
        import spark.implicits._

        val bigdata_db_url: String = JDBCBasicUtils.getProperty("recommend.url")
        val bigdata_db_user: String = JDBCBasicUtils.getProperty("recommend.username")
        val bigdata_db_pwd: String = JDBCBasicUtils.getProperty("recommend.password")


        //输出推荐类型
        val schema = StructType(List(
            StructField("uuid", StringType),
            StructField("retype", IntegerType)
        ))
        val rowRDD = resultRdd.map(bean => {
            Row(bean._1, bean._4)
        })
        val recommendTypeDf = spark.createDataFrame(rowRDD, schema)
        saveToMysql(bigdata_db_url, bigdata_db_user, bigdata_db_pwd, tableNameRecommendType, createSqlRecommendType, recommendTypeDf)

        //输出推荐数据
        val beanDF = resultRdd.flatMap(x => x._2 ++ x._3).toDF
        beanDF.cache()
        beanDF.createOrReplaceTempView("t_recommend_item")
        val sql_recommend_item_photo_event =
            """select uuid,
              | itemId as photo_id,
              | int(score * 100000) as score
              | from t_recommend_item where itemType = 1 """.stripMargin

        val recommendItemPhotoDf: DataFrame = spark.sql(sql_recommend_item_photo_event)
        saveToMysql(bigdata_db_url, bigdata_db_user, bigdata_db_pwd, tableNameRecommendItemPhoto, createSqlRecommendItemPhoto, recommendItemPhotoDf)

        val sql_recommend_item_case_event =
            """select uuid,
              | itemId as case_id,
              | int(score * 100000) as score
              | from t_recommend_item where itemType = 2 """.stripMargin

        val recommendItemCaseDf: DataFrame = spark.sql(sql_recommend_item_case_event)
        saveToMysql(bigdata_db_url, bigdata_db_user, bigdata_db_pwd, tableNameRecommendItemCase, createSqlRecommendItemCase, recommendItemCaseDf)

        //输出推送数据
        var pushRdd: RDD[(String, String)] = null
        val pushpaths = getParameterOrElse("pushpaths", null)
        if (pushpaths == null) {
            spark.stop()
            return
        }
        val paths = pushpaths.split(',')
        paths.foreach(p => {
            val rdd = sc.textFile(p)
                    .map(BeanHandler.toAppAcc)
                    .filter(_ != null)
                    .map(x => (x.appid, x.uuid))
                    .distinct()
            if (pushRdd == null)
                pushRdd = rdd
            else
                pushRdd = pushRdd.union(rdd).distinct()
        })


        //输出推送数据
        val schemaPush = StructType(List(
            StructField("appid", StringType),
            StructField("uuid", StringType)
        ))
        val rowPushRDD = pushRdd.map(bean => {
            Row(bean._1, bean._2)
        })
        val pushDf: DataFrame = spark.createDataFrame(rowPushRDD, schemaPush).toDF()
        pushDf.createOrReplaceTempView("t_all_recently_uuid")
        val sql_push_uuid =
            """select A.appid as appid,
              | A.uuid as uuid,
              | B.itemType as push_type
              | from
              | (select * from t_all_recently_uuid) as A
              | JOIN
              | (select uuid,itemType from t_recommend_item group by uuid,itemType) as B
              | ON
              | A.uuid=B.uuid""".stripMargin
        val pushUUIDDf: DataFrame = spark.sql(sql_push_uuid)

        //推送用户
        val tableNamePushUUID = s"ja_data_push_uuid_$format"
        val createSqlPushUUID =
            s"""
               |CREATE TABLE IF NOT EXISTS `$tableNamePushUUID` (
               |  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
               |  `appid` char(16) NOT NULL,
               |  `uuid` char(32) NOT NULL,
               |  `push_type` int(10) NOT NULL DEFAULT '0' COMMENT '1图片 2案例',
               |  PRIMARY KEY (`id`),
               |  KEY `appid` (`appid`),
               |  KEY `uuid` (`uuid`)
               |) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='推送用户';
            """.stripMargin
        saveToMysql(bigdata_db_url, bigdata_db_user, bigdata_db_pwd, tableNamePushUUID, createSqlPushUUID, pushUUIDDf)

        if (!IS_OUTPUT_ONLINE) {
            spark.stop()
            return
        }

        val online_db_url: String = JDBCBasicUtils.getProperty("online.recommend.url")
        val online_db_user: String = JDBCBasicUtils.getProperty("online.recommend.username")
        val online_db_pwd: String = JDBCBasicUtils.getProperty("online.recommend.password")
        saveToMysql(online_db_url, online_db_user, online_db_pwd, tableNameRecommendType, createSqlRecommendType, recommendTypeDf)
        saveToMysql(online_db_url, online_db_user, online_db_pwd, tableNameRecommendItemPhoto, createSqlRecommendItemPhoto, recommendItemPhotoDf)
        saveToMysql(online_db_url, online_db_user, online_db_pwd, tableNameRecommendItemCase, createSqlRecommendItemCase, recommendItemCaseDf)

        val jedisConn: Jedis = RedisUtils.getPConn(JDBCBasicUtils.getProperty("online.redis.url"),
            JDBCBasicUtils.getProperty("online.redis.port").toInt,
            JDBCBasicUtils.getProperty("online.redis.index").toInt)
        val typeCount: Int = getTableCount(bigdata_db_url, bigdata_db_user, bigdata_db_pwd, tableNameRecommendType)
        val itemPhotoCount: Int = getTableCount(bigdata_db_url, bigdata_db_user, bigdata_db_pwd, tableNameRecommendItemPhoto)
        val itemCaseCount: Int = getTableCount(bigdata_db_url, bigdata_db_user, bigdata_db_pwd, tableNameRecommendItemCase)

        if (typeCount >= 7893) {
            jedisConn.set("RECOMMEND_TYPE_TABLE_NAME", tableNameRecommendType)
            jedisConn.set("RECOMMEND_TYPE_TABLE_COUNT", typeCount.toString)
            jedisConn.set("RECOMMEND_TYPE_TABLE_UPDATE_TIME", System.currentTimeMillis().toString)
        }
        if (itemPhotoCount >= 298950) {
            jedisConn.set("RECOMMEND_ITEM_PHOTO_TABLE_NAME", tableNameRecommendItemPhoto)
            jedisConn.set("RECOMMEND_ITEM_PHOTO_TABLE_COUNT", itemPhotoCount.toString)
            jedisConn.set("RECOMMEND_ITEM_PHOTO_TABLE_UPDATE_TIME", System.currentTimeMillis().toString)
        }
        if (itemCaseCount >= 248970) {
            jedisConn.set("RECOMMEND_ITEM_CASE_TABLE_NAME", tableNameRecommendItemCase)
            jedisConn.set("RECOMMEND_ITEM_CASE_TABLE_COUNT", itemCaseCount.toString)
            jedisConn.set("RECOMMEND_ITEM_CASE_TABLE_UPDATE_TIME", System.currentTimeMillis().toString)
        }

        jedisConn.close()
        spark.stop()
    }
}

object MobileJobRecommend {
    def main(args: Array[String]) {
        val job = new MobileJobRecommend()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
