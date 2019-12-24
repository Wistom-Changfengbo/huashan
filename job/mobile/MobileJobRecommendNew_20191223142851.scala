package com.wangjia.bigdata.core.job.mobile

import java.io.{ByteArrayOutputStream, DataOutputStream}

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.wangjia.bigdata.core.bean.info.{AppInfo, IpAddress}
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{IPUtils, JAInfoUtils}
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.hbase.{HBaseConst, HBaseTableName}
import com.wangjia.utils.{HBaseUtils, JavaUtils, MD5Utils}
import org.apache.hadoop.hbase.client.{Get, Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * Created by Administrator on 2018/7/9.
  */
class MobileJobRecommendNew extends EveryDaySparkJob {

  case class NewRecommendItem(uuid: String, itemId: Int, itemType: Int, score: Float)

  case class IDRelation(uuid: String, userids: ListBuffer[(Int, String)], ips: mutable.Map[String, Int], appids: mutable.Set[String], lastTime: Long)

  //图片topn
  val RECOMMEND_PHOTO_TOP_N: Int = 250
  //案例topn
  val RECOMMEND_CASE_TOP_N: Int = 125

  //推送日期范围
  val PUSH_DATE_RANGE: Int = 7

  //应用信息
  private var mapAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null

  override protected def init(): Unit = {
    super.init()
    val sc = SparkExtend.ctx
    //加载广播应用信息
    val mapAppInfos = JAInfoUtils.loadAppInfos
    mapAppInfosBroadcast = sc.broadcast(mapAppInfos)
  }

  /**
    * 计算推荐
    *
    * @param uuid
    * @param iter
    * @param rs
    * @return
    */
  def computeRecommendItem(uuid: String, iter: Iterable[NewRecommendItem], rs: Result): (String, (ListBuffer[NewRecommendItem], ListBuffer[NewRecommendItem], Int)) = {
    val result1 = new ListBuffer[NewRecommendItem]
    val result2 = new ListBuffer[NewRecommendItem]
    var sumScore1: Float = 0F
    var sumScore2: Float = 0F
    var num1: Int = 0
    var num2: Int = 0
    var bContinue: Boolean = true
    val list = new ListBuffer[NewRecommendItem]
    list.addAll(iter.toList.filter(_.score > 0).sortWith(_.score > _.score))

    while (list.nonEmpty && bContinue) {
      val item: NewRecommendItem = list.remove(0)
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
      (uuid, (result1, result2, 1))
    else
      (uuid, (result1, result2, 2))
  }

  def loadItemBeanRdd(inPath: String): RDD[NewRecommendItem] = {
    SparkExtend.ctx.textFile(this.inPath).map { case (line) =>
      try {
        val fields: Array[String] = line.split('\t')
        NewRecommendItem(fields(0), fields(1).toInt, fields(2).toInt, fields(3).toFloat)
      } catch {
        case e: Exception => e.printStackTrace(); null
      }
    }.filter(_ != null)
  }

  def loaduuidAndUserIds(inPath: String): RDD[IDRelation] = {
    SparkExtend.ctx.textFile(inPath)
      .map(BeanHandler.toAppAcc)
      .filter(_ != null)
      .groupBy(_.uuid)
      .map { case (uuid, iter) => {
        var lastTime: Long = 0
        val userids: ListBuffer[(Int, String)] = new ListBuffer[(Int, String)]
        val ips = mutable.Map[String, Int]()
        val appids = mutable.Set[String]()
        val iterator = iter.toIterator
        var msg: MobileLogMsg = null
        while (iterator.hasNext) {
          msg = iterator.next()
          if (msg.time > lastTime)
            lastTime = msg.time
          if (!msg.userid.isEmpty) {
            val appinfo = mapAppInfosBroadcast.value.getOrElse(msg.appid, null)
            if (appinfo != null && appinfo.accountId != 0) {
              userids += ((appinfo.accountId, msg.userid))
              if (userids.size > 1000)
                userids.distinct
            }
          }
          ips.put(msg.ip, ips.getOrElse(msg.ip, 0) + 1)
          appids.add(msg.appid)
        }
        IDRelation(uuid, userids.distinct, ips, appids, lastTime)
      }
      }
  }

  def toPute(it: (String, List[(Int, Float)], List[(Int, Float)])): Put = {
    val arry = new JSONArray()
    val uuid: String = it._1
    val put = new Put(Bytes.toBytes(uuid))
    //photo
    arry.clear()
    val photoTopN = it._2
    if (photoTopN.nonEmpty) {
      val photoBf = new ByteArrayOutputStream
      val photoDos = new DataOutputStream(photoBf)
      photoTopN.foreach(item => {
        val obj = new JSONObject()
        obj.put("id", item._1)
        obj.put("score", item._2)
        arry.add(obj)
        photoDos.writeInt(item._1)
      })
      photoDos.flush()
      photoBf.flush()
      put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("photo"), photoBf.toByteArray)
      put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("photo-des"), Bytes.toBytes(arry.toJSONString))
      photoDos.close()
      photoBf.close()
    }
    //case
    arry.clear()
    val caseTopN = it._3
    if (caseTopN.nonEmpty) {
      val caseBf = new ByteArrayOutputStream
      val caseDos = new DataOutputStream(caseBf)
      caseTopN.foreach(item => {
        val obj = new JSONObject()
        obj.put("id", item._1)
        obj.put("score", item._2)
        arry.add(obj)
        caseDos.writeInt(item._1)
      })
      caseDos.flush()
      caseBf.flush()
      put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("case"), caseBf.toByteArray)
      put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("case-des"), Bytes.toBytes(arry.toJSONString))
      caseDos.close()
      caseBf.close()
    }
    put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_LASTTIME, Bytes.toBytes(this.logDate.getTime))
    //photo
    //photo-des
    //case
    //case-des
    //lasttime
    put
  }

  def recommendByUUID(topNRdd: RDD[(String, (ListBuffer[NewRecommendItem], ListBuffer[NewRecommendItem], Int))],
                      idsRdd: RDD[IDRelation]): Unit = {
    val minTime: Long = System.currentTimeMillis() - 15 * 24 * 3600000L
    val rdd = idsRdd.filter(_.lastTime > minTime)
      .map(x => (x.uuid, 1))
      .join(topNRdd)
      .map(x => (x._1, x._2._2._1.map(i => (i.itemId, i.score)).toList, x._2._2._2.map(i => (i.itemId, i.score)).toList))
    rdd.foreachPartition(iter => save2HBase[(String, List[(Int, Float)], List[(Int, Float)])](HBaseTableName.RECOMMEND_PHOTO_CASE, iter, toPute))
    rdd.map(_._1).foreachPartition(iter => save2HBase[String](HBaseTableName.RECOMMEND_PHOTO_CASE, iter, it => {
      val put = new Put(Bytes.toBytes(it))
      put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("re_type"), Bytes.toBytes("uuid"))
      put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_UUID, Bytes.toBytes(it))
      put
    }))
  }

  def recommendByUserId(topNRdd: RDD[(String, (ListBuffer[NewRecommendItem], ListBuffer[NewRecommendItem], Int))],
                        idsRdd: RDD[IDRelation]): Unit = {
    val rdd = idsRdd.filter(_.userids.nonEmpty)
      .map(x => (x.uuid, x.userids))
      .join(topNRdd)
      .flatMap(x => {
        val list = new ListBuffer[((Int, String), (ListBuffer[NewRecommendItem], ListBuffer[NewRecommendItem]))]
        x._2._1.foreach(user => list += ((user, (x._2._2._1, x._2._2._2))))
        list
      })
      .groupByKey()
      .map[(String, (Int, String), List[(Int, Float)], List[(Int, Float)])](x => {
      val accountId = x._1._1
      val userid = x._1._2
      val photoCache = new ListBuffer[(Int, Float)]()
      val caseCache = new ListBuffer[(Int, Float)]()
      x._2.foreach(photoAndCase => {
        photoCache ++= photoAndCase._1.map(i => (i.itemId, i.score))
        caseCache ++= photoAndCase._2.map(i => (i.itemId, i.score))
      })
      val photoList = photoCache.groupBy(_._1).map(i => (i._1, i._2.map(_._2).max)).toList.sortWith(_._2 > _._2)
      val caseList = caseCache.groupBy(_._1).map(i => (i._1, i._2.map(_._2).max)).toList.sortWith(_._2 > _._2)

      val sb = new java.lang.StringBuilder()
      sb.append(userid)
      sb.reverse()
      sb.append('#')
      sb.append(accountId)
      (MD5Utils.MD5(sb.toString), (accountId, userid), if (photoList.size > RECOMMEND_PHOTO_TOP_N) photoList.subList(0, RECOMMEND_PHOTO_TOP_N).toList else photoList,
        if (caseList.size > RECOMMEND_CASE_TOP_N) caseList.subList(0, RECOMMEND_CASE_TOP_N).toList else caseList)
    })
    rdd.map(x => (x._1, x._3, x._4)).foreachPartition(iter => save2HBase[(String, List[(Int, Float)], List[(Int, Float)])](HBaseTableName.RECOMMEND_PHOTO_CASE, iter, toPute))
    rdd.map(x => (x._1, x._2)).foreachPartition(iter => save2HBase[(String, (Int, String))](HBaseTableName.RECOMMEND_PHOTO_CASE, iter, it => {
      val put = new Put(Bytes.toBytes(it._1))
      put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("re_type"), Bytes.toBytes("userid"))
      put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_ACCOUNTID, Bytes.toBytes(it._2._1))
      put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_USERID, Bytes.toBytes(it._2._2))
      put
    }))
  }

  def recommendByArea(topNRdd: RDD[(String, (ListBuffer[NewRecommendItem], ListBuffer[NewRecommendItem], Int))],
                      idsRdd: RDD[IDRelation]): Unit = {
    val ipAddressRdd = idsRdd.map(x => {
      var maxNum: Int = Integer.MIN_VALUE
      var maxIp: String = null
      x.ips.foreach(item => {
        if (item._2 > maxNum) {
          maxNum = item._2
          maxIp = item._1
        }
      })
      val ipAddress = IPUtils.getAddress(maxIp)
      if (ipAddress != null && ipAddress != IpAddress.NULL) {
        val str = ipAddress.province + "@" + ipAddress.city
        (x.uuid, str, MD5Utils.MD5(str))
      } else
        null
    }).filter(_ != null)

    val rdd = ipAddressRdd.map(x => (x._1, x._3))
      .join(topNRdd)
      .flatMap(x => {
        val address = x._2._1
        //((地址,物品类型,物品ID),得分)
        val list = new ListBuffer[((String, Int, Int), Float)]
        x._2._2._1.foreach(i => list += (((address, i.itemType, i.itemId), i.score)))
        x._2._2._2.foreach(i => list += (((address, i.itemType, i.itemId), i.score)))
        list
      })
      .groupByKey()
      .map(x => {
        val list = x._2.toList
        (x._1._1, (x._1._2, x._1._3, list.sum / list.size))
      })
      .groupByKey()
      .map(x => {
        val list = x._2.toList
        val photoList = list.filter(_._1 == 1).map(x => (x._2, x._3)).sortWith(_._2 > _._2)
        val caseList = list.filter(_._1 == 2).map(x => (x._2, x._3)).sortWith(_._2 > _._2)
        (x._1, if (photoList.size > RECOMMEND_PHOTO_TOP_N) photoList.subList(0, RECOMMEND_PHOTO_TOP_N).toList else photoList,
          if (caseList.size > RECOMMEND_CASE_TOP_N) caseList.subList(0, RECOMMEND_CASE_TOP_N).toList else caseList)
      })

    rdd.foreachPartition(iter => save2HBase[(String, List[(Int, Float)], List[(Int, Float)])](HBaseTableName.RECOMMEND_PHOTO_CASE, iter, toPute))
    ipAddressRdd.map(x => (x._3, x._2))
      .distinct()
      .foreachPartition(iter => save2HBase[(String, String)](HBaseTableName.RECOMMEND_PHOTO_CASE, iter, it => {
        val put = new Put(Bytes.toBytes(it._1))
        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("re_type"), Bytes.toBytes("area"))
        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("area"), Bytes.toBytes(it._2))
        put
      }))
  }

  def recommendByPush(topNRdd: RDD[(String, (ListBuffer[NewRecommendItem], ListBuffer[NewRecommendItem], Int))],
                      idsRdd: RDD[IDRelation]): Unit = {
    val uuid2appidRdd = idsRdd.flatMap(x => {
      val list = new ListBuffer[(String, String)]
      x.appids.foreach(appid => list += ((x.uuid, appid)))
      list
    })
    val uuid2typeRdd = topNRdd.map(x => (x._1, x._2._3))

    val rdd = uuid2appidRdd.join(uuid2typeRdd).map(x => {
      val uuid = x._1
      val appid = x._2._1
      val _type = x._2._2

      val sb = new mutable.StringBuilder()
      sb.append(appid)
      sb.append(new mutable.StringBuilder(JavaUtils.timeMillis2DayNum(this.logDate.getTime).toHexString).reverse.toString())
      sb.append(uuid)

      (sb.toString(), appid, uuid, _type)
    })

    rdd.foreachPartition(iter => save2HBase[(String, String, String, Int)](HBaseTableName.RECOMMEND_PUSH, iter, it => {
      val put = new Put(Bytes.toBytes(it._1))
      put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_APPID, Bytes.toBytes(it._2))
      put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_UUID, Bytes.toBytes(it._3))
      put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("re_type"), Bytes.toBytes(it._4))
      put
    }))
  }

  override protected def job(args: Array[String]): Unit = {
    val pushpaths = getParameterOrElse("pushpaths", null)
    if (pushpaths == null)
      throw new RuntimeException("pushpaths is null")

    HBaseUtils.createTable(HBaseTableName.UUID_ITEM_STAR)
//    HBaseUtils.createTable(HBaseTableName.RECOMMEND_PHOTO_CASE, 30 * 24 * 3600)
//    HBaseUtils.createTable(HBaseTableName.RECOMMEND_PUSH, 7 * 24 * 3600)

    val uuid2ItemBeanRdd = loadItemBeanRdd(this.inPath).groupBy(_.uuid)

    val uuid2TopNRdd = uuid2ItemBeanRdd.mapPartitions(iteraor => {
      initHBase()
      val conn = new ExTBConnection
      val tbStar = conn.getTable(HBaseTableName.UUID_ITEM_STAR).getTable
      val list = new ListBuffer[(String, (ListBuffer[NewRecommendItem], ListBuffer[NewRecommendItem], Int))]
      val cache = new ListBuffer[(String, Iterable[NewRecommendItem])]
      val gets = new ListBuffer[Get]
      val func = () => {
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
      iteraor.foreach(x => {
        cache += x
        gets += new Get(Bytes.toBytes(x._1))
        if (gets.length > Config.BATCH_SIZE) {
          func()
        }
      })
      if (gets.nonEmpty) {
        func()
      }
      conn.close()
      list.toIterator
    })
    uuid2TopNRdd.cache()
    val idsRdd = loaduuidAndUserIds(pushpaths)
    idsRdd.cache()

    recommendByUUID(uuid2TopNRdd, idsRdd)
    recommendByUserId(uuid2TopNRdd, idsRdd)
    recommendByArea(uuid2TopNRdd, idsRdd)
    recommendByPush(uuid2TopNRdd, idsRdd)
  }
}

object MobileJobRecommendNew {
  def main(args: Array[String]) {
    val job = new MobileJobRecommendNew()
    job.run(args)
    println(System.currentTimeMillis() - job.jobStartTime)
  }
}