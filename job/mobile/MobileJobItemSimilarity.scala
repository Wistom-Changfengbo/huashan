package com.wangjia.bigdata.core.job.mobile

import java.text.SimpleDateFormat
import java.util.Locale

import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, StringBuilder}


/**
  * Created by Administrator on 2018/2/28.
  */
class MobileJobItemSimilarity extends EveryDaySparkJob {

    /**
      *
      * @param uuidIndex
      * @param itemidIndex
      * @param score
      * @param uuid
      * @param itemid
      * @param itemType
      */
    case class ItemScore(uuidIndex: Int, itemidIndex: Int, var score: Float, uuid: String, itemid: Int, itemType: Int)


    /**
      * 计算相似度
      *
      * @param itemid1
      * @param mapScore1
      * @param itemid2
      * @param mapScore2
      * @return
      */
    def computeItemPearsonDis(itemid1: Int, mapScore1: mutable.Map[Int, Float], itemid2: Int, mapScore2: mutable.Map[Int, Float]): (Int, Int, Double) = {
        val x_itemid = itemid1
        val x_mapScore = mapScore1
        val y_itemid = itemid2
        val y_mapScore = mapScore2

        var sumXY: Float = 0
        var sumX: Float = 0
        var sumY: Float = 0
        var sumSqrtX: Float = 0
        var sumSqrtY: Float = 0
        var n: Int = 0

        x_mapScore.foreach(kv => {
            val x = kv._2
            val y = y_mapScore.getOrElse(kv._1, 0F)
            if (y != 0) {
                sumXY += x * y
                sumX += x
                sumY += y
                sumSqrtX += x * x
                sumSqrtY += y * y
                n += 1
            }
        })

        if (n == 0)
            return (x_itemid, y_itemid, 0)
        val denominator: Double = Math.sqrt(sumSqrtX - Math.pow(sumX, 2) / n) * Math.sqrt(sumSqrtY - Math.pow(sumY, 2) / n)
        if (denominator.isNaN || Math.abs(denominator) <= 0.0001)
            return (x_itemid, y_itemid, 0)
        (x_itemid, y_itemid, (sumXY - (sumX * sumY) / n) / denominator)
    }

    /**
      * 计算相似度
      *
      * @param itemid1
      * @param mapScore1
      * @param itemid2
      * @param mapScore2
      * @return
      */
    def computeItemSimilarly(itemid1: Int, mapScore1: mutable.Map[Int, Float], itemid2: Int, mapScore2: mutable.Map[Int, Float]): (Int, Int, Double) = {
        val x_itemid = itemid1
        val x_mapScore = mapScore1
        val y_itemid = itemid2
        val y_mapScore = mapScore2

        var sumXY: Float = 0
        var sumSqrtX: Float = 0
        var sumSqrtY: Float = 0

        x_mapScore.foreach(kv => {
            val x = kv._2
            sumSqrtX += x * x
            val y = y_mapScore.getOrElse(kv._1, 0F)
            if (y != 0) {
                sumXY += x * y
            }
        })
        y_mapScore.foreach(kv => sumSqrtY += kv._2 * kv._2)

        (x_itemid, y_itemid, sumXY / math.sqrt(sumSqrtX * sumSqrtY))
    }

    /**
      * 计算TOPN
      *
      * @param iter
      * @return
      */
    def computeSimilarlyTopN(iter: Iterable[(Int, Int, Double)]): ListBuffer[(Int, Int, Double)] = {
        val list = new ListBuffer[(Int, Int, Double)]
        try {
            iter.foreach(item => {
                if (item._3 >= 0.5)
                    list += item
            })
        } catch {
            case e: Exception => e.printStackTrace(); println(iter)
        }
        list
    }

    override protected def job(args: Array[String]): Unit = {

        val sc = SparkExtend.ctx

        val linesRdd = sc.textFile(this.inPath)
        val beanRdd = linesRdd.map(line => {
            try {
                val fields: Array[String] = line.split('\t')
                ItemScore(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3), fields(4).toInt, 1)
            } catch {
                case e: Exception => e.printStackTrace(); null
            }
        }).filter(_ != null)

        val uuid2ItemsRdd = beanRdd.groupBy(_.uuidIndex)
                .map(x => {
                    val items = x._2.toList.sortWith(_.score > _.score)
                    val meanScore = items.map(_.score).sum / items.length
                    items.foreach(_.score -= meanScore)
                    items
                })

        val relationItemIdRdd = uuid2ItemsRdd.flatMap(x => {
            val list = new ListBuffer[(Int, Int)]
            var i: Int = 0
            val max: Int = x.length

            var bContinue: Boolean = true

            while (bContinue && i < max) {
                val id = x(i).itemid
                x.foreach(item => {
                    if (id != item.itemid && id < item.itemid)
                        list += ((id, item.itemid))
                    else if (id != item.itemid && id > item.itemid)
                        list += ((item.itemid, id))
                })
                if (list.length > 2000)
                    bContinue = false
                i += 1
            }
            list
        }).distinct()

        val itemid2ItemsRdd = uuid2ItemsRdd.flatMap(x => x)
                .groupBy(_.itemid)
                .map(x => {
                    val itemid = x._1
                    val map = mutable.Map[Int, Float]()
                    x._2.foreach(item => map.put(item.uuidIndex, item.score))
                    (itemid, (itemid, map))
                })

        val relationItemRdd: RDD[((Int, mutable.Map[Int, Float]), (Int, mutable.Map[Int, Float]))] = relationItemIdRdd.join(itemid2ItemsRdd)
                .map(_._2)
                .join(itemid2ItemsRdd)
                .map(_._2)

        val format = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH)
        val dateStr = format.format(this.logDate)
        val outPath = "hdfs://node-1:8020/wj-temp/item-case-similarly/" + dateStr
        relationItemRdd.map(x => computeItemSimilarly(x._1._1, x._1._2, x._2._1, x._2._2))
                .filter(_._3 >= 0.2)
                .map(x => {
                    val sb = new StringBuilder()
                    sb.append(x._1)
                    sb.append('\t')
                    sb.append(x._2)
                    sb.append('\t')
                    sb.append(2)
                    sb.append('\t')
                    sb.append(x._3.toFloat)
                    sb.toString()
                })
                .saveAsTextFile(outPath)
    }
}

object MobileJobItemSimilarity {
    def main(args: Array[String]): Unit = {
        val job = new MobileJobItemSimilarity()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}