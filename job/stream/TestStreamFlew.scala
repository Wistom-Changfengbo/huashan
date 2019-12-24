package com.wangjia.bigdata.core.job.stream

import com.wangjia.bigdata.core.job.EveryDaySparkJob
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


/**
  * Created by Cfb on 2018/5/4.
  */
case class peo(name:String,num:Int)
class TestStreamFlew extends Serializable{
    def updateFuncIterClass =(iter:Iterator[(String,Seq[peo],Option[peo])])=>{
        val iterResult =iter.map(x=>{
            println("key="+x._1)
            val lnew =x._2
            println("新的Seq的size="+lnew.size)
            lnew.foreach(j=>{
                println("新的Seq="+j+x._1)
            })
            val lold:peo=x._3.getOrElse(peo("none",1000))
            println("老的Seq="+lold)
            (x._1,lold)
//            println("新的Seq"+x._2)
//            println("新的size"+x._2.size)
//            val lnew =x._2
//            val lold =x._3.getOrElse(0)
//            val sum =x._2.sum
//            (x._1,sum)
        })
        iterResult
    }


    def updateFuncIter =(iter:Iterator[(String,Seq[Int],Option[Int])])=>{
        val iterResult =iter.map(x=>{
            println("key"+x._1)
            println("新的Seq"+x._2)
            println("新的size"+x._2.size)
            val lnew =x._2
            val lold =x._3.getOrElse(0)
            val sum =x._2.sum
            (x._1,sum)
        })
        iterResult
    }
    /**
      * 任务入口方法
      *
      * @param args
      */

    protected def job(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "root")
        val _conf = new SparkConf().setMaster("local[2]").setAppName("hello")
        val ssc = new StreamingContext(_conf, Seconds(20L))
        ssc.sparkContext.setLogLevel("ERROR")
        ssc.checkpoint("hdfs://ambari.am0.com:9000/tmp/checkPoint/")
        val lines = ssc.socketTextStream("ambari.am0.com", 9999)
        val sum:DStream[(String,Int)] = lines.flatMap(_.split(",")).map(x => (x, 1)).reduceByKey(_ + _)

        val sumClass:DStream[(String,peo)] =lines.flatMap(_.split(",")).map(x => (x,peo(x, 1)))
        val sumClassResult =sumClass.updateStateByKey(updateFuncIterClass,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
        sumClassResult.foreachRDD(_.foreach(println))
        sumClassResult.print()
//        val sum2 =sum.updateStateByKey(updateFuncIter, new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
//        val sum1 =sum.updateStateByKey(updateFunc(100) _, new HashPartitioner(ssc.sparkContext.defaultParallelism))
        ssc.start()
        ssc.awaitTermination()
    }

    def updateFunc(pasttime:Long)(seq: Seq[Int], of: Option[Int]): Option[Int] = {
//        println(pasttime)
        println(seq.size)
        seq.foreach(x=>println("新数据"+x))

        val newSum: Int = seq.sum
        val preSum: Int = of.getOrElse(0)
        println("旧数据"+preSum)
        val som =Some(newSum + preSum)
        som
    }
}

object TestStreamFlew {
    def main(args: Array[String]) {
        val job = new TestStreamFlew
        job.job(args)
    }
}
