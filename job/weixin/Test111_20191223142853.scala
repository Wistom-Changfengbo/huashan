package com.wangjia.bigdata.core.job.weixin

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.gson.{Gson, JsonObject, JsonParser}
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.handler.label.Uuid2LabelSumHandler
import com.wangjia.hbase.{HBaseConst, HBaseTableName}
import com.wangjia.hbase.conn.{ExTBConnection, ExTable}
import com.wangjia.math.ExMath
import com.wangjia.utils.{JavaUtils, RedisUtils}
import org.apache.hadoop.hbase.client.{Get, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Cfb on 2018/3/15.
  */
case class Clue(time: String, phone: String, people: String, city: String, sex: String, vality: String, consume: String, guanzhu: String, label: String, search: String)

case class GL(uuid: String, did: String, applist: java.util.HashMap[String, String], dtype: String)

class Test111 extends EveryDaySparkJob {
    //xiansuo/uuidsss.txt

    private val  inpath ="/xiansuo/uuidsss.txt"
    private var mapTag:Broadcast[mutable.Map[String,String]] = null
    private var mapNewTag:Broadcast[mutable.Map[String,String]] = null
    /**
      * 初始化SparkContext
      */
    override protected def init(): Unit = {
        val sc =SparkExtend.ctx
        val conn: Jedis = RedisUtils.getConn("192.168.100.21", 6379, 4)
        val keys: util.Set[String] = conn.keys("app:tag:*")
        val iterator: util.Iterator[String] = keys.iterator()
        val map = mutable.Map[String,String]()
        while(iterator.hasNext){
            val next: String = iterator.next()
            val all: util.Map[String, String] = conn.hgetAll(next)
            val keySet: util.Set[String] = all.keySet()
            val keyIter: util.Iterator[String] = keySet.iterator()
            var values:String =""
            while(keyIter.hasNext){
                val key: String = keyIter.next()
                val value:String =all.get(key)
                values =values.concat(value+",")
            }
            map(next) =values
        }
        val keysNew: util.Set[String] = conn.keys("app:newtag*")
        val keysNewIter: util.Iterator[String] = keysNew.iterator()
        val mapNew = mutable.Map[String,String]()
        while(keysNewIter.hasNext) {
            val key: String = keysNewIter.next()
            val value: String = conn.get(key)
//            println(value)
            mapNew(key)=value
        }

        mapTag =sc.broadcast(map)
        mapNewTag =sc.broadcast(mapNew)
        RedisUtils.closeConn(conn)
    }

    /**
      * 任务入口方法
      *
      * @param args
      */
    override protected def job(args: Array[String]): Unit = {
        val sc: SparkContext = SparkExtend.ctx
        val beanRdd = sc.textFile(this.inpath).map(x => {
            try{
            val splits = x.split("\t")
            var platform = ""
            if (splits(2) == "2") {
                platform = "ios"
            } else {
                platform = "android"
            }
            val applist = splits(3)
            val jsonObj = new JsonParser().parse(applist).getAsJsonObject

            val gson: Gson = new Gson()
            val json: java.util.HashMap[String, String] = gson.fromJson(applist, classOf[java.util.HashMap[String, String]])

            GL(splits(0), splits(1), json, platform)
            }catch {
                case e:Exception=>println(x)
                    null
            }
        })
//        beanRdd.foreach(println)
//        beanRdd.take(1).foreach(x=>{
//            val yhbq =yonghubiaoqian(mapTag,mapNewTag,x)
//        })



        import scala.collection.JavaConversions._
        val resultRDD =beanRdd.mapPartitions(iter => {
            initHBase()
//            val conn: Jedis = RedisUtils.getConn("192.168.100.21", 6379, 4)
//            println(conn.hgetAll("app:tag:android_cn.com.cf8.school"))
            val exconn: ExTBConnection = new ExTBConnection()
            val table: Table = exconn.getTable(HBaseTableName.UUID_LABEL_SUM).getTable()
            val finalList =ListBuffer[(String,String,String)]()
            iter.foreach(x => {
//                println(x)
                try{
                var calcData:String=""
                //用户标签
                val yhbq =yonghubiaoqian(mapTag,mapNewTag,x)
                if(yhbq!=null){
                val get: Get = new Get(Bytes.toBytes(x.uuid))
                //用户关注的标签和权重  1个字段
                 calcData = calguanzhu(get, table)
                }
                finalList +=((x.uuid,yhbq,calcData))
                }catch{
                    case e:Exception=>println(x)
                }

            })
//            RedisUtils.closeConn(conn)
            exconn.close()
            finalList.toIterator
        })
//        (001a68f5d842287379b30fcb7399eb3e,爱好唱歌|团购达人|爱看直播|孩子0-6岁|爱听音乐|,30万左右:0.0`181-320平米:0.0`15万左右:0.0`_sf_liveness:0.0`10万左右:0.0`跃层:0.0`淡黄:0.0`别墅:0.0`二居:0.0`5万左右:0.0`卧室:0.0`北欧:0.0`混搭:0.0`)

        resultRDD.filter(x=>{
            x._2.contains("未婚") || x._2.contains("考研")
        }).map(x=>(x._2,x._3))







        //        val file: RDD[String] = sc.textFile("C:\\Users\\Administrator\\Desktop\\C1.txt")
        //         file.take(2).foreach(println)
        //
        //         val filterRDD =file.filter(x=>x.contains("未婚") || x.contains("考研")).map(x=>{
        //            try{
        //                val arrs =x.split(",")
        ////                if(arrs.size==9){
        //             val time =arrs(0)
        //             val phone =arrs(1)
        //             val people =arrs(2)
        //             val city =arrs(3)
        //             val sex =arrs(4)
        //             val vality =arrs(5)
        //             val consume =arrs(6)
        //             val guanzhu =arrs(7)
        //             val label =arrs(8)
        ////             val search =arrs(9)
        //             Clue(time,phone,people,city,sex,vality,consume,guanzhu,label,"")
        ////                }else{
        ////                    val time =arrs(0)
        ////                    val phone =arrs(1)
        ////                    val people =arrs(2)
        ////                    val city =arrs(3)
        ////                    val sex =arrs(4)
        ////                    val vality =arrs(5)
        ////                    val consume =arrs(6)
        ////                    val guanzhu =arrs(7)
        ////                    val label =arrs(8)
        ////                    Clue(time,phone,people,city,sex,vality,consume,guanzhu,label,"")
        ////                }
        //            }catch {
        //                case e: Exception => e.printStackTrace(); println(x)
        //            null
        //            }
        //         }).filter(_!=null)
        ////        println(filterRDD.count())
        ////        println()
        //        filterRDD.flatMap(x=>x.guanzhu.split("\\|")).map(x=>(x,1)).filter(x=>x._1!="" || x._1!=null).foreach(println)

    }


    def calguanzhu(get: Get, tb: Table): String = {
        val result: Result = tb.get(get)
        val value: String = Bytes.toString(result.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG))
        val familyMap: util.NavigableMap[Array[Byte], Array[Byte]] = result.getFamilyMap(HBaseConst.BYTES_CF1)
        val keySet: util.Set[Array[Byte]] = familyMap.keySet()
        val iterator: util.Iterator[Array[Byte]] = keySet.iterator()
        //计算N条数据加起来的总的   标签/权重值
        val calcData: util.Map[String, Double] = new util.HashMap[String, Double]
        while (iterator.hasNext) {
            val rowkey: String = Bytes.toString(iterator.next())
            val singleResult = Bytes.toString(result.getValue(HBaseConst.BYTES_CF1, rowkey.getBytes()))
            val labelJson = new JsonParser().parse(singleResult).getAsJsonObject()
            var sum: Double = 0.0
            var ratio: Double = 0
            var diffDay: Int = 0
            var keyOutter: String = ""
            if (labelJson.get("labels") != null) {
                val arrs = labelJson.get("labels").getAsJsonArray()
                val time = labelJson.get("time").getAsLong
                var i = 0
                var max = arrs.size()
                //                val labelList: ListBuffer[(String, Double)] = new ListBuffer[(String, Double)]
                while (i < max) {
                    val asJsonObject: JsonObject = arrs.get(i).getAsJsonObject
                    val key = asJsonObject.get("key").getAsString
                    val value = asJsonObject.get("value").getAsLong
                    diffDay = JavaUtils.timeMillis2DayNum(System.currentTimeMillis) - JavaUtils.timeMillis2DayNum(time)
                    ratio = ExMath.attenuation(0.825, diffDay)
                    sum = calcData.getOrDefault(key, 0.0)
                    sum += (value * ratio)
                    //                    labelList.append((key, sum))
                    calcData.put(key, sum)
                    i += 1
                }
            }
        }
        val sb:StringBuilder =new StringBuilder
        val it: util.Iterator[String] = calcData.keySet().iterator()
        while(it.hasNext){
            val key =it.next()
            sb.append(key+":"+calcData.get(key)+"`")
        }
        sb.toString()
    }
    def yonghubiaoqian(mapTag:Broadcast[mutable.Map[String,String]],mapNewTag:Broadcast[mutable.Map[String,String]],x:GL): String = {
//        println("applist"+x.applist)
        //用户标签
        val keySet = x.applist.keySet()
        val iterator = keySet.iterator()
        val strings: ListBuffer[String] = new ListBuffer[String]
        val filterTag: util.HashSet[String] = new util.HashSet[String]()
        val filterNew: util.HashSet[String] = new util.HashSet[String]()
        val filteryonghu: ListBuffer[(String, String)] = ListBuffer[(String, String)]()
        val sb: StringBuilder = new StringBuilder()
        while (iterator.hasNext) {
            val pkgName: String = iterator.next()
//            println("pkgName="+pkgName)
            //                        strings +="app:tag:" + x.dtype + "_" + pkgName
            val valueTag: String = mapTag.value.getOrElse("app:tag:" + x.dtype + "_" + pkgName,"no")
            if(valueTag!="no"){
                val split: Array[String] = valueTag.split(",")
                if(split.length==1){
                    filterTag.add(split(0))
                }else if(split.length==2){
                    filterTag.add(split(0))
                    filterTag.add(split(1))
                }
            }
//            //App标签
//            val all: util.Map[String, String] = conn.hgetAll("app:tag:" + x.dtype + "_" + pkgName)
//            //用户标签
//            filter.addAll(all.values())
        }

//        println("length"+filter.size())
//        println(filterTag)

        if (!filterTag.isEmpty) {
            val iterator1: util.Iterator[String] = filterTag.iterator()
            while (iterator1.hasNext) {
                val key: String = iterator1.next()
//                val valueNew: String = mapNewTag.value("app:newtag:" + key)
                val all1: String = mapNewTag.value.getOrElse("app:newtag:" + key,"no")
                if (all1 != "no"){
                    filterNew.add(all1)
                }
            }
        }
        val filterNewIter: util.Iterator[String] = filterNew.iterator()
        while(filterNewIter.hasNext){
            sb.append(filterNewIter.next())
            sb.append("|")
        }
        sb.toString()
//        println(sb.toString())
//        if(sb.contains("未婚") || sb.contains("考研")){
//            sb.toString()
//        }else{
//            null
//        }

    }
}

object Test111 {
    def main(args: Array[String]) {
        val job = new Test111()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
