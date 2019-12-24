package com.wangjia.bigdata.core.job.weixin

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar
import java.util.Map.Entry

import com.alibaba.fastjson.JSON
import com.google.gson.{JsonElement, JsonObject, JsonParser}
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.es.{EsConnection, EsPageQuery, ExEsConnection}
import org.elasticsearch.search.SearchHit

import scala.collection.mutable.ListBuffer

/**
  * Created by Cfb on 2018/2/2.
  */
class MobileJobPU extends EveryDaySparkJob {
    override protected def job(args: Array[String]): Unit = {
//        val sc = SparkExtend.ctx
//        val zhuangxiu =sc.textFile("D:\\Documents\\usertag22.txt").map(x=>(x.split("\t")(6)))
//                .filter(_.contains("装修")).flatMap(x=>{
//            val deviceInfo: util.Map[String, String] = JSON.parseObject(x, classOf[util.Map[String, String]])
//            val tuples: Iterator[(String, String)] = deviceInfo.iterator
//            val list: ListBuffer[(String, String)] = new ListBuffer[(String,String)]()
//            while(tuples.hasNext){
//                val next: (String, String) = tuples.next()
//                if(next._2.contains("装修")){
//                    list +=((next._1,next._2))
//                }
//            }
//            list
//        }).distinct()
//        zhuangxiu.repartition(1).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\2222\\app1")

        //        sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\225011.txt").filter(x=>{
        //            if(x.split(",").size>3){
        //                x.split(",")(3).contains("北京")
        //            }
        //            null
        //        })
        //        sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\225011.txt").filter(x=>x.split(",")(3).contains("北京")).repartition(1).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\result2262")
        //
        //        val all1 =sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\all").map(x=>(x.split(",")(0),x.split(",")(1),x.split(",")(2),x.split(",")(3),x.split(",")(4),x.split(",")(5),x.split(",")(6),x.split(",")(7),x.split(",")(8)))
        //
        //        val chang1 = sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\chang.txt").map(x=>(x.split("\t")(0),x.split("\t")(6)))
        //        chang1.take(1).foreach(println)
        //        val result =all1.map(x=>(x._1,x)).join(chang1).map(x=>{
        //            (x._2._1._1,x._2._1._2,x._2._1._3,x._2._1._4,x._2._1._5,x._2._1._6,x._2._1._7,x._2._1._8,x._2._1._9,x._2._2)
        //        })

        //        sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\guanzhu.txt").map(x=>x.replace("%23","#")).take(10).map(x=>{
        //            if (x.split(",").size == 2)
        //                println(x.split(",")(0), x.split(",")(1))
        //            else
        //                println(x.replace(",",""),"")
        //
        //            null
        //        })

        //
        //        val guanzhu = sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\guanzhu.txt").map(x=>x.replace("%23","#")).map(x=> {
        //
        //            if (x.split(",").size == 2){
        //                (x.split(",")(0), x.split(",")(1))
        //            }
        //            else{
        //                (x.replace(",",""),"")
        //            }
        //        }).filter(_!=null)
        //        println(guanzhu.count())
        //       val fi = result.map(x=>{
        //            val parser =new JsonParser()
        //            try{
        //            val asJsonObject: JsonObject = parser.parse(x._10).getAsJsonObject
        //            val entrySet: util.Set[Entry[String, JsonElement]] = asJsonObject.entrySet()
        //            val iterator: util.Iterator[Entry[String, JsonElement]]= entrySet.iterator()
        //            val sb =new StringBuilder()
        //            while(iterator.hasNext){
        //                val next: Entry[String, JsonElement] = iterator.next()
        //                sb.append(next.getValue.getAsString)
        //                sb.append("|")
        //            }
        //            (x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,sb.toString())
        //            }catch{
        //                case e:Exception=>null
        //            }
        //        }).map(x=>(x._2,x))
        //        println(fi.count())
        //        fi.join(guanzhu).map(x=>(x._2._1._1,x._2._1._2,x._2._1._3,x._2._1._4,x._2._1._5,x._2._1._6,x._2._1._7,x._2._1._8,x._2._1._9,x._2._1._10,x._2._2)).repartition(1).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\result2238")
        //        result.map(x=>{
        //            val asJsonObject: JsonObject = parser.parse(x._10).getAsJsonObject
        //            val entrySet: util.Set[Entry[String, JsonElement]] = asJsonObject.entrySet()
        //
        //        })
        //        result.repartition(1).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\applist")


//        val clueRdd1 = sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\iporother1.txt").map(x => (x.split("\t")(0), x.split("\t")(1), x.split("\t")(2), x.split("\t")(3), x.split("\t")(4))).map(x => (x._1, x))
//        val all = sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\new\\useAll.txt")
//                .map(x => (x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3), x.split(",")(4), x.split(",")(5), x.split(",")(6), x.split(",")(7), x.split(",")(8))).map(x => (x._1, x))
//        val mat = all.leftOuterJoin(clueRdd1).filter(x => x._2._2 != None)
//        val mat1 = mat.map(x => {
//            (x._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._2.getOrElse(null)._4, x._2._1._6, x._2._1._7, x._2._1._8, x._2._1._9)
//        })
//        val unmat = all.leftOuterJoin(clueRdd1).filter(x => x._2._2 == None).map(x => {
//            (x._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, x._2._1._7, x._2._1._8, x._2._1._9)
//        })
//        //        mat1.union(unmat).repartition(1).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\1222")
//        sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\1222\\part-00000").filter(_.contains("北京")).repartition(1).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\new\\usewewe44.txt")
        //                .map(x=>(x.split(",")(0),x.split(",")(1),x.split(",")(2),x.split(",")(3),x.split(",")(4),x.split(",")(5),x.split(",")(6),x.split(",")(7)))
        //        allRdd.mapPartitions(iter=>{
        //            val value: EsConnection = new EsConnection()
        //                        //uuid,ip,addre,phone
        //
        //                        val list =new ListBuffer[(String,String,String,String,String,String,String,String,String)]()
        //                        iter.foreach(x=>{
        //                            try{
        //                            val sss: String = value.sss1(x._1)
        //                            list += ((sss,x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8))
        //                            }catch{
        //                                case e=>println(e.getStackTrace)
        //                            }
        //                        })
        //
        //                        list.toIterator
        //        }).repartition(1).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\result007")
        //        91b8807cfa88a41768c7803d5d9756dc,223.104.4.239,13862548387,浙江嘉兴,ios,0.32523999999999986,6,理财|爱看新闻|孩子7-11岁|爱好旅游|团购达人|爱玩游戏|爱好健身|爱听音乐|热衷全球购|有车|
        //        sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\tag11.txt").map(x=>(x.split(",")(0),x.split(",")(1),x.split(",")(2),x.split(",")(3),x.split(",")(4),x.split(",")(5),x.split(",")(6),x.split(",")(7)))
        //                 .map(x=>(x._1,x._4,x._2,x._3,x._5,x._6,x._7,x._8)).repartition(1).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\result004")
        //                sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\tag.txt").filter(_.contains("北京")).repartition(1).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\result005")
        //        val nullRdd =sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\final.txt").map(_.replace("(","").replace(")","")).filter(!_.contains("null"))
        //        nullRdd.saveAsTextFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\result002")
//        val lineRdd = sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\new\\usertag11.txt")
//        val clueRdd = sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\iporother.txt")
//        ////        (FEC29CFC-95C8-4FB5-8BAE-272E22DBA6F0,    223.72.82.221,     13001207867,    刘女士,      北京北京)
//        ////        (0013aaf723045c4c8250b6320c06a33f,ios,0.579598,8,爱好旅游|爱好健身|爱听音乐|孩子0-6岁|高知|追求奢侈品|科技发烧友|爱看直播|爱看电影|爱玩游戏|爱好烹饪|热爱阅读|炒股|)
//
//        //        00010a1416aa9b9703528855a0e0d144	000000000000000#6dbbdc0061633cf7	android	6	0	{"爱好健身":1,"热衷全球购":1,"爱好烹饪":1,"爱听音乐":1}
//        //        1200	装修设计-ftmd-android	000000000000000#04f89ac565dd62f1	84eb17f406c9badc62060f0aa7bb37f3	42.90.147.40	15809330070	2017/9/21 14:09:23	Y咿呀Y	甘肃平凉
//
//        val orginFinishedClueRdd = clueRdd.map(x => (x.split("\t")(3), x.split("\t")(2), x.split("\t")(4), x.split("\t")(5), x.split("\t")(8))).map(x => (x._1, x))
//        orginFinishedClueRdd.take(10).foreach(println)
//        //
//        //
//        //
//        val orginFinishedLine = lineRdd.map(x => (x.split("\t")(0), x.split("\t")(1), x.split("\t")(2), x.split("\t")(3), x.split("\t")(4), x.split("\t")(5))).map(x => {
//            val sb = new StringBuilder()
//            val list = new ListBuffer[String]()
//            val arrs = x._6.replace("{", "").replace("}", "").split(",")
//            for (obj <- arrs) {
//                val string = obj.split(":")(1) + obj.split(":")(0)
//                list += string
//            }
//            val end = list.sortWith(_ > _)
//            for (i <- end) {
//                sb.append(i.split("\"")(1))
//                sb.append("|")
//            }
//            (x._1, x._2, x._3, x._4, x._5, sb.toString())
//        })
//        orginFinishedLine.take(1).foreach(println)
//
//        val mathchRdd = orginFinishedLine.map(x => (x._1, x)).leftOuterJoin(orginFinishedClueRdd)
//
//        val clueMatchRdd = mathchRdd.filter(_._2._2 != None)
//        clueMatchRdd.take(1).foreach(println)
//        clueMatchRdd.map(x => {
//            val y: (String, String, String, String, String) = x._2._2.getOrElse(null)
//            (x._1, y._2, y._3, y._5, y._4, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6)
//            //                    (x._1,y._2,x._2._1._3,x._2._1._5,x._2._1._4,y._3,x._2._1._5,y._5)
//        }).repartition(1).saveAsTextFile("C:\\\\Users\\\\Administrator\\\\Desktop\\\\Doc\\\\newwork\\\\result10000")
//        ////        (3b07f30f298e4bf3ac54ee4a5e68d1cd,((3b07f30f298e4bf3ac54ee4a5e68d1cd,ios,3.260061999999999,6,爱玩游戏|爱听音乐|爱看直播|爱好旅游|爱看新闻|爱好烹饪|爱好唱歌|爱好健身|热衷全球购|孩子7-11岁|孩子0-3岁|),None))
//        //
//
//        val unMatchRdd = mathchRdd.filter(_._2._2 == None).map(_._2._1)
//        //
//        //
//        ////        val clueRdd =sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\calculate\\part-00000")
//        //
//        //
//        //
//        //        unMatchRdd.take(1).foreach(println)
//        val hanlerRDD = unMatchRdd.repartition(5)
//        hanlerRDD.take(1).foreach(println)
//        val resultUnMatchedRdd = hanlerRDD.mapPartitions(iter => {
//            val value: EsConnection = new EsConnection()
//            //uuid,ip,addre,phone
//            val list = new ListBuffer[(String, String, String, String, String, String, String, String, String)]()
//            iter.foreach(x => {
//                val sss: String = value.sss(x._1)
//                list += ((x._1, x._2, sss.split("`")(1), sss.split("`")(0), "phone", x._3, x._4, x._5, x._6))
//            })
//            list.toIterator
//        })
//        resultUnMatchedRdd.saveAsTextFile("C:\\Users\\Administrator\\Desktop\\Doc\\newwork\\result1005")
//
//
//
//
//        //
//        //        val sdf =new SimpleDateFormat("yyyy/MM/dd")
//        //        val sdf1 =new SimpleDateFormat("yyyy/MM/dd")
//        //        val date0201 =sdf.parse("2018/01/31")
//        //        val cal = Calendar.getInstance()
//        //        cal.setTimeInMillis(date0201.getTime)
//        //        val list =new ListBuffer[String]
//        //
//        //        val year =cal.get(1)
//        //        val month ={
//        //            var fmon =""
//        //            val  mon=(cal.get(2)+1).toString
//        //            if(mon.length==1)
//        //                fmon ="0"+mon
//        //            else
//        //                fmon=mon
//        //            fmon
//        //        }
//        //        val day ={
//        //            var fda =""
//        //            val  da=cal.get(5).toString
//        //            if(da.length==1)
//        //                fda ="0"+da
//        //            else
//        //                fda=da
//        //            fda
//        //        }
//        //
//        //
//        //
//        //
//        //
//        //        println(year+"-"+month+"-"+day)
//        //        val file  =sc.textFile("C:\\Users\\Administrator\\Desktop\\Doc\\Work\\渠道折线图\\broken-line.txt")
//        //                  .map(x=>(x.split(",")(1),x.split(",")(0),x.split(",")(2))).distinct().map(x=> {
//        //                    val date =sdf.parse(x._2)
//        //                    val x2 =sdf1.format(date)
//        //            (x._1,x2,x._3)
//        //           })
//        //
//        //        file.groupBy(_._1).map(_._2.toList.sortWith(_._2<_._2)).map(x=>{
//        //
//        //            val sbDate =new StringBuilder()
//        //            val sbPv =new StringBuilder()
//        //            x.foreach(x=>{
//        //                sbDate.append(x._2)
//        //                sbDate.append("`")
//        //                sbPv.append(x._3)
//        //                sbPv.append("`")
//        //            })
//        //            (x.head._1,sbDate.toString(),sbPv.toString())
//        //        }).foreach(println)
//        //        C:\Users\Administrator\Desktop\weihunkaoyan\part-00000
//        //        val file  =sc.textFile("G:\\LOG\\log\\clue").map(x=>x.replace("(","").replace(")",""))
//        //        .filter(x=>x.contains("未婚")|| x.contains("考研"))
//        //        println(file.count()+"=未婚+考研总数")
//        //        println(file.filter(_.contains("现代简约")).count()+"=现代简约")
//        //        println(file.filter(_.contains("美式")).count()+"=美式")
//        //        println(file.filter(_.contains("欧式")).count()+"=欧式")
//        //                            .filter(x=> !x.contains("kw") && !x.contains("yige")).map(x=>x.replace("(","").replace(")","")).filter(x=>{
//        ////            x.contains("黄") || x.contains("色")
//        //            x.contains("居") || x.contains("公寓") || x.contains("复式") || x.contains("LOFT") || x.contains("型") || x.contains("别墅") || x.contains("跃层")
//        //        })
//        //             file.foreach(println)
//
//
//        //        val file = sc.textFile("C:\\Users\\wangjie\\Desktop\\1")
//        //        val filterRDD =file.map(x=>x.replace("(","").replace(")",""))
//        //                .filter(x=>x.contains("未婚")|| x.contains("考研"))
//        //        println(filterRDD.count())
//        //        val result =filterRDD.map(x=>x.split(",")(2))
//        //                .flatMap(x=>(x.split("`")))
//        //                .map(x=>{
//        //                    if(x.split(":").size==2)
//        //                        (x.split(":")(0),x.split(":")(1).toDouble)
//        //                    else
//        //                        ("yige",9.000)
//        //                }).reduceByKey(_+_)
//        //        //      .map(x=>{
//        //        //      if(x.size!=0)
//        //        //      (x.split(":")(0),x.split(":")(1))
//        //        //      else
//        //        //        null
//        //        //    })
//        //
//        //        result.repartition(1).saveAsTextFile("C:\\Users\\wangjie\\Desktop\\4")
//        //    }

    }
}
object MobileJobPU {

    def main(args: Array[String]) {

        val job = new MobileJobPU()
        job.run(args)

        //        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
