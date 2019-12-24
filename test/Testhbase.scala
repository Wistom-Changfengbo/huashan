package com.wangjia.bigdata.core.test

import java.net.URLDecoder

import com.wangjia.bigdata.core.handler.{BeanHandler, LogClearHandler}
import com.wangjia.hbase.HBaseTableName
import com.wangjia.utils.HBaseUtils
import org.apache.hadoop.hbase.HBaseConfiguration

/**
  * Created by Cfb on 2018/4/20.
  */
object Testhbase extends  App{
    val log ="101.254.99.26 - - [07/May/2018:16:20:35 +0800] [timestamp:1525681235.031] \"POST /wxxcx/pv HTTP/1.0\"\"%7B%22device_info%22%3A%7B%22ja_uuid%22%3A%7B%22data%22%3Anull%7D%2C%22unionid%22%3A%22%22%2C%22system%22%3A%2211.3.1%22%2C%22version%22%3A%2210.1.22.687%22%2C%22currentBattery%22%3A%2275%25%22%2C%22brand%22%3A%22iPhone%22%2C%22windowHeight%22%3A554%2C%22pixelRatio%22%3A2%2C%22platform%22%3A%22iOS%22%2C%22screenHeight%22%3A667%2C%22language%22%3A%22zh-Hans%22%2C%22app%22%3A%22alipay%22%2C%22storage%22%3A%2259.59%20GB%22%2C%22windowWidth%22%3A375%2C%22model%22%3A%22iPhone8%2C1%22%2C%22screenWidth%22%3A375%2C%22fontSizeSetting%22%3A16%2C%22networkType%22%3A%22WIFI%22%7D%2C%22ja_version%22%3A%222.0.0%22%2C%22app_version%22%3A%220.0.4%22%2C%22app_identifier%22%3A%222018041902582356%22%2C%22appid%22%3A%222018041902582356%22%2C%22userid%22%3A%22%22%2C%22time%22%3A1525681234969%2C%22logtype%22%3A%2250%22%2C%22subtype%22%3A%221%22%2C%22pageid%22%3A%22detailes_case%22%2C%22data%22%3A%7B%22id%22%3A%221000929%22%7D%7D EOF\" 200 3 \"-\" \"%E6%94%AF%E4%BB%98%E5%AE%9D/10.1.22.687 CFNetwork/897.15 Darwin/17.5.0\" "
    LogClearHandler.clearWeiXinLog(log,null,null)



    //得到IP
    val ipEndIndex = log.indexOf(" - - [")
    val ip = log.substring(0, ipEndIndex).split(',')(0)

    //得到服务器时间
    val timeStartIndex = log.indexOf("[timestamp:", ipEndIndex)
    val timeEndIndex = log.indexOf("] ", timeStartIndex)
    val timeStr: String = log.substring(timeStartIndex + 11, timeEndIndex)
    val serverTime: Long = (timeStr.toDouble * 1000).toLong

    //得到数据体
    val newLog: String = log.replace("\\x22", "\"")
    val dataStartIndex = newLog.indexOf("\"\"{\"") + 2
    val dataEndIndex = newLog.indexOf(" EOF\"", dataStartIndex)
//    val dataStr = newLog.substring(dataStartIndex, dataEndIndex).replace("%","")
    val jsonStr = URLDecoder.decode(newLog.replace("\\x", "%"), "UTF-8")













//    var ss ="114.96.129.99 - - [04/May/2018:11:24:40 +0800] [timestamp:1525404280.911] \"POST /wxxcx/pv HTTP/1.0\"\"{\\x22device_info\\x22:{\\x22ja_uuid\\x22:\\x22c60129d6bde29e533e86c9212e7601e8\\x22,\\x22unionid\\x22:\\x22olMhQt0keVvk4Uo8U1eu-Szsx9T8\\x22,\\x22screenWidth\\x22:360,\\x22pixelRatio\\x22:2,\\x22system\\x22:\\x22Android 5.1\\x22,\\x22benchmarkLevel\\x22:7,\\x22windowWidth\\x22:360,\\x22brand\\x22:\\x22360\\x22,\\x22screenHeight\\x22:640,\\x22version\\x22:\\x226.6.6\\x22,\\x22fontSizeSetting\\x22:16,\\x22language\\x22:\\x22zh_CN\\x22,\\x22windowHeight\\x22:513,\\x22model\\x22:\\x221501_M02\\x22,\\x22platform\\x22:\\x22android\\x22,\\x22SDKVersion\\x22:\\x222.0.4\\x22,\\x22networkType\\x22:\\x22wifi\\x22},\\x22ja_version\\x22:\\x222.0.0\\x22,\\x22app_version\\x22:\\x221.6.2\\x22,\\x22app_identifier\\x22:\\x22wx973d233e72974da8\\x22,\\x22appid\\x22:\\x22wx973d233e72974da8\\x22,\\x22userid\\x22:6198830,\\x22time\\x22:1525404278859,\\x22logtype\\x22:\\x2240\\x22,\\x22subtype\\x22:\\x22__sys_app_location\\x22,\\x22pageid\\x22:\\x22main_tab_case_list\\x22,\\x22data\\x22:{\\x22longitude\\x22:116.80144,\\x22latitude\\x22:33.97202,\\x22double\\x22:\\x22__sys_decare_tencent\\x22}} EOF\" 200 3 \"https://servicewechat.com/wx973d233e72974da8/20/page-frame.html\" \"Mozilla/5.0 (Linux; Android 5.1; 1501_M02 Build/LMY47D; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/53.0.2785.143 Crosswalk/24.53.595.0 XWEB/50 MMWEBSDK/19 Mobile Safari/537.36 MicroMessenger/6.6.6.1300(0x26060636) NetType/WIFI Language/zh_CN MicroMessenger/6.6.6.1300(0x26060636) NetType/WIFI Language/zh_CN\" "
//
//
//
//
//
//
//
//    //    val haha =LogClearHandler.clearWeiXinLog(ss)
//
//
//    ss =ss.replace("\\x22", "\"")
//    val dataStartIndex = ss.indexOf("\"\"{\"") + 2
//    val dataEndIndex = ss.indexOf(" EOF\"", dataStartIndex)
//    val dataStr = ss.substring(dataStartIndex, dataEndIndex)
//    val jStr = URLDecoder.decode(dataStr.replace("\\x", "%"), "UTF-8")
//    val bean =BeanHandler.toWeiXin(dataStr)
//    println(bean)

    val _conf = HBaseConfiguration.create()
    _conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    _conf.set("hbase.zookeeper.quorum", "ambari.am0.com")
            _conf.set("hbase.zookeeper.property.clientPort", "2181")
    HBaseUtils.setConfiguration(_conf)
    HBaseUtils.createTable(HBaseTableName.UUID_DEVICE_MSG)
    println("s")
}
