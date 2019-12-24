package com.wangjia.bigdata.core.job.test_web

import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util.Locale

import com.alibaba.fastjson.JSON
import com.wangjia.bigdata.core.bean.info.{AccountInfo, AppInfo}
import com.wangjia.bigdata.core.handler.LogClearStatus
import com.wangjia.utils.EncrypUtils

import scala.collection.mutable

case class wzj(appid: String, logtype: String, pageid: String, subtype: String)

object LogClearTest {
  private val KEY = "7F76BA2B075CB3134A9A0D5023425E4D".getBytes

  private val DEF_IMEI: String = "000000000000000"

  private val serverTimeFormat = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue: SimpleDateFormat = {
      new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
    }
  }

  private val probeTimeFormat = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue: SimpleDateFormat = {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    }
  }

  def clearMobileLog(log: String,
                     mapAppInfos: mutable.Map[String, AppInfo],
                     mapIdentifierAppInfos: mutable.Map[String, AppInfo],
                     mapAccountInfos: mutable.Map[Int, AccountInfo]): (Int, wzj) = {
    //基础转换 IP 服务器时间 日志
    val ipAndTimeAndJson: (String, Long, String) = {
      try {
        val ipIndex = log.indexOf(" - - [")
        val endIndex = log.indexOf("] ", ipIndex)

        //得到服务器时间
        val timeStartIndex = log.indexOf("[timestamp:", endIndex)
        val serverTime: Long = {
          if (timeStartIndex > 0) {
            val timeEndIndex = log.indexOf("] ", timeStartIndex)
            val timeStr: String = log.substring(timeStartIndex + 11, timeEndIndex)
            (timeStr.toDouble * 1000).toLong
          } else {
            val timeStr: String = log.substring(ipIndex + 6, endIndex)
            serverTimeFormat.get().parse(timeStr).getTime
          }
        }

        if (ipIndex > 50) {
          return (LogClearStatus.ERROR_FORMAT, null)
        }
        //得到IP
        val ipStr = log.substring(0, ipIndex).split(',')(0)
        //得到加密数据
        var sIndex = log.indexOf("data=")
        if (sIndex <= 0)
          return (LogClearStatus.ERROR_FORMAT, null)
        sIndex += "data=".length
        val eIndex = {
          val len = log.indexOf(" HTTP", sIndex)
          if (len > sIndex)
            len
          else
            log.indexOf(" EOF", sIndex)
        }

        if (eIndex <= sIndex)
          return (LogClearStatus.ERROR_FORMAT, null)
        val str = log.substring(sIndex, eIndex)
        //得到Json字符串
        if (!str.startsWith("TGR"))
          return (LogClearStatus.ERROR_FORMAT, null)
        val urleStr = URLDecoder.decode(str, "UTF-8")
        val jStr = EncrypUtils.decrypt(urleStr, KEY)

        (ipStr, serverTime, jStr)
      } catch {
        case e: Exception => println(log); e.printStackTrace(); return (LogClearStatus.ERROR_FORMAT, null)
      }
    }
    //JSON简析
    val json = {
      try {
        JSON.parseObject(ipAndTimeAndJson._3)
      } catch {
        case e: Exception => e.printStackTrace(); return (LogClearStatus.ERROR_JSON, null)
      }
    }
    try {
      val zz = wzj(json.getString("appid"), json.getString("logtype"), json.getString("pageid"), json.getString("subtype"))
      return (LogClearStatus.SUCCESS, zz)
    } catch {
      case e: Exception => e.printStackTrace(); return (LogClearStatus.ERROR_JSON, null)
    }
    return (LogClearStatus.ERROR_JSON, null)
  }


}
