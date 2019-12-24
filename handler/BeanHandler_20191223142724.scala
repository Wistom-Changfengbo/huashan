package com.wangjia.bigdata.core.handler

import java.net.URLDecoder
import java.util

import com.alibaba.fastjson.JSON
import com.google.gson.Gson
import com.wangjia.bigdata.core.bean.common.{LogBase, UserClue}
import com.wangjia.bigdata.core.bean.info.PageInfo
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.bean.web.WebLogMsg
import com.wangjia.bigdata.core.bean.weixin.WeixinLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.common.LogType
import com.wangjia.utils.UUID

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/1/12.
  */
object BeanHandler {

    def getPageInfo(pageRuleInfos: mutable.Map[String, Array[PageInfo]], appid: String, url: String): PageInfo = {
        val arryPageInfos = pageRuleInfos.get(appid) match {
            case Some(x) => x
            case None => return null
        }
        arryPageInfos.foreach(x => {
            if (x.loose == 0 && x.isMeet(url))
                return x
        })
        null
    }

    def getPageInfoLooseIds(pageRuleInfos: mutable.Map[String, Array[PageInfo]], appid: String, url: String): ListBuffer[Int] = {
        val ids = ListBuffer[Int]()
        val arryPageInfos = pageRuleInfos.get(appid) match {
            case Some(x) => x
            case None => return ids
        }
        arryPageInfos.foreach(x => {
            if (x.loose == 0)
                return ids
            if (x.isMeet(url))
                ids += x.id
        })
        ids
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
      * 微信日志对象转换成字符串
      *
      * @param bean
      * @return
      */
    def toString(bean: WeixinLogMsg): String = {
        val sb: StringBuilder = new StringBuilder
        sb.append(bean.ja_version).append(Config.FIELD_SEPARATOR)
        sb.append(bean.app_version).append(Config.FIELD_SEPARATOR)
        sb.append(bean.appid).append(Config.FIELD_SEPARATOR)
        sb.append(bean.uuid).append(Config.FIELD_SEPARATOR)
        sb.append(bean.userid).append(Config.FIELD_SEPARATOR)
        sb.append(bean.logtype).append(Config.FIELD_SEPARATOR)
        sb.append(bean.pageid).append(Config.FIELD_SEPARATOR)
        sb.append(bean.subtype).append(Config.FIELD_SEPARATOR)
        sb.append(bean.time).append(Config.FIELD_SEPARATOR)
        sb.append(bean.localTime).append(Config.FIELD_SEPARATOR)
        sb.append(bean.title).append(Config.FIELD_SEPARATOR)
        sb.append(bean.data).append(Config.FIELD_SEPARATOR)
        sb.append(bean.staytime).append(Config.FIELD_SEPARATOR)
        sb.append(bean.visitindex).append(Config.FIELD_SEPARATOR)
        sb.append(bean.recentPage).append(Config.FIELD_SEPARATOR)
        sb.append(bean.ip).append(Config.FIELD_SEPARATOR)

        val gson: Gson = new Gson()
        val strDeviceInfo = gson.toJson(bean.deviceInfo)
        if (strDeviceInfo != null && !strDeviceInfo.equals("null"))
            sb.append(strDeviceInfo)
        sb.append(Config.FIELD_SEPARATOR)

        sb.toString()
    }

    /**
      * 字符串转换成微信日志对象
      *
      * @param msg
      * @return
      */
    def toWeiXin(msg: String): WeixinLogMsg = {
        try {
            val fields: Array[String] = msg.split(Config.FIELD_SEPARATOR)
            val it: Iterator[String] = fields.toIterator

            val ja_version: String = it.next()
            val app_version: String = it.next()
            val appid: String = it.next()
            val uuid: String = it.next()
            val userid: String = it.next()
            val logtype: String = it.next()
            val pageid: String = it.next()
            val subtype: String = it.next()
            val time: Long = it.next().toLong
            val localTime: Long = it.next().toLong
            val title: String = it.next()
            val data: String = it.next()
            val staytime: Long = it.next().toLong
            val visitindex: Int = it.next().toInt
            val recentPage: String = it.next().toString
            val ip: String = it.next()
            val deviceInfo: util.Map[String, Object] = JSON.parseObject(it.next(), classOf[util.Map[String, Object]])

            return WeixinLogMsg(ja_version, app_version, appid, uuid, userid, logtype, pageid, subtype, time, localTime, title, data, staytime, visitindex, recentPage, ip, deviceInfo)
        } catch {
            case e: Exception => e.printStackTrace(); println(msg)
        }
        null
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
      * Web日志对象转换成字符串
      *
      * @param bean
      * @return
      */
    def toString(bean: WebLogMsg): String = {
        val sb: StringBuilder = new StringBuilder
        sb.append(bean.ja_version).append(Config.FIELD_SEPARATOR)
        sb.append(bean.appid).append(Config.FIELD_SEPARATOR)
        sb.append(bean.uuid).append(Config.FIELD_SEPARATOR)
        sb.append(bean.userid).append(Config.FIELD_SEPARATOR)
        sb.append(bean.logtype).append(Config.FIELD_SEPARATOR)
        sb.append(bean.subtype).append(Config.FIELD_SEPARATOR)
        sb.append(bean.ref).append(Config.FIELD_SEPARATOR)
        sb.append(bean.url).append(Config.FIELD_SEPARATOR)
        sb.append(bean.time).append(Config.FIELD_SEPARATOR)
        sb.append(bean.title).append(Config.FIELD_SEPARATOR)
        sb.append(bean.data).append(Config.FIELD_SEPARATOR)
        sb.append(bean.staytime).append(Config.FIELD_SEPARATOR)
        sb.append(bean.visitindex).append(Config.FIELD_SEPARATOR)
        sb.append(bean.ip).append(Config.FIELD_SEPARATOR)
        sb.append(bean.acc).append(Config.FIELD_SEPARATOR)

        val gson: Gson = new Gson()
        val strDeviceInfo = gson.toJson(bean.deviceInfo)
        if (strDeviceInfo != null && !strDeviceInfo.equals("null"))
            sb.append(strDeviceInfo)
        sb.append(Config.FIELD_SEPARATOR)

        sb.toString()
    }

    /**
      * 字符串转换成WEB日志对象
      *
      * @param msg
      * @return
      */
    def toWeb(bAcc: Boolean = false)(msg: String): WebLogMsg = {
        try {
            val fields: Array[String] = msg.split(Config.FIELD_SEPARATOR)
            val it: Iterator[String] = fields.toIterator

            val ja_version: String = it.next()
            val appid: String = it.next()
            val uuid: String = it.next()
            val userid: String = it.next()
            val logtype: String = it.next()
            val subtype: String = it.next()
            val ref: String = it.next()
            val url: String = it.next()
            val time: Long = it.next().toLong
            val title: String = it.next()
            val data: String = it.next()
            val staytime: Long = it.next().toLong
            val visitindex: Int = it.next().toInt
            val ip: String = it.next()
            val acc: Int = it.next().toInt
            val deviceInfo: util.Map[String, Object] = JSON.parseObject(it.next(), classOf[util.Map[String, Object]])

            if (bAcc && acc != 1)
                return null
            return WebLogMsg(ja_version, appid, uuid, userid, logtype, subtype, ref, url, time, title, data, staytime, visitindex, ip, acc, deviceInfo)
        } catch {
            case e: Exception => e.printStackTrace(); println(msg)
        }
        null
    }

    val toWebAll = toWeb(bAcc = false) _
    val toWebAcc = toWeb(bAcc = true) _

    def toBase(bean: WeixinLogMsg): LogBase = LogBase(bean.appid, bean.uuid, bean.userid, bean.logtype, bean.subtype, bean.time, bean.staytime, bean.visitindex, bean.ip)

    def toBase(bean: WebLogMsg): LogBase = LogBase(bean.appid, bean.uuid, bean.userid, bean.logtype, bean.subtype, bean.time, bean.staytime, bean.visitindex, bean.ip)

    /**
      * WEB端 日志对象转换成线索对象
      *
      * @param bean
      * @return
      */
    def toUserClue(bean: WebLogMsg): UserClue = {
        try {
            val dataJson = if (bean.data.contains("{")) JSON.parseObject(bean.data) else JSON.parseObject(URLDecoder.decode(bean.data, "UTF-8"))

            val clueId: String = UUID.randomUUID
            val appId: String = bean.appid
            val userid: String = bean.userid
            val ip: String = bean.ip
            val uuid: String = bean.uuid
            val cookieId: String = bean.dStrInfo("ja_uuid")
            val deviceId: String = bean.dStrInfo("device_uuid")
            val deviceName: String = bean.dStrInfo("system")
            val version: String = ""
            val time: Long = bean.time
            val name: String = if (dataJson.containsKey("name")) dataJson.getString("name") else ""
            val phone: String = if (dataJson.containsKey("phone")) dataJson.getString("phone") else ""

            val cityId: String = {
                if (dataJson.containsKey("city_id"))
                    dataJson.getString("city_id")
                else if (dataJson.containsKey("city"))
                    dataJson.getString("city")
                else
                    ""
            }
            val houseArea: String = {
                if (dataJson.containsKey("house_area"))
                    dataJson.getString("house_area")
                else if (dataJson.containsKey("area"))
                    dataJson.getString("area")
                else
                    ""
            }

            val style: String = if (dataJson.containsKey("style")) dataJson.getString("style") else ""
            val flatsName: String = if (dataJson.containsKey("flats_name")) dataJson.getString("flats_name") else ""

            UserClue(clueId, uuid, ip, appId, userid, cookieId, deviceId, deviceName, version, time, name, phone, cityId, houseArea, style, flatsName)
        } catch {
            case e: Exception => e.printStackTrace(); null
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
      * APP日志对象转换成字符串
      *
      * @param bean
      * @return
      */
    def toString(bean: MobileLogMsg): String = {
        val sb: StringBuilder = new StringBuilder
        sb.append(bean.ja_version).append(Config.FIELD_SEPARATOR)
        sb.append(bean.app_version).append(Config.FIELD_SEPARATOR)
        sb.append(bean.appid).append(Config.FIELD_SEPARATOR)
        sb.append(bean.uuid).append(Config.FIELD_SEPARATOR)
        sb.append(bean.userid).append(Config.FIELD_SEPARATOR)
        sb.append(bean.logtype).append(Config.FIELD_SEPARATOR)
        sb.append(bean.pageid).append(Config.FIELD_SEPARATOR)
        sb.append(bean.subtype).append(Config.FIELD_SEPARATOR)
        sb.append(bean.time).append(Config.FIELD_SEPARATOR)
        sb.append(bean.localTime).append(Config.FIELD_SEPARATOR)
        sb.append(bean.title).append(Config.FIELD_SEPARATOR)
        sb.append(bean.data).append(Config.FIELD_SEPARATOR)
        sb.append(bean.staytime).append(Config.FIELD_SEPARATOR)
        sb.append(bean.visitindex).append(Config.FIELD_SEPARATOR)
        sb.append(bean.recentPage).append(Config.FIELD_SEPARATOR)
        sb.append(bean.ip).append(Config.FIELD_SEPARATOR)

        val gson: Gson = new Gson()
        val strDeviceInfo = gson.toJson(bean.deviceInfo)
        if (strDeviceInfo != null && !strDeviceInfo.equals("null"))
            sb.append(strDeviceInfo)
        sb.append(Config.FIELD_SEPARATOR)

        sb.toString()
    }

    /**
      * 字符串转换成APP日志对象
      *
      * @param msg
      * @return
      */
    def toApp(bUUID: Boolean = false)(msg: String): MobileLogMsg = {
        try {
            val fields: Array[String] = msg.split(Config.FIELD_SEPARATOR)
            val it: Iterator[String] = fields.toIterator

            val ja_version: String = it.next()
            val app_version: String = it.next()
            val appid: String = it.next()
            if(appid=="1208") println("appid==="+appid)
            val uuid: String = it.next()
            val userid: String = it.next()
            val logtype: String = it.next()
            val pageid: String = it.next()
            val subtype: String = it.next()
            val time: Long = it.next().toLong
            val localTime: Long = it.next().toLong
            val title: String = it.next()
            val data: String = it.next()
            val staytime: Long = it.next().toLong
            val visitindex: Int = it.next().toInt
            val recentPage: String = it.next().toString
            val ip: String = it.next()

            if (bUUID && uuid.length != 32)
                return null

            val deviceInfo: util.Map[String, Object] = JSON.parseObject(it.next(), classOf[util.Map[String, Object]])
            return MobileLogMsg(ja_version, app_version, appid, uuid, userid, logtype, pageid, subtype, time, localTime, title, data, staytime, visitindex, recentPage, ip, deviceInfo)
        } catch {
            case e: Exception => e.printStackTrace(); println(msg)
        }
        null
    }

    /**
      * 字符串转换成APP对应事件对象
      *
      * @param eventid
      * @param line
      * @return
      */
    def toAppEvent(eventid: String)(line: String): MobileLogMsg = {
        val bean = toApp(bUUID = true)(line)
        if (bean != null && bean.logtype == LogType.EVENT && bean.subtype.equals(eventid))
            bean
        else
            null
    }

    val toAppAll = toApp(bUUID = false) _
    val toAppAcc = toApp(bUUID = true) _
    val toAppPage = (line: String) => {
        val bean = toApp(bUUID = true)(line)
        if (bean != null && bean.logtype == LogType.PAGE)
            bean
        else
            null
    }
    val toAppEventAll = (line: String) => {
        val bean = toApp(bUUID = true)(line)
        if (bean != null && bean.logtype == LogType.EVENT)
            bean
        else
            null
    }

    /**
      * APP端 日志对象转换成线索对象
      *
      * @param bean
      * @return
      */
    def toUserClue(bean: MobileLogMsg): UserClue = {
        try {
            val dataJson = if (bean.data.contains("{")) JSON.parseObject(bean.data) else JSON.parseObject(URLDecoder.decode(bean.data, "UTF-8"))

            val clueId: String = UUID.randomUUID
            val appId: String = bean.appid
            val userid: String = bean.userid
            val uuid: String = bean.uuid
            val ip: String = bean.ip
            val cookieId: String = ""
            val deviceId: String = bean.dStrInfo("deviceid")
            val deviceName: String = bean.dStrInfo("model")
            val version: String = bean.app_version
            val time: Long = bean.time
            val name: String = {
                if (dataJson.containsKey("name"))
                    dataJson.getString("name")
                else if (dataJson.containsKey("nickname"))
                    dataJson.getString("nickname")
                else
                    ""
            }
            val phone: String = {
                if (dataJson.containsKey("phone"))
                    dataJson.getString("phone")
                else if (dataJson.containsKey("mobile"))
                    dataJson.getString("mobile")
                else
                    ""
            }
            val cityId: String = {
                if (dataJson.containsKey("city_id"))
                    dataJson.getString("city_id")
                else if (dataJson.containsKey("city"))
                    dataJson.getString("city")
                else
                    ""
            }
            val houseArea: String = {
                if (dataJson.containsKey("house_area"))
                    dataJson.getString("house_area")
                else if (dataJson.containsKey("area"))
                    dataJson.getString("area")
                else
                    ""
            }
            val style: String = if (dataJson.containsKey("style")) dataJson.getString("style") else ""
            val flatsName: String = if (dataJson.containsKey("flats_name")) dataJson.getString("flats_name") else ""

            UserClue(clueId, uuid, ip, appId, userid, cookieId, deviceId, deviceName, version, time, name, phone, cityId, houseArea, style, flatsName)
        } catch {
            case e: Exception => e.printStackTrace(); null
        }
    }
}
