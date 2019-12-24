package com.wangjia.bigdata.core

import java.net.URLDecoder
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wangjia.bigdata.core.bean.info.{AccountInfo, AppInfo}
import com.wangjia.bigdata.core.bean.web.WebLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.LogClearStatus
import com.wangjia.bigdata.core.utils.JAInfoUtils
import com.wangjia.common.{LogEventId, LogType}
import com.wangjia.utils.{DeviceIDUtils, JavaUtils}

import scala.collection.mutable

/**
  * Created by Cfb on 2018/3/7.
  */
object testLog extends App{
val log ="{\"id\":\"1001\",\"ip\":\"119.130.215.108\",\"date\":\"04/Mar/2018:00:00:34 +0800\",\"uuid\":\"d63f3161f9bfc2b8f9e03145ca31a0de\",\"feuuid\":\"d63f3161f9bfc2b8f9e03145ca31a0de\",\"userid\":\"\",\"url\":\"https://pic.jiajuol.com/picview_458735_5_0_0.html\",\"ref\":\"\",\"sw\":1280,\"sh\":1024,\"agnet\":\"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)\",\"cookie\":\"1\",\"flash\":\"0\",\"title\":\"%E4%B9%A6%E6%88%BF%E8%BE%83%E4%B8%BA%E8%A7%84%E6%95%B4%EF%BC%8C%E4%B9%9F%E8%AE%BE%E8%AE%A1%E5%BE%97%E5%8D%81%E5%88%86%E5%B9%B2%E7%BB%83%EF%BC%8C%E4%B9%A6%E6%9F%9C%E5%92%8C%E4%B9%A6%E6%A1%8C%E7%BB%9F%E4%B8%80%E9%87%87%E7%94%A8%E6%9E%AB%E6%9C%A8%E6%89%93%E9%80%A0%EF%BC%8C%E6%B8%A9%E6%B6%A6%E5%8E%9A%E5%AE%9E%E7%9A%84%E6%9D%90%E8%B4%A8%E7%AA%81%E6%98%BE%E5%87%BA%E4%B8%BB%E4%BA%BA%E5%AF%B9%E8%B4%A8%E6%84%9F%E7%9A%84%E8%BF%BD%E6%B1%82%EF%BC%9B%E7%BA%A2%E8%89%B2%E6%A4%85%E5%AD%90%E4%B8%8E%E9%BB%84%E8%89%B2%E8%8A%B1%E5%9E%8B%E5%9C%B0%E6%AF%AF%E5%BD%A2%E6%88%90%E7%BE%8E%E5%BC%8F%E9%9B%8D%E5%AE%B9%E5%8D%8E%E7%BE%8E%E7%9A%84%E8%89%BA%E6%9C%AF%EF%BC%8C%E8%AE%A9%E4%BA%BA%E5%80%8D%E6%84%9F%E6%B8%A9%E9%A6%A8%E3%80%82_%E4%B9%A6%E6%88%BF_458735-%E5%AE%B6%E5%B1%85%E5%9C%A8%E7%BA%BF%E8%A3%85%E4%BF%AE%E6%95%88%E6%9E%9C%E5%9B%BE\",\"type\":30,\"time\":1520092834912,\"is_people\":1}"
    //JSON简析
    val json:JSONObject = {
            JSON.parseObject(log.replace(Config.FIELD_SEPARATOR, "\\~").replace("\\x", "%"))
    }
    val mapAppInfos = JAInfoUtils.loadAppInfos
    val mapAccountInfos = JAInfoUtils.loadAccountInfos

    val ja_version: String = "1.0.0"
    val appid: String = json.getString("id")
    val appInfo: AppInfo = mapAppInfos.getOrElse(appid, null)

    if (appInfo == null) {
        println("空")
//        return (LogClearStatus.ERROR_NOT_APPID, log)
    }

    val ja_uuid: String = {
        if (json.containsKey("feuuid"))
            json.getString("feuuid")
        else if (json.containsKey("uuid"))
            json.getString("uuid")
        else
            ""
    }

    if (DeviceIDUtils.isErrorBrowserID(ja_uuid))
       println(LogClearStatus.ERROR_JA_UUID, log)

    val logType: String = json.getString("type")
    val subtype: String = if (json.containsKey("sub_type")) json.getString("sub_type") else "1"
    val userid: String = if (json.containsKey("userid")) checkoutUserId(appInfo, mapAccountInfos, json.getString("userid")) else ""

    val ip: String = json.getString("ip").split(',')(0)

    val time = json.getLongValue("time")

    val ref: String = json.getString("ref")
    val url: String = json.getString("url")
    if (url.length < 10)
        println("url.length < 10")

    val title = {
        val str = json.getString("title")
        try {
            URLDecoder.decode(str, "UTF-8").replace("`", "|")
        } catch {
            case e: Exception => str
        }
    }

    val uuid: String = DeviceIDUtils.cookie2UUID(ja_uuid)

    val sw: Int = json.getIntValue("sw")
    val sh: Int = json.getIntValue("sh")
    val agent: String = json.getString("agnet")
    val system: String = JavaUtils.parseSystemInfo(agent)
    val browser: String = JavaUtils.parseBrowserInfo(agent)

    val bCookie: Int = if (json.containsKey("cookie")) json.getIntValue("cookie") else 0
    val bFlash: Int = if (json.containsKey("flash")) json.getIntValue("flash") else 0
    val acc: Int = if (json.containsKey("is_people")) json.getIntValue("is_people") else 0
    val data: String = if (json.containsKey("data")) URLDecoder.decode(json.getString("data"), "UTF-8") else ""
    val language: String = if (json.containsKey("language")) json.getString("language") else ""

    val visitindex: Int = -1
    val staytime: Long = 0

    val deviceInfo: util.Map[String, Object] = new util.HashMap[String, Object]()
    deviceInfo.put("screenWidth", new Integer(sw))
    deviceInfo.put("screenHeight", new Integer(sh))
    deviceInfo.put("language", language)
    deviceInfo.put("system", system)
    deviceInfo.put("browser", browser)
    deviceInfo.put("agent", agent)
    deviceInfo.put("status", new Integer(200))
    deviceInfo.put("bcookie", new Integer(bCookie))
    deviceInfo.put("bflash", new Integer(bFlash))
    deviceInfo.put("ja_uuid", ja_uuid)

    val bean: WebLogMsg = logType match {
        case "20" =>
            //登录
            if (subtype.equals("1"))
                WebLogMsg(ja_version, appid, uuid, userid, LogType.EVENT, LogEventId.USER_LOGIN, ref, url, time, title, data, staytime, visitindex, ip, acc, deviceInfo)
            //注册
            else if (subtype.equals("3"))
                WebLogMsg(ja_version, appid, uuid, userid, LogType.EVENT, LogEventId.USER_REGISTER, ref, url, time, title, data, staytime, visitindex, ip, acc, deviceInfo)
            //登出
            else
                WebLogMsg(ja_version, appid, uuid, userid, LogType.EVENT, LogEventId.USER_LOGOUT, ref, url, time, title, data, staytime, visitindex, ip, acc, deviceInfo)
        case "30" =>
            WebLogMsg(ja_version, appid, uuid, userid, LogType.PAGE, subtype, ref, url, time, title, data, staytime, visitindex, ip, acc, deviceInfo)
        case "40" =>
            //线索事件
            if (subtype.equals("get_user_club") || subtype.equals("get_user_clue"))
                WebLogMsg(ja_version, appid, uuid, userid, logType, LogEventId.USER_COMMIT_CLUE, ref, url, time, title, data, staytime, visitindex, ip, acc, deviceInfo)
            else
                WebLogMsg(ja_version, appid, uuid, userid, logType, subtype, ref, url, time, title, data, staytime, visitindex, ip, acc, deviceInfo)


    }





    println()

    def checkoutUserId(appInfo: AppInfo,
                       mapAccountInfos: mutable.Map[Int, AccountInfo],
                       userid: String): String = {
        val accountInfo = mapAccountInfos.getOrElse(appInfo.accountId, null)
        if (accountInfo == null)
            return ""
        if (accountInfo.isMeet(userid))
            return userid
        ""
    }
}
