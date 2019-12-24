
package com.wangjia.bigdata.core.handler

import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util
import java.util.Locale

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.gson.Gson
import com.wangjia.bigdata.core.bean.im.ImLogMsg
import com.wangjia.bigdata.core.bean.info.{AccountInfo, AppInfo, ProbeInfo}
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.bean.probe.ProbeLogMsg
import com.wangjia.bigdata.core.bean.web.WebLogMsg
import com.wangjia.bigdata.core.bean.weixin.WeixinLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.common._
import com.wangjia.utils.{DeviceIDUtils, EncrypUtils, ImeiUtils, JavaUtils}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/1/16.
  */
object LogClearHandler {

    private val probeTimeFormat = new ThreadLocal[SimpleDateFormat]() {
        override protected def initialValue: SimpleDateFormat = {
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        }
    }

    private val serverTimeFormat = new ThreadLocal[SimpleDateFormat]() {
        override protected def initialValue: SimpleDateFormat = {
            new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
        }
    }

    private val gsonThreadLocal = new ThreadLocal[Gson]() {
        override protected def initialValue: Gson = new Gson()
    }

    private val KEY = "7F76BA2B075CB3134A9A0D5023425E4D".getBytes

    private val DEF_IMEI: String = "000000000000000"

    /**
      * 校验USERID
      *
      * @param appInfo         APP配置信息帐号系统配置信息
      * @param mapAccountInfos
      * @param userid          USERID
      * @return 返回校验后的USERID 如果校验不过返回""
      */
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

    /**
      * 得到APP设备信息
      *
      * @param appType
      * @param comJson
      * @return
      */
    private def getAppDeviceId(appType: Int, comJson: JSONObject): String = {
        val device_uuid: String = comJson.getString("device_uuid")
        if (appType != DeviceType.DEVICE_PHONE_ANDROID)
            return device_uuid
        if (!comJson.containsKey("device_info"))
            return DeviceIDUtils.normalDeviceID(appType, device_uuid)
        val deviceInfoObj = comJson.getJSONObject("device_info")
        if (!deviceInfoObj.containsKey("imei") || !deviceInfoObj.containsKey("android_id"))
            return DeviceIDUtils.normalDeviceID(appType, device_uuid)
        val imei: String = deviceInfoObj.getString("imei")
        val androidid: String = deviceInfoObj.getString("android_id")
        if (imei.length == 14)
            return ImeiUtils.getImeiBy14(imei) + "#" + androidid
        if (imei.length == 15)
            return imei + "#" + androidid
        DeviceIDUtils.normalDeviceID(appType, device_uuid)
    }


    /**
      * 简析WEB端日志
      *
      * @param log
      * @param mapAppInfos
      * @param mapAccountInfos
      * @return
      */
    def clearWebLog(log: String,
                    mapAppInfos: mutable.Map[String, AppInfo],
                    mapAccountInfos: mutable.Map[Int, AccountInfo]): (Int, Any) = {
        //JSON简析
        val json = {
            try {
                JSON.parseObject(log.replace(Config.FIELD_SEPARATOR, "\\~").replace("\\x", "%"))
            } catch {
                case e: Exception => e.printStackTrace(); return (LogClearStatus.ERROR_JSON, log)
            }
        }

        try {
            if (!json.containsKey("id")
                    || !json.containsKey("type")
                    || !json.containsKey("ip")
                    || !json.containsKey("ref")
                    || !json.containsKey("url")
                    || !json.containsKey("time")) {
                return (LogClearStatus.ERROR_NOT_VITAL_FIELD, log)
            }

            val ja_version: String = "1.0.0"
            val appid: String = json.getString("id")
            val appInfo: AppInfo = mapAppInfos.getOrElse(appid, null)

            if (appInfo == null) {
                return (LogClearStatus.ERROR_NOT_APPID, log)
            }

            val ja_uuid: String = {
                var _ja_uuid = ""
                if (json.containsKey("feuuid"))
                    _ja_uuid = json.getString("feuuid")
                if (_ja_uuid == "" && json.containsKey("uuid"))
                    _ja_uuid = json.getString("uuid")
                _ja_uuid
            }


            if (DeviceIDUtils.isErrorBrowserID(ja_uuid))
                return (LogClearStatus.ERROR_JA_UUID, log)

            val logType: String = json.getString("type")
            val subtype: String = if (json.containsKey("sub_type")) json.getString("sub_type") else "1"
            val userid: String = if (json.containsKey("userid")) checkoutUserId(appInfo, mapAccountInfos, json.getString("userid")) else ""

            val ip: String = json.getString("ip").split(',')(0)

            val time = json.getLongValue("time")

            val ref: String = json.getString("ref")
            val url: String = json.getString("url")
            if (url.length < 10)
                return (LogClearStatus.ERROR_URL, log)

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

            (LogClearStatus.SUCCESS, bean)
        } catch {
            case e: Exception => e.printStackTrace(); (LogClearStatus.ERROR_JSON, log)
        }
    }

    /**
      * 简析Mobile端日志
      *
      * @param log
      * @param mapAppInfos
      * @param mapIdentifierAppInfos
      * @param mapAccountInfos
      * @return
      */
    def clearMobileLog(log: String,
                       mapAppInfos: mutable.Map[String, AppInfo],
                       mapIdentifierAppInfos: mutable.Map[String, AppInfo],
                       mapAccountInfos: mutable.Map[Int, AccountInfo]): (Int, Any) = {
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
                    return (LogClearStatus.ERROR_FORMAT, log)
                }
                //得到IP
                val ipStr = log.substring(0, ipIndex).split(',')(0)
                //得到加密数据
                var sIndex = log.indexOf("data=")
                if (sIndex <= 0)
                    return (LogClearStatus.ERROR_FORMAT, log)
                sIndex += "data=".length
                val eIndex = {
                    val len = log.indexOf(" HTTP", sIndex)
                    if (len > sIndex)
                        len
                    else
                        log.indexOf(" EOF", sIndex)
                }

                if (eIndex <= sIndex)
                    return (LogClearStatus.ERROR_FORMAT, log)
                val str = log.substring(sIndex, eIndex)
                //得到Json字符串
                if (!str.startsWith("TGR"))
                    return (LogClearStatus.ERROR_FORMAT, log)
                val urleStr = URLDecoder.decode(str, "UTF-8")
                val jStr = EncrypUtils.decrypt(urleStr, KEY)

                (ipStr, serverTime, jStr)
            } catch {
                case e: Exception => println(log); e.printStackTrace(); return (LogClearStatus.ERROR_FORMAT, log)
            }
        }
        //JSON简析
        val json = {
            try {
                JSON.parseObject(ipAndTimeAndJson._3)
            } catch {
                case e: Exception => e.printStackTrace(); return (LogClearStatus.ERROR_JSON, log)
            }
        }

        if (!json.containsKey("ja_version"))
            return clearMobileVdef(mapAppInfos, mapIdentifierAppInfos, mapAccountInfos, ipAndTimeAndJson._1, ipAndTimeAndJson._2, json, log)
        val ja_version: String = json.getString("ja_version")
        if (ja_version.startsWith("2."))
            return clearMobileV2(mapAppInfos, mapIdentifierAppInfos, mapAccountInfos, ipAndTimeAndJson._1, ipAndTimeAndJson._2, json, log)

        (LogClearStatus.ERROR_NOT_JA_VERSION, log)
    }


    def clearMobileLog_test(log: String,
                            mapAppInfos: mutable.Map[String, AppInfo],
                            mapIdentifierAppInfos: mutable.Map[String, AppInfo],
                            mapAccountInfos: mutable.Map[Int, AccountInfo]): (Int, Any) = {

        clearMobileLog(log, mapAppInfos, mapIdentifierAppInfos, mapAccountInfos)
    }


    /**
      * 移动端老版本日志简析
      *
      * @param mapAppInfos
      * @param mapIdentifierAppInfos
      * @param mapAccountInfos
      * @param ip
      * @param serverTime
      * @param json
      * @param log
      * @return
      */
    private def clearMobileVdef(mapAppInfos: mutable.Map[String, AppInfo],
                                mapIdentifierAppInfos: mutable.Map[String, AppInfo],
                                mapAccountInfos: mutable.Map[Int, AccountInfo],
                                ip: String,
                                serverTime: Long,
                                json: JSONObject,
                                log: String): (Int, Any) = {
        val list = new ListBuffer[MobileLogMsg]()

        try {
            val comJson = json.getJSONObject("common")
            val ja_version = "1.0.0"
            //app配置信息
            val appInfo: AppInfo = {
                var _info: AppInfo = null
                if (comJson.containsKey("app_identifier")) {
                    _info = mapIdentifierAppInfos.getOrElse(comJson.getString("app_identifier"), null)
                }
                if (_info == null && comJson.containsKey("app_id")) {
                    _info = mapIdentifierAppInfos.getOrElse(comJson.getString("app_id"), null)
                }
                _info
            }
            if (appInfo == null)
                return (LogClearStatus.ERROR_NOT_APP, log)
            //appid
            val appid: String = appInfo.appId
            //平台 ios/android
            val platform = {
                if (appInfo.appType == DeviceType.DEVICE_PHONE_IOS)
                    "ios"
                else if (appInfo.appType == DeviceType.DEVICE_PHONE_ANDROID)
                    "android"
                else
                    return (LogClearStatus.ERROR_NOT_KNOWN, log)
            }
            //版本号
            val app_version = comJson.getString("app_version")
            //渠道
            val channel = if (comJson.containsKey("app_channel")) comJson.getString("app_channel") else ""
            //设备型号
            val device = comJson.getString("device_model")
            //系统
            val system = comJson.getString("os_name") + " " + comJson.getString("os_version")
            //用户ID
            val userid = if (json.containsKey("wj_uid")) checkoutUserId(appInfo, mapAccountInfos, json.getString("wj_uid")) else ""
            //设备ID
            val deviceid = getAppDeviceId(appInfo.appType, comJson)
            //MAC地址
            val mac = if (comJson.containsKey("mac_id")) comJson.getString("mac_id").replaceAll(":", "").toLowerCase else ""
            //唯一ID
            val uuid = {
                if (appInfo.appType == DeviceType.DEVICE_PHONE_IOS)
                    DeviceIDUtils.iosIDFA2UUID(deviceid)
                else if (appInfo.appType == DeviceType.DEVICE_PHONE_ANDROID)
                    DeviceIDUtils.androidID2UUID(deviceid)
                else
                    ""
            }
            //网络类型
            val net = comJson.getString("net_type")
            //宽高
            val wxh = comJson.getString("xy_screen").split('x')
            val sw = wxh(0).toInt
            val sh = wxh(1).toInt
            //事件
            val events = json.getJSONArray("events")
            val em = events.size()
            var ei = 0
            //设备信息
            val deviceInfo: util.Map[String, Object] = new util.HashMap[String, Object]()
            deviceInfo.put("screenWidth", new Integer(sw))
            deviceInfo.put("screenHeight", new Integer(sh))
            //deviceInfo.put("language", "xxx")
            deviceInfo.put("platform", platform)
            deviceInfo.put("networkType", net)
            //deviceInfo.put("brand", "xxx")
            deviceInfo.put("model", device)
            deviceInfo.put("system", system)
            deviceInfo.put("channel", channel)

            if (mac.length > 0)
                deviceInfo.put("mac", mac)
            if (appInfo.appType == DeviceType.DEVICE_PHONE_IOS)
                deviceInfo.put("idfa", deviceid)
            else if (appInfo.appType == DeviceType.DEVICE_PHONE_ANDROID) {
                val imei = DeviceIDUtils.getAndroidImei(deviceid)
                if (imei != null && !DeviceIDUtils.isErrorIMEI(imei))
                    deviceInfo.put("imei", imei)
                val systemid = DeviceIDUtils.getAndroidSystemId(deviceid)
                if (systemid != null && !DeviceIDUtils.isErrorAndroidID(systemid))
                    deviceInfo.put("systemid", systemid)
            }
            deviceInfo.put("deviceid", deviceid)

            while (ei < em) {
                val obj = events.getJSONObject(ei)
                //时间戳
                val localTime = obj.getLongValue("time")
                val sTime: Long = localTime
                //页面ID
                val pageid = if (obj.containsKey("id")) obj.getString("id") else ""

                //data
                val sData = if (obj.containsKey("data") && obj.get("data") != null) obj.getJSONObject("data").toJSONString else ""

                //title
                val title = {
                    if (sData != "") {
                        val data = obj.getJSONObject("data")
                        if (data.containsKey("title"))
                            data.getString("title")
                        else
                            ""
                    } else ""
                }

                //日志类型 10应用事件 20用户事件 30页面事件 40自定义事件 50页面跳转
                val logType: String = obj.getString("type")

                //子类型
                val subtype: String = if (obj.containsKey("sub_type")) obj.getString("sub_type") else "1"
                logType match {
                    //APP事件
                    case "10" => {
                        val eid = {
                            if (subtype == "1") //应用事件前台
                                LogEventId.APP_ENTER_FOREGROUND
                            else if (subtype == "2") //应用事件后台
                                LogEventId.APP_ENTER_BACKGROUND
                            else
                                null
                        }
                        if (eid != null) {
                            list += MobileLogMsg(ja_version, app_version, appid, uuid, userid, LogType.EVENT, pageid, eid, sTime, localTime, title, sData, 0, 0, "", ip, deviceInfo)
                        }
                    }

                    //用户事件
                    case "20" => {
                        val eid = {
                            if (subtype == "1") //用户事件登录
                                LogEventId.USER_LOGIN
                            else if (subtype == "2") //用户事件登出
                                LogEventId.USER_LOGOUT
                            else if (subtype == "3") //用户事件注册
                                LogEventId.USER_REGISTER
                            else
                                null
                        }
                        if (eid != null) {
                            val mapData: util.Map[String, Object] = new util.HashMap[String, Object]()
                            val userid = if (obj.containsKey("base_user_id")) obj.getString("base_user_id") else ""
                            val auth_token = if (obj.containsKey("auth_token")) obj.getString("auth_token") else ""
                            val channel = {
                                val _channel = if (obj.containsKey("channel")) obj.getString("channel") else ""
                                _channel match {
                                    case "tencent" => UserChannel.TENCENT
                                    case "weixin" => UserChannel.WEIXIN
                                    case "telcode" => UserChannel.PHONE
                                    case "weibo" => UserChannel.WEIBO
                                    case "telpass" => UserChannel.REGISTER
                                    case _ => _channel
                                }

                            }
                            val username = if (obj.containsKey("username")) obj.getString("username") else ""
                            mapData.put("userid", userid)
                            mapData.put("token", auth_token)
                            mapData.put("channel", channel)
                            mapData.put("username", username)
                            val jdata = gsonThreadLocal.get().toJson(mapData)
                            list += MobileLogMsg(ja_version, app_version, appid, uuid, userid, LogType.EVENT, pageid, eid, sTime, localTime, title, jdata, 0, 0, "", ip, deviceInfo)
                        }
                    }

                    //页面点击
                    case "30" if (subtype == "1" && obj.containsKey("x_y")) => {
                        //坐标x_y 只有点击事件时有值
                        val point = obj.getString("x_y").split('x')
                        val x = wxh(0).toInt
                        val y = wxh(1).toInt

                        val orientation = "landscape"
                        val mapData: util.Map[String, Object] = new util.HashMap[String, Object]()
                        mapData.put("orientation", orientation)
                        mapData.put("x", new Integer(x))
                        mapData.put("y", new Integer(y))
                        val jdata = gsonThreadLocal.get().toJson(mapData)
                        list += MobileLogMsg(ja_version, app_version, appid, uuid, userid, LogType.EVENT, pageid, LogEventId.UI_CLICK_POINT, sTime, localTime, title, jdata, 0, 0, "", ip, deviceInfo)
                    }

                    //自定义事件
                    case "40" => {
                        if (subtype == "location") {
                            //自定义事件GPS
                            val jdata = obj.getJSONObject("data")
                            jdata.put("decare", GPSDecare.BAIDU)
                            list += MobileLogMsg(ja_version, app_version, appid, uuid, userid, LogType.EVENT, pageid, LogEventId.APP_LOCATION, sTime, localTime, title, jdata.toJSONString, 0, 0, "", ip, deviceInfo)
                        } else if (subtype == "all_app_info") {
                            //自定义事件应用列表
                            list += MobileLogMsg(ja_version, app_version, appid, uuid, userid, LogType.EVENT, pageid, LogEventId.APP_APPLIST, sTime, localTime, title, sData, 0, 0, "", ip, deviceInfo)
                        } else if (subtype == "get_user_club" || subtype == "get_user_clue" || subtype == "commit_free_design") {
                            //自定义事件提交线索
                            list += MobileLogMsg(ja_version, app_version, appid, uuid, userid, LogType.EVENT, pageid, LogEventId.USER_COMMIT_CLUE, sTime, localTime, title, sData, 0, 0, "", ip, deviceInfo)
                        } else if (subtype == "all_contact_info") {
                            //自定义事件联系人
                            list += MobileLogMsg(ja_version, app_version, appid, uuid, userid, LogType.EVENT, pageid, LogEventId.APP_CONTACT_LIST, sTime, localTime, title, sData, 0, 0, "", ip, deviceInfo)
                        }
                        else {
                            //其它事件
                            list += MobileLogMsg(ja_version, app_version, appid, uuid, userid, LogType.EVENT, pageid, subtype, sTime, localTime, title, sData, 0, 0, "", ip, deviceInfo)
                        }
                    }

                    //页面跳转
                    case "50" if (subtype == "1") => {
                        val open_time: Long = if (obj.containsKey("open_time")) obj.getLongValue("open_time") else 0L
                        val staytime = sTime - open_time
                        if (open_time > 0 && staytime > 0 && staytime < 600000)
                            list += MobileLogMsg(ja_version, app_version, appid, uuid, userid, LogType.PAGE, pageid, "1", open_time, open_time, title, sData, staytime, 0, "", ip, deviceInfo)
                    }

                    //其余情况不处理
                    case _ =>
                }
                ei += 1
            }
        } catch {
            case e: Exception => e.printStackTrace(); println(json.toJSONString); return (LogClearStatus.ERROR_JSON, log)
        }
        (LogClearStatus.SUCCESS, list)
    }

    /**
      * 移动端v2版日志简析
      *
      * @param mapAppInfos
      * @param mapIdentifierAppInfos
      * @param mapAccountInfos
      * @param ip
      * @param serverTime
      * @param json
      * @param log
      * @return
      */
    private def clearMobileV2(mapAppInfos: mutable.Map[String, AppInfo],
                              mapIdentifierAppInfos: mutable.Map[String, AppInfo],
                              mapAccountInfos: mutable.Map[Int, AccountInfo],
                              ip: String,
                              serverTime: Long,
                              json: JSONObject,
                              log: String): (Int, Any) = {
        val list = new ListBuffer[MobileLogMsg]()
        try {
            if (!json.containsKey("device_info")
                    || !json.containsKey("ja_version")
                    || !json.containsKey("app_packagename")
                    || !json.containsKey("appid")
                    || !json.containsKey("app_version")
                    || !json.containsKey("logtype")
                    || !json.containsKey("subtype")
                    || !json.containsKey("pageid")
                    || !json.containsKey("time")) {
                return (LogClearStatus.ERROR_NOT_VITAL_FIELD, log)
            }
            val ja_version: String = json.getString("ja_version")
            val app_version: String = json.getString("app_version")
            val deviceInfo: JSONObject = json.getJSONObject("device_info")
            val appInfo: AppInfo = mapAppInfos.getOrElse(json.getString("appid"), null)
            if (appInfo == null)
                return (LogClearStatus.ERROR_NOT_APP, log)
            //appid
            val appid: String = appInfo.appId
            //平台 ios/android
            val platform = {
                if (appInfo.appType == DeviceType.DEVICE_PHONE_IOS)
                    "ios"
                else if (appInfo.appType == DeviceType.DEVICE_PHONE_ANDROID)
                    "android"
                else
                    return (LogClearStatus.ERROR_NOT_KNOWN, log)
            }
            val userid: String = if (json.containsKey("userid")) json.getString("userid") else ""
            val logtype: String = json.getString("logtype")
            val pageid: String = json.getString("pageid")
            val subtype: String = json.getString("subtype")
            val time: Long = serverTime
            val localTime: Long = json.getLongValue("time")
            val title: String = if (json.containsKey("title")) json.getString("title") else ""
            val data: String = if (json.containsKey("data")) json.getString("data") else ""
            val staytime: Long = 0
            val visitindex: Int = -1
            val recentPage: String = ""

            //设备ID
            val deviceid: String = {
                if (appInfo.appType == DeviceType.DEVICE_PHONE_IOS) {
                    if (deviceInfo.containsKey("idfa"))
                        deviceInfo.getString("idfa")
                    else
                        return (LogClearStatus.ERROR_DEVICEID, log)
                } else if (appInfo.appType == DeviceType.DEVICE_PHONE_ANDROID) {
                    val imei: String = {
                        val _imei: String = if (deviceInfo.containsKey("imei")) deviceInfo.getString("imei") else DEF_IMEI
                        if (_imei.nonEmpty && _imei.length >= 14 && _imei.length <= 15)
                            _imei
                        else
                            DEF_IMEI
                    }
                    val androidid: String = deviceInfo.getString("android_id")
                    if (androidid != null && !DeviceIDUtils.isErrorAndroidID(androidid))
                        deviceInfo.put("systemid", androidid)
                    if (imei.length == 14)
                        ImeiUtils.getImeiBy14(imei) + "#" + androidid
                    else if (imei.length == 15)
                        imei + "#" + androidid
                    else
                        return (LogClearStatus.ERROR_DEVICEID, log)
                } else
                    return (LogClearStatus.ERROR_DEVICEID, log)
            }
            //唯一ID
            val uuid = {
                if (appInfo.appType == DeviceType.DEVICE_PHONE_IOS)
                    DeviceIDUtils.iosIDFA2UUID(deviceid)
                else if (appInfo.appType == DeviceType.DEVICE_PHONE_ANDROID)
                    DeviceIDUtils.androidID2UUID(deviceid)
                else
                    ""
            }

            val mapDeviceInfo = new util.HashMap[String, Object]()
            mapDeviceInfo.put("app_packagename", json.getString("app_packagename"))
            mapDeviceInfo.put("screenWidth", deviceInfo.getInteger("screenWidth"))
            mapDeviceInfo.put("screenHeight", deviceInfo.getInteger("screenHeight"))
            mapDeviceInfo.put("language", deviceInfo.getString("language"))
            mapDeviceInfo.put("platform", platform)
            mapDeviceInfo.put("networkType", if (deviceInfo.containsKey("networkType")) deviceInfo.getString("networkType") else "-")
            mapDeviceInfo.put("brand", deviceInfo.getString("brand"))
            mapDeviceInfo.put("model", deviceInfo.getString("model"))
            mapDeviceInfo.put("system", deviceInfo.getString("system") + " " + deviceInfo.getString("version"))
            if (deviceInfo.containsKey("imei"))
                mapDeviceInfo.put("imei", deviceInfo.getString("imei"))
            if (deviceInfo.containsKey("wlan_address"))
                mapDeviceInfo.put("mac", deviceInfo.getString("wlan_address"))
            if (deviceInfo.containsKey("systemid")) {
                mapDeviceInfo.put("systemid", deviceInfo.getString("systemid"))
                mapDeviceInfo.put("android_id", deviceInfo.getString("systemid"))
            }
            mapDeviceInfo.put("channel", deviceInfo.getString("channel"))
            mapDeviceInfo.put("deviceid", deviceid)

            list += MobileLogMsg(ja_version, app_version, appid, uuid, userid, logtype, pageid, subtype, time, localTime, title, data, staytime, visitindex, recentPage, ip, mapDeviceInfo)
        } catch {
            case e: Exception => e.printStackTrace(); println(json.toJSONString); return (LogClearStatus.ERROR_JSON, log)
        }
        (LogClearStatus.SUCCESS, list)
    }

    /**
      * 简析微信端日志
      *
      * @param log
      * @param mapAppInfos
      * @param mapIdentifierAppInfos
      * @return
      */
    def clearWeiXinLog(log: String,
                       mapAppInfos: mutable.Map[String, AppInfo],
                       mapIdentifierAppInfos: mutable.Map[String, AppInfo]): (Int, Any) = {
        //基础转换 IP 服务器时间 日志
        val ipAndTimeAndJson: (String, Long, String) = {
            try {
                //得到IP
                val ipEndIndex = log.indexOf(" - - [")
                val ip = log.substring(0, ipEndIndex).split(',')(0)

                //得到服务器时间
                val timeStartIndex = log.indexOf("[timestamp:", ipEndIndex)
                val timeEndIndex = log.indexOf("] ", timeStartIndex)
                val timeStr: String = log.substring(timeStartIndex + 11, timeEndIndex)
                val serverTime: Long = (timeStr.toDouble * 1000).toLong

                //得到数据体
                val newLog: String = {
                    val _log = log.replace("\\x22", "\"").replace("\\x", "%")
                    URLDecoder.decode(_log, "UTF-8")
                }
                val dataStartIndex = newLog.indexOf("\"\"{\"") + 2
                val dataEndIndex = newLog.indexOf(" EOF\"", dataStartIndex)
                val jsonStr = newLog.substring(dataStartIndex, dataEndIndex)
                if (jsonStr.length < 100) {
                    return (LogClearStatus.ERROR_FORMAT, log)
                }
                (ip, serverTime, jsonStr)
            } catch {
                case e: Exception => e.printStackTrace();
                    return (LogClearStatus.ERROR_FORMAT, log)
            }
        }

        //JSON简析
        val json = {
            try {
                JSON.parseObject(ipAndTimeAndJson._3)
            } catch {
                case e: Exception => e.printStackTrace();
                    return (LogClearStatus.ERROR_JSON, log)
            }
        }

        try {
            if (!json.containsKey("device_info")
                    || !json.containsKey("ja_version")
                    || !json.containsKey("app_identifier")
                    || !json.containsKey("app_version")
                    || !json.containsKey("logtype")
                    || !json.containsKey("subtype")
                    || !json.containsKey("pageid")
                    || !json.containsKey("time")
                    || !json.containsKey("data")) {
                return (LogClearStatus.ERROR_NOT_VITAL_FIELD, log)
            }

            val ja_version: String = json.getString("ja_version")
            val app_version: String = json.getString("app_version")
            val deviceInfo: util.Map[String, Object] = json.getObject("device_info", classOf[util.Map[String, Object]])
            val appid: String = {
                val app_identifier = json.getString("app_identifier")
                if (app_identifier == "null") {
                    val _appid = json.getString("appid")
                    if (!mapAppInfos.contains(_appid))
                        return (LogClearStatus.ERROR_NOT_APPID, log)
                    _appid
                } else {
                    val appInfo = mapIdentifierAppInfos.getOrElse(app_identifier, null)
                    if (appInfo == null)
                        return (LogClearStatus.ERROR_NOT_IDENTIFIER, log)
                    appInfo.appId
                }
            }
            val uuid: String = {
                if (!deviceInfo.containsKey("ja_uuid"))
                    return (LogClearStatus.ERROR_NOT_VITAL_FIELD, log)
                val ja_uuid = deviceInfo.get("ja_uuid").asInstanceOf[String]
                if (ja_uuid == null || DeviceIDUtils.isErrorBrowserID(ja_uuid))
                    return (LogClearStatus.ERROR_NOT_VITAL_FIELD, log)
                DeviceIDUtils.cookie2UUID(ja_uuid)
            }
            val userid: String = if (json.containsKey("userid")) json.getString("userid") else ""
            val logtype: String = json.getString("logtype")
            val pageid: String = json.getString("pageid")
            val subtype: String = json.getString("subtype")
            val time: Long = ipAndTimeAndJson._2
            val localTime: Long = json.getLongValue("time")
            val title: String = {
                val _title = json.getString("title")
                if (_title == null)
                    ""
                else
                    _title
            }
            val data: String = {
                try {
                    val _str = json.getString("data")
                    val jobj = JSON.parseObject(_str)
                    if (jobj != null)
                        jobj.toJSONString
                    else
                        ""
                } catch {
                    case e: Exception => e.printStackTrace(); ""
                }
            }
            val staytime: Long = 0
            val visitindex: Int = -1
            val recentPage: String = ""
            val ip: String = ipAndTimeAndJson._1

            val bean = WeixinLogMsg(ja_version, app_version, appid, uuid, userid, logtype, pageid, subtype, time, localTime, title, data, staytime, visitindex, recentPage, ip, deviceInfo)

            (LogClearStatus.SUCCESS, bean)
        } catch {
            case e: Exception => e.printStackTrace(); (LogClearStatus.ERROR_JSON, log)
        }
    }

    /**
      * 简析探针日志
      *
      * @param mapProbeInfos
      * @param log
      * @return
      */
    def clearProbeLog(log: String,
                      mapProbeInfos: mutable.Map[String, ProbeInfo]): (Int, Any) = {
        try {
            val fields = log.split("\\|")
            if (fields.size != 5)
                return (LogClearStatus.ERROR_FORMAT, log)
            if (fields(2) == "000")
                return (LogClearStatus.ERROR_FORMAT, log)
            val probeMac = fields(0)
            val info = mapProbeInfos.getOrElse(probeMac, null)
            if (info == null)
                return (LogClearStatus.ERROR_NOT_APP, log)
            val id = info.probeId
            val mac = fields(1)
            val signal = fields(2).trim.toInt
            if (math.abs(signal) >= info.signal)
                return (LogClearStatus.ERROR_PROBE_LACK_SIGNAL, log)
            val frameType = fields(3)
            val time = probeTimeFormat.get().parse(fields(4)).getTime
            val venueId = info.venueId
            (LogClearStatus.SUCCESS, ProbeLogMsg(venueId, id, probeMac, mac, signal, frameType, time))
        } catch {
            case e: Exception => println(log); e.printStackTrace(); (LogClearStatus.ERROR_NOT_KNOWN, log)
        }
    }

    /**
      * 简析IM日志
      *
      * @param log
      * @return
      */
    def clearImLog(log: String): (Int, Any) = {
        //得到JSON数据体
        val jsonStr: String = {
            try {
                val newLog: String = log.replace("\\x22", "\"")
                val dataStartIndex = newLog.indexOf("\"\"{\"") + 2
                val dataEndIndex = newLog.indexOf(" EOF\"", dataStartIndex)
                val dataStr = newLog.substring(dataStartIndex, dataEndIndex)
                val jStr = URLDecoder.decode(dataStr.replace("\\x", "%"), "UTF-8")
                if (jStr.length < 100) {
                    return (LogClearStatus.ERROR_FORMAT, log)
                }
                jStr
            } catch {
                case e: Exception => e.printStackTrace();
                    return (LogClearStatus.ERROR_FORMAT, log)
            }
        }

        try {
            val json: JSONObject = JSON.parseObject(jsonStr)

            val ja_version: String = if (json.containsKey("ja_version")) json.getString("ja_version") else "1.0.0"
            val client_id: String = json.getString("client_id")
            val client: String = json.getString("client")
            if (client_id == "" || client == "")
                return (LogClearStatus.ERROR_NOT_VITAL_FIELD, log)
            val customer: String = json.getString("customer")
            val msgtype: String = json.getString("type")
            val from_type: Int = json.getIntValue("from_type")
            val time: Long = json.getLongValue("time")
            val localTime: Long = json.getLongValue("c_time")
            val content: String = json.getString("content")
            val client_ip: String = if (json.containsKey("user_ip")) json.getString("user_ip") else ""
            val source: String = if (json.containsKey("source")) json.getString("source") else ""
            val avatar: String = if (json.containsKey("avatar")) json.getString("avatar") else ""

            (LogClearStatus.SUCCESS, ImLogMsg(ja_version, client_id, client, customer, msgtype, from_type, time, localTime, content, client_ip, source, avatar))
        } catch {
            case e: Exception => println(log); e.printStackTrace(); (LogClearStatus.ERROR_JSON, log)
        }
    }
}