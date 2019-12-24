package com.wangjia.bigdata.core.handler

import java.util.regex.Pattern

import com.wangjia.bean.DeviceDes
import com.wangjia.bigdata.core.bean.info.AppInfo
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.bean.web.WebLogMsg
import com.wangjia.common.{DeviceType, LogEventId, LogType}
import com.wangjia.handler.idcenter.IDType
import com.wangjia.utils.DeviceIDUtils

import scala.collection.mutable

/**
  * 更新计算设备信息
  *
  * Created by Administrator on 2018/2/28.
  */
object DeviceDesHandler {

    //提取手机号
    private val pattern = Pattern.compile("((13[0-9])|(14[5|7])|(15([0-3]|[5-9]))|(18[0-9])|(17[0-9]))\\d{8}")

    /**
      * 更新WEB端设备信息
      *
      * @param uuid
      * @param _des
      * @param logMsgs
      * @param mapAppInfos
      * @return
      */
    def updateWebDeviceDes(uuid: String, _des: DeviceDes, logMsgs: List[WebLogMsg], mapAppInfos: mutable.Map[String, AppInfo]): DeviceDes = {
        val des = if (_des == null) new DeviceDes() else _des
        //设备APP信息
        val appid2time: mutable.Map[String, Long] = mutable.Map[String, Long]()
        //设置基础信息
        val head = logMsgs.head
        var minTime: Long = head.time
        var maxTime: Long = head.time
        var acc: Int = head.acc
        logMsgs.foreach(b => {
            if (b.time < minTime)
                minTime = b.time
            if (b.time > maxTime)
                maxTime = b.time
            if (b.acc == 1)
                acc = 1

            //APPID
            if (appid2time.getOrElse(b.appid, java.lang.Long.MAX_VALUE) > b.time)
                appid2time.put(b.appid, b.time)
            //PHONE
            if (b.logtype == LogType.EVENT && b.subtype == LogEventId.USER_COMMIT_CLUE) {
                val matcher = pattern.matcher(b.data)
                while (matcher.find()) {
                    des.addPhone(matcher.group(), b.time)
                }
            }
            //UserID
            val appInfo = mapAppInfos.getOrElse(b.appid, null)
            if (appInfo != null) {
                val userid = b.userid
                if (userid.length > 2) {
                    des.addUserId(appInfo.accountId + "#" + userid, b.time)
                }
            }
        })

        val deviceid: String = head.dStrInfo("ja_uuid")

        des.setFirstTime(minTime)
        des.setLastTime(maxTime)
        des.setDeviceId(deviceid)
        des.setUuid(head.uuid)
        des.setType(DeviceType.DEVICE_PC_BROWSER)
        des.setName(head.dStrInfo("browser"))

        des.setSystem(head.dStrInfo("system"))
        des.setSw(head.dIntInfo("screenWidth"))
        des.setSh(head.dIntInfo("screenHeight"))
        if (acc == 1)
            des.setAcc(acc)
        des.setbCookie(head.dIntInfo("bcookie"))
        des.setbFlash(head.dIntInfo("bflash"))

        //id
        des.addId(IDType.ID_COOKIE, deviceid, head.time)

        //添加APPID
        appid2time.foreach(x => des.addAppId(x._1, x._2))

        des
    }


    /**
      * 更新APP端设备信息
      *
      * @param uuid
      * @param _des
      * @param logMsgs
      * @param mapAppInfos
      * @return
      */
    def updateAppDeviceDes(uuid: String, _des: DeviceDes, logMsgs: List[MobileLogMsg], mapAppInfos: mutable.Map[String, AppInfo]): DeviceDes = {
        val des = if (_des == null) new DeviceDes() else _des
        //设备APP信息
        val appid2time: mutable.Map[String, Long] = mutable.Map[String, Long]()
        //设置基础信息
        val head = logMsgs.head
        var minTime: Long = head.time
        var maxTime: Long = head.time
        val acc: Int = 1
        logMsgs.foreach(b => {
            if (b.time < minTime)
                minTime = b.time
            if (b.time > maxTime)
                maxTime = b.time
            //APPID
            if (appid2time.getOrElse(b.appid, java.lang.Long.MAX_VALUE) > b.time)
                appid2time.put(b.appid, b.time)
            //PHONE
            if (b.logtype == LogType.EVENT && b.subtype == LogEventId.USER_COMMIT_CLUE) {
                val matcher = pattern.matcher(b.data)
                while (matcher.find()) {
                    des.addPhone(matcher.group(), b.time)
                }
            }
            //UserID
            val appInfo = mapAppInfos.getOrElse(b.appid, null)
            if (appInfo != null) {
                val userid = b.userid
                if (userid.length > 2) {
                    des.addUserId(appInfo.accountId + "#" + userid, b.time)
                }
            }
        })
        des.setFirstTime(minTime)
        des.setLastTime(maxTime)
        des.setUuid(head.uuid)

        //mac
        val mac: String = head.dStrInfo("mac")
        if (mac.length > 0)
            des.addId(IDType.ID_MAC, mac, head.time)
        //设备信息
        val deviceid: String = head.dStrInfo("deviceid")
        val platform: String = head.dStrInfo("platform")
        if (platform.equals("android")) {
            des.setType(DeviceType.DEVICE_PHONE_ANDROID)
            des.addId(IDType.ID_ANDROID_IMEI_SYSTEMID, deviceid, head.time)
            val imei: String = DeviceIDUtils.getAndroidImei(deviceid)
            if (imei != null && !DeviceIDUtils.isErrorIMEI(imei))
                des.addId(IDType.ID_ANDROID_IMEI, imei, head.time)
            val systemId: String = DeviceIDUtils.getAndroidSystemId(deviceid)
            if (systemId != null && !DeviceIDUtils.isErrorSystemId(systemId))
                des.addId(IDType.ID_ANDROID_SYSTEMID, systemId, head.time)
        } else if (platform.equals("ios")) {
            des.setType(DeviceType.DEVICE_PHONE_IOS)
            if (!DeviceIDUtils.isErrorIOSIDFA(deviceid)) {
                des.addId(IDType.ID_IOS_IDFA, deviceid, head.time)
            }
        } else
            des.setType(DeviceType.DEVICE_UNKNOWN)
        des.setDeviceId(deviceid)
        des.setName(head.dStrInfo("model"))
        des.setSystem(head.dStrInfo("system"))
        des.setSw(head.dIntInfo("screenWidth"))
        des.setSh(head.dIntInfo("screenHeight"))
        if (acc == 1)
            des.setAcc(acc)
        des.setbCookie(0)
        des.setbFlash(0)

        //添加APPID
        appid2time.foreach(x => des.addAppId(x._1, x._2))

        des
    }
}
