package com.wangjia.bigdata.core.bean.weixin

import java.util


/**
  * 微信端日志
  *
  * @param ja_version  JA版本号
  * @param app_version APP版本号
  * @param appid       APPID
  * @param uuid        UUID
  * @param userid      用户ID
  * @param logtype     事件主类型 40：自定义事件 50页面事件
  * @param pageid      页面ID
  * @param subtype     事件子类型（当logtype是页面事件50时为1）
  * @param time        服务器时间
  * @param localTime   本地时间
  * @param title       标题
  * @param data        数据
  * @param staytime    停留时间
  * @param visitindex  会话下标
  * @param recentPage  最近一个页面   {'pageid':'xxxx','data':'xxxxx','title':'xxxx','time':1111}
  * @param ip          IP
  * @param deviceInfo  设备信息
  *                    screenWidth  屏幕宽 360
  *                    screenHeight 屏幕高 768
  *                    language     语言  "zh_CN"
  *                    platform     平台  "android"
  *                    networkType  网络类型    "wifi"
  *                    brand        品牌  "vivo"
  *                    model        模型  "vivo X9"
  *                    system       系统  "Android 7.1.1"
  *
  *                    ja_uuid      JAUUID  "11c2f7f924acd789e4c749c0ae95a063"
  *                    unionid      UNIONID "olMhQtzbisfqGSCqY41Z2smM6GPQ"
  */
case class WeixinLogMsg(ja_version: String,
                        app_version: String,
                        appid: String,
                        var uuid: String,
                        userid: String,
                        logtype: String,
                        pageid: String,
                        subtype: String,
                        time: Long,
                        localTime: Long,
                        title: String,
                        data: String,
                        var staytime: Long,
                        var visitindex: Int,
                        var recentPage: String,
                        ip: String,
                        deviceInfo: util.Map[String, Object]) {

    def dStrInfo(key: String): String = {
        if (deviceInfo.containsKey(key)) deviceInfo.get(key).asInstanceOf[String] else ""
    }

    def dStrInfo(key: String, default: String): String = {
        if (deviceInfo.containsKey(key)) deviceInfo.get(key).asInstanceOf[String] else default
    }

    def dIntInfo(key: String): Int = {
        if (deviceInfo.containsKey(key)) deviceInfo.get(key).asInstanceOf[Int] else -1
    }

    def dIntInfo(key: String, default: Int): Int = {
        if (deviceInfo.containsKey(key)) deviceInfo.get(key).asInstanceOf[Int] else default
    }
}