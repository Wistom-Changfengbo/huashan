package com.wangjia.bigdata.core.handler

import com.wangjia.bigdata.core.bean.common.{PageEvent, PageReq, UserVisit}
import com.wangjia.bigdata.core.bean.info.{AppInfo, PageInfo, SourceInfo, StrRule}
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.bean.web.WebLogMsg
import com.wangjia.bigdata.core.bean.weixin.WeixinLogMsg
import com.wangjia.bigdata.core.utils.IPUtils
import com.wangjia.common.{DeviceType, LogType, Platform}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/2/23.
  */
object VisitTrackHandler {

    /**
      * 计算WEB访问轨迹
      *
      * @param beans
      * @param sourceInfos
      * @return
      */
    def countUserVisit(beans: List[WebLogMsg], sourceInfos: Array[SourceInfo]): UserVisit = {
        try {
            val pages = new ListBuffer[WebLogMsg]
            val events = new ListBuffer[WebLogMsg]
            beans.foreach(x => {
                if (x.logtype == LogType.PAGE)
                    pages += x
                else if (x.logtype == LogType.EVENT)
                    events += x
            })

            val pList = new ListBuffer[PageReq]
            var i = 0
            var max = pages.size
            var allDt: Long = 0
            while (i < max) {
                val bean = pages(i)
                val time = bean.time
                val dt = bean.staytime
                val req = PageReq(bean.url, bean.title, bean.ref, time, dt, bean.data)
                pList += req
                allDt += dt
                i += 1
            }

            val eList = new ListBuffer[PageEvent]
            i = 0
            max = events.size
            while (i < max) {
                val bean = events(i)
                val time = bean.time
                val req = PageEvent(bean.logtype, bean.subtype, bean.data, time)
                eList += req
                i += 1
            }

            val startBean = beans.head
            val visit = new UserVisit
            visit.deviceId = startBean.dStrInfo("ja_uuid")
            visit.platform = Platform.PLATFORM_WEB
            visit.uuid = startBean.uuid
            visit.appid = startBean.appid
            visit.ref = startBean.ref
            visit.ip = startBean.ip
            visit.start = startBean.time
            visit.time = allDt
            visit.version = "-"
            visit.net = "-"
            visit.pReqs = pList.toArray
            visit.pEvents = eList.toArray
            visit.add = IPUtils.getAddress(startBean.ip)

            val info = RuleHandler.getMeetRule(sourceInfos.asInstanceOf[Array[StrRule]], startBean.ref)
            if (info != null) {
                visit.ref = info.asInstanceOf[SourceInfo].name
            }

            visit
        } catch {
            case e: Exception => e.printStackTrace(); null
        }
    }

    /**
      * 计算APP访问轨迹
      *
      * @param beans
      * @param pageRuleInfos
      * @return
      */
    def countUserVisit(beans: List[MobileLogMsg], pageRuleInfos: mutable.Map[String, Array[PageInfo]]): UserVisit = {
        try {
            val pages = new ListBuffer[MobileLogMsg]
            val events = new ListBuffer[MobileLogMsg]
            beans.foreach(x => {
                if (x.logtype == LogType.PAGE)
                    pages += x
                else if (x.logtype == LogType.EVENT)
                    events += x
            })

            val pList = new ListBuffer[PageReq]
            var i = 0
            var max = pages.size
            var allDt: Long = 0
            while (i < max) {
                val bean = pages(i)
                val info = getPageInfo(pageRuleInfos, bean.appid, bean.pageid)
                val name = if (info != null) info.name else ""
                val time = bean.time
                val dt = bean.staytime
                val req = PageReq(bean.pageid, name, "", time, dt, bean.data)
                pList += req
                allDt += dt
                i += 1
            }

            val eList = new ListBuffer[PageEvent]
            i = 0
            max = events.size
            while (i < max) {
                val bean = events(i)
                val time = bean.time
                val req = PageEvent(bean.logtype, bean.subtype, bean.data, time)
                eList += req
                i += 1
            }

            val startBean = beans.head
            val visit = new UserVisit
            visit.deviceId = startBean.dStrInfo("deviceid")
            visit.platform = Platform.PLATFORM_MOBILE
            visit.uuid = startBean.uuid
            visit.appid = startBean.appid
            visit.ref = startBean.dStrInfo("channel")
            visit.ip = startBean.ip
            visit.start = startBean.time
            visit.time = allDt
            visit.version = startBean.app_version
            visit.net = startBean.dStrInfo("networkType")
            visit.pReqs = pList.toArray
            visit.pEvents = eList.toArray
            visit.add = IPUtils.getAddress(startBean.ip)

            visit
        } catch {
            case e: Exception => e.printStackTrace(); null
        }
    }


    /**
      * 计算小程序访问轨迹
      *
      * @param beans
      * @param pageRuleInfos
      * @return
      */
    def countUserVisit(beans: List[WeixinLogMsg], pageRuleInfos: mutable.Map[String, Array[PageInfo]], appInfo: AppInfo): UserVisit = {
        if (appInfo == null || beans == null || beans.isEmpty)
            return null
        try {
            val pages = new ListBuffer[WeixinLogMsg]
            val events = new ListBuffer[WeixinLogMsg]
            beans.foreach(x => {
                if (x.logtype == LogType.PAGE)
                    pages += x
                else if (x.logtype == LogType.EVENT)
                    events += x
            })

            val pList = new ListBuffer[PageReq]
            var i = 0
            var max = pages.size
            var allDt: Long = 0
            while (i < max) {
                val bean = pages(i)
                val info = getPageInfo(pageRuleInfos, bean.appid, bean.pageid)
                val name = if (info != null) info.name else ""
                val time = bean.time
                val dt = bean.staytime
                val req = PageReq(bean.pageid, name, "", time, dt, bean.data)
                pList += req
                allDt += dt
                i += 1
            }

            val eList = new ListBuffer[PageEvent]
            i = 0
            max = events.size
            while (i < max) {
                val bean = events(i)
                val time = bean.time
                val req = PageEvent(bean.logtype, bean.subtype, bean.data, time)
                eList += req
                i += 1
            }

            val startBean = beans.head
            val visit = new UserVisit
            visit.deviceId = startBean.dStrInfo("ja_uuid")
            visit.platform = Platform.PLATFORM_WEIXIN
            visit.uuid = startBean.uuid
            visit.appid = startBean.appid
            if (appInfo.appType == DeviceType.DEVICE_WEIXIN)
                visit.ref = "weixin"
            else if (appInfo.appType == DeviceType.DEVICE_ZFB)
                visit.ref = "zfb"
            else
                visit.ref = "unknown"
            visit.ip = startBean.ip
            visit.start = startBean.time
            visit.time = allDt
            visit.version = startBean.app_version
            visit.net = startBean.dStrInfo("networkType")
            visit.pReqs = pList.toArray
            visit.pEvents = eList.toArray
            visit.add = IPUtils.getAddress(startBean.ip)

            visit
        } catch {
            case e: Exception => e.printStackTrace(); null
        }
    }

    /**
      * 得到页面信息
      *
      * @param pageRuleInfos
      * @param appId
      * @param url
      * @return
      */
    def getPageInfo(pageRuleInfos: mutable.Map[String, Array[PageInfo]], appId: String, url: String): PageInfo = {
        val arryPageInfos = pageRuleInfos.get(appId) match {
            case Some(x) => x
            case None => return null
        }
        arryPageInfos.foreach(x => {
            if (x.loose == 0 && x.isMeet(url))
                return x
        })
        null
    }
}