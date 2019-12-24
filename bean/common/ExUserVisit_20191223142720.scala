package com.wangjia.bigdata.core.bean.common

import com.wangjia.bigdata.core.bean.info.{PageInfo, SourceInfo, StrRule}
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.bean.web.WebLogMsg
import com.wangjia.bigdata.core.handler.{BeanHandler, RuleHandler}
import com.wangjia.bigdata.core.utils.IPUtils
import com.wangjia.common.{LogEventId, LogType, Platform}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/7/19.
  */
class ExUserVisit extends UserVisit {

    //最后访问的时间
    var lastTime: Long = 0

    //最后更新的时间
    var updateTime: Long = 0

    //页面请求集合
    var pReqsList: ListBuffer[PageReq] = null

    //事件
    var pEventsList: ListBuffer[PageEvent] = null

    //是否被改变
    var bChange: Boolean = true

    def addTrack(bean: WebLogMsg, timeMillis: Long): Unit = {
        this.updateTime = timeMillis
        this.bChange = true
        if (bean.logtype == LogType.PAGE)
            addTrackPage(bean)
        else if (bean.logtype == LogType.EVENT)
            addTrackEvent(bean)
    }

    def addTrackPage(bean: WebLogMsg): Unit = {
        this.lastTime = bean.time
        val index = this.pReqsList.size
        val req = PageReq(bean.url, bean.title, bean.ref, bean.time, 0, bean.data)
        if (index == 0) {
            this.pReqsList += req
            return
        }

        val lastReq = this.pReqsList.remove(index - 1)
        val dt = bean.time - lastReq.start
        this.time += dt
        val upLastReq = PageReq(lastReq.pageUrl, lastReq.title, lastReq.refUrl, lastReq.start, dt, lastReq.data)

        this.pReqsList += upLastReq
        this.pReqsList += req
    }

    def addTrackEvent(bean: WebLogMsg): Unit = {
        this.pEventsList += PageEvent(bean.logtype, bean.subtype, bean.data, bean.time)
    }

    //App的轨迹
    def addTrack(bean: MobileLogMsg, timeMillis: Long, pageRuleInfos: mutable.Map[String, Array[PageInfo]]): Unit = {
        this.updateTime = timeMillis
        this.bChange = true
        if (bean.logtype == LogType.PAGE)
            addTrackPage(bean, pageRuleInfos)
        else if (bean.logtype == LogType.EVENT)
            addTrackEvent(bean)
    }

    //App的页面
    def addTrackPage(bean: MobileLogMsg, pageRuleInfos: mutable.Map[String, Array[PageInfo]]): Unit = {
        if (bean.ja_version.startsWith("2."))
            return addTrackPageV2(bean, pageRuleInfos)
        addTrackPageVDef(bean, pageRuleInfos)
    }

    def addTrackPageV2(bean: MobileLogMsg, pageRuleInfos: mutable.Map[String, Array[PageInfo]]): Unit = {
        this.lastTime = bean.time
        val info = BeanHandler.getPageInfo(pageRuleInfos, bean.appid, bean.pageid)
        val name = if (info != null) info.name else ""
        val index = this.pReqsList.size
        if (index == 0) {
            var beginTime: Long = 0
            this.pEventsList.foreach(e => if (e.eventId.equals(LogEventId.APP_ENTER_FOREGROUND)) beginTime = e.time)
            if (beginTime == 0)
                this.pReqsList += PageReq(bean.pageid, name, "", bean.time, 0, bean.data)
            else
                this.pReqsList += PageReq(bean.pageid, name, "", beginTime, bean.time - beginTime, bean.data)
            return
        }

        val last: PageReq = this.pReqsList.last
        val upPageCloseTime = last.start + last.time
        val req = PageReq(bean.pageid, name, "", upPageCloseTime, bean.time - upPageCloseTime, bean.data)
        this.pReqsList += req
    }


    def addTrackPageVDef(bean: MobileLogMsg, pageRuleInfos: mutable.Map[String, Array[PageInfo]]): Unit = {
        this.lastTime = bean.time
        val info = BeanHandler.getPageInfo(pageRuleInfos, bean.appid, bean.pageid)
        val name = if (info != null) info.name else ""
        val req = PageReq(bean.pageid, name, "", bean.time, 0, bean.data)
        val index = this.pReqsList.size
        if (index == 0) {
            this.pReqsList += req
            return
        }

        val lastReq = this.pReqsList.remove(index - 1)
        val dt = bean.time - lastReq.start
        this.time += dt
        val upLastReq = PageReq(lastReq.pageUrl, lastReq.title, lastReq.refUrl, lastReq.start, dt, lastReq.data)

        this.pReqsList += upLastReq
        this.pReqsList += req
    }

    //App的轨迹事件
    def addTrackEvent(bean: MobileLogMsg): Unit = {
        this.pEventsList += PageEvent(bean.logtype, bean.subtype, bean.data, bean.time)
    }

    def toVisit(sourceInfos: Array[StrRule]): UserVisit = {
        this.add = IPUtils.getAddress(this.ip)
        val info = RuleHandler.getMeetRule(sourceInfos, this.ref)
        if (info != null) {
            this.ref = info.asInstanceOf[SourceInfo].name
        }
        this.pReqs = this.pReqsList.toArray
        this.pEvents = this.pEventsList.toArray
        var dtTime: Long = 0
        this.pReqsList.foreach(dtTime += _.time)
        this.time = dtTime
        this
    }

}

object ExUserVisit {

    def apply(bean: WebLogMsg, timeMillis: Long): ExUserVisit = {
        val visit = new ExUserVisit

        visit.appid = bean.appid
        visit.uuid = bean.uuid
        visit.deviceId = bean.dStrInfo("ja_uuid")
        visit.platform = Platform.PLATFORM_WEB
        visit.ref = bean.ref
        visit.ip = bean.ip
        visit.start = bean.time
        visit.time = 0
        visit.version = "-"
        visit.net = "-"

        visit.lastTime = bean.time
        visit.updateTime = timeMillis

        visit.pReqsList = new ListBuffer[PageReq]
        visit.pEventsList = new ListBuffer[PageEvent]

        visit
    }

    def apply(bean: MobileLogMsg, timeMillis: Long): ExUserVisit = {
        val visit = new ExUserVisit

        visit.appid = bean.appid
        visit.uuid = bean.uuid
        visit.deviceId = bean.dStrInfo("deviceid")
        visit.platform = Platform.PLATFORM_MOBILE
        visit.ref = bean.dStrInfo("channel")
        visit.ip = bean.ip
        visit.start = bean.time
        visit.time = 0
        visit.version = bean.app_version
        visit.net = bean.dStrInfo("networkType", "-")

        visit.lastTime = bean.time
        visit.updateTime = timeMillis

        visit.pReqsList = new ListBuffer[PageReq]
        visit.pEventsList = new ListBuffer[PageEvent]
        visit
    }

}
