package com.wangjia.bigdata.core.bean.common

/**
  *
  * @param appid      APPID
  * @param uuid       UUID
  * @param userid     用户ID
  * @param logtype    事件主类型 40：自定义事件 50页面事件
  * @param subtype    事件子类型（当logtype是页面事件50时为1）
  * @param time       服务器时间
  * @param staytime   停留时间
  * @param visitindex 会话下标
  * @param ip         IP
  */
case class LogBase(appid: String,
                   uuid: String,
                   userid: String,
                   logtype: String,
                   subtype: String,
                   time: Long,
                   var staytime: Long,
                   var visitindex: Int,
                   ip: String)