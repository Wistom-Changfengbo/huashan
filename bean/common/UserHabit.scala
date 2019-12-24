package com.wangjia.bigdata.core.bean.common

/**
  *
  * @param appid      APPID
  * @param platform   平台，Android or IOS 在移动端统计时使用
  * @param sum_time   总的访问时间
  * @param sum_user   总的用户数
  * @param sum_access 总的访问次数
  * @param sum_page   总的页面数
  */
case class UserHabit(appid: String, platform: String, sum_time: Long, sum_user: Long, sum_access: Long, sum_page: Long)