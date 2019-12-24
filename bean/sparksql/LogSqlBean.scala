package com.wangjia.bigdata.core.bean.sparksql

/**
  * Created by Administrator on 2017/9/8.
  */
case class LogSqlBean(appid: String,
                      userId: String,
                      cookieId: String,
                      uuid: String,
                      logType: String,
                      sub_type: String,
                      time: Long,
                      ip: String,
                      ref: String,
                      sourceid: Int,
                      kw_field: String,
                      kw: String,
                      url: String,
                      acc: Int,
                      system: String,
                      browser: String,
                      sw: Int,
                      sh: Int,
                      bCookie: Int,
                      bFlash: Int)