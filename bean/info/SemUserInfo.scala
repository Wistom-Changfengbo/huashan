package com.wangjia.bigdata.core.bean.info

/**
  * Sem商业开发者中心数据
  *
  * @param id       数据库ID
  * @param username 用户名
  * @param appid appid
  * @param sourceid 渠道id
  * @param password 密码
  * @param token    token
  * @param target   target
  */
case class SemUserInfo(id: Int,
                       appid:String,
                       sourceid:String,
                       username: String,
                       password: String,
                       token: String,
                       target: String) {

}
