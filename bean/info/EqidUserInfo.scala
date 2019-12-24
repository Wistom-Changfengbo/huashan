package com.wangjia.bigdata.core.bean.info

/**
  * 百度云，reference，eqid查询，用户信息
  *
  * @param id        数据库ID
  * @param username  用户名
  * @param appid     appid
  * @param sourceid  渠道id
  * @param accesskey accesskey
  * @param secretkey secretkey
  * @param target    target
  */
case class EqidUserInfo(id: Int,
                        appid: String,
                        sourceid: String,
                        username: String,
                        accesskey: String,
                        secretkey: String,
                        target: String) {

}
