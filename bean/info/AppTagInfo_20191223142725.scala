package com.wangjia.bigdata.core.bean.info

/**
  * APP TAG INFO
  *
  * @param id              ID
  * @param platform        平台
  * @param pkg_name        包名
  * @param tag_id          TAG ID
  * @param tag_name        TAG 名
  * @param sex_weight      性别权重
  * @param user_tag_id     用户 TAG ID
  * @param user_tag_name   用户 TAG 名
  * @param user_tag_weight 用户 TAG 权重
  * @param mutex           互斥
  */
case class AppTagInfo(id: Int,
                      platform: String,
                      pkg_name: String,
                      tag_id: Int,
                      tag_name: String,
                      sex_weight: Double,
                      user_tag_id: Int,
                      user_tag_name: String,
                      user_tag_weight: Double,
                      mutex: String)
