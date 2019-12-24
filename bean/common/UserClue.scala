package com.wangjia.bigdata.core.bean.common

/**
  * 用户线索
  *
  * @param clueId     线索ID
  * @param uuid       设备唯一ID
  * @param ip         ip
  * @param appId      应用ID
  * @param userId     用户ID
  * @param cookieId   站点Cookie
  * @param deviceId   移动端设备ID
  * @param deviceName 设备型号
  * @param version    版本号
  * @param time       时间戳
  * @param name       用户称呼
  * @param phone      手机号
  * @param cityId     城市ID
  * @param houseArea  住房面积
  * @param style      装修风格
  * @param flatsName  楼盘名
  */
case class UserClue(clueId: String,
                    uuid: String,
                    ip: String,
                    appId: String,
                    userId: String,
                    cookieId: String,
                    deviceId: String,
                    deviceName: String,
                    version: String,
                    time: Long,
                    name: String,
                    phone: String,
                    cityId: String,
                    houseArea: String,
                    style: String,
                    flatsName: String)