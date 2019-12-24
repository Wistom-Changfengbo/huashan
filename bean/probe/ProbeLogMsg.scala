package com.wangjia.bigdata.core.bean.probe

/**
  * Created by Cfb on 2017/8/1.
  *
  * @param venueId   场馆ID
  * @param id        探针的唯一id
  * @param uuid      探针的原始唯一id
  * @param mac       mac地址
  * @param signal    信号强度 可以用来当距离
  * @param frameType 帧类型
  * @param time      time
  */
case class ProbeLogMsg(venueId: Int,
                       id: String,
                       uuid: String,
                       mac: String,
                       signal: Int,
                       frameType: String,
                       time: Long) {
    override def toString = s"ProbeInfo(venueId=$venueId, id=$id, uuid=$uuid, mac=$mac, signal=$signal, frameType=$frameType, time=$time)"
}


