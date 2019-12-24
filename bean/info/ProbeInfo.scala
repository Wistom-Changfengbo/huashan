package com.wangjia.bigdata.core.bean.info

/**
  * @param uuid    探针的唯一id
  * @param probeId 探针内部ID
  * @param signal  信号强度 可以用来当距离
  * @param ftype   帧类型
  * @param name    探针名称
  * @param active  是否可用
  * @param jd      精度
  * @param wd      纬度
  * @param venueId 场馆ID
  */
case class ProbeInfo(uuid: String, probeId: String, signal: Int, ftype: String, name: String, active: Int, jd: Double, wd: Double, venueId: Int)