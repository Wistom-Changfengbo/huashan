package com.wangjia.bigdata.core.bean.info

/**
  * 子产品信息
  *
  * @param id     ID
  * @param appId  appId
  * @param name   名字
  * @param _rType 规则类型
  * @param _rules 规则
  * @param weight 权重
  * @param active 1使用 0停用
  */
class AppChildInfo(val id: Int, val appId: String, val name: String, _rType: Int, _rules: String, val weight: Int, val active: Int)
        extends StrRule(_rType, _rules) {

    override def toString = s"AppChildInfo(id=$id, appId=$appId, name=$name, rType=$rType, rules=$rules, weight=$weight, active=$active)"
}

object AppChildInfo {
    def apply(id: Int, appId: String, name: String, _rType: Int, _rules: String, weight: Int, active: Int): AppChildInfo =
        new AppChildInfo(id, appId, name, _rType, _rules, weight, active)
}