package com.wangjia.bigdata.core.bean.info

/**
  * 页面信息
  *
  * @param id      ID
  * @param appId   appId
  * @param childId 子产品ID
  * @param name    名字
  * @param _rType  规则类型
  * @param _rules  规则
  * @param loose   是否不排它
  * @param weight  权重
  * @param active  1使用 0停用
  */
class PageInfo(val id: Int, val appId: String, val childId: Int, val name: String, _rType: Int, _rules: String, val loose: Int, val weight: Int, val active: Int)
        extends StrRule(_rType, _rules) {

    override def toString = s"PageInfo(id=$id, appId=$appId, childId=$childId, name=$name, rType=$rType, rules=$rules, loose=$loose, weight=$weight, active=$active)"
}

object PageInfo {
    def apply(id: Int, appId: String, childId: Int, name: String, _rType: Int, _rules: String, loose: Int, weight: Int, active: Int): PageInfo =
        new PageInfo(id, appId, childId, name, _rType, _rules, loose, weight, active)
}