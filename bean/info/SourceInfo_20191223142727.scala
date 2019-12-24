package com.wangjia.bigdata.core.bean.info

/**
  * 来源信息
  *
  * @param id      ID
  * @param name    名字
  * @param _rType  规则类型
  * @param _rules  规则
  * @param keyName 关键字段
  * @param weight  权重
  * @param active  1使用 0停用
  */
class SourceInfo(val id: Int, val name: String, _rType: Int, _rules: String, val keyName: String, val weight: Int, val active: Int)
        extends StrRule(_rType, _rules) {

    override def toString = s"SourceInfo($id, $name, $rType, $rules, $weight, $active)"
}

object SourceInfo {
    def apply(id: Int, name: String, _rType: Int, _rules: String, keyname: String, weight: Int, active: Int): SourceInfo =
        new SourceInfo(id, name, _rType, _rules, keyname, weight, active)
}
