package com.wangjia.bigdata.core.bean.info

/**
  * 帐号系统配置信息
  *
  * @param id     ID
  * @param _rType 规则类型
  * @param _rules 规则
  * @param name   名字
  * @param des    描述信息
  */
class AccountInfo(val id: Int, _rType: Int, _rules: String, val name: String, val des: String)
        extends StrRule(_rType, _rules) {

    override def toString = s"AccountInfo(id=$id, rType=$rType, rules=$rules name=$name, des=$des)"
}

object AccountInfo {
    def apply(id: Int, _rType: Int, _rules: String, name: String, des: String): AccountInfo =
        new AccountInfo(id, _rType, _rules, name, des)

}