package com.wangjia.bigdata.core.bean.info

/**
  * 用户标签
  *
  * @param id        id
  * @param pid       父标签ID
  * @param _rType    匹配规则类型
  * @param _rules    匹配规则
  * @param name      标签名
  * @param des       标签描述
  * @param companyId 公司ID
  */
class UserLabel(val id: Int,
                val pid: Int,
                _rType: Int,
                _rules: String,
                val name: String,
                val des: String,
                val companyId: Int) extends StrRule(_rType, _rules) {

    override def toString = s"UserLabel(id=$id, pid=$pid, type=$rType, rules=$rules,  name=$name, des=$des, companyId=$companyId)"
}

object UserLabel {
    def apply(id: Int, pid: Int, _rType: Int, _rules: String, name: String, des: String, companyId: Int): UserLabel =
        new UserLabel(id, pid, _rType, _rules, name, des, companyId)
}