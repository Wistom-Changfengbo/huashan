package com.wangjia.bigdata.core.bean.info

import com.wangjia.bigdata.core.bean.common.ObjectBean
import com.wangjia.bigdata.core.handler.RuleHandler

/**
  * 规则类
  *
  * @param rType 规则类
  * @param rules 规则
  */
class StrRule(val rType: Int, val rules: String) extends ObjectBean {
    def isMeet(str: String): Boolean = {
        RuleHandler.isMeet(this, str)
    }
}