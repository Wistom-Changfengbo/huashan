package com.wangjia.bigdata.core.handler

import java.util.regex.Pattern

import com.wangjia.bigdata.core.bean.info.StrRule

/**
  * 规则判断处理
  */
object RuleHandler {

    /**
      * 匹配规则：
      * 1、包含
      * 2、不包含
      * 3、以XX开头
      * 4、以XX结尾
      * 5、正则
      * 6、特殊规则
      *
      * @param rule
      * @param str
      * @return
      */
    def isMeet(rule: StrRule, str: String): Boolean = {
        rule.rType match {
            case 1 => isContains(rule.rules, str)
            case 2 => isNotContains(rule.rules, str)
            case 3 => isStartWith(rule.rules, str)
            case 4 => isEndWith(rule.rules, str)
            case 5 => isRegex(rule.rules, str)
            case 6 => throw new Exception("not have rule type 6")
            case 7 => isEquals(rule.rules, str)
            case _ => throw new Exception("don't know rule type")
        }
    }

    def isMeet(rules: Array[StrRule], str: String): Boolean = {
        for (rule <- rules) {
            if (isMeet(rule, str)) {
                return true
            }
        }
        false
    }

    def getMeetIndex(rules: Array[StrRule], str: String): Int = {
        var i = 0
        while (i < rules.length) {
            if (isMeet(rules(i), str)) {
                return i
            }
            i += 1
        }
        -1
    }

    def getMeetRule(rules: Array[StrRule], str: String): StrRule = {
        var i = 0
        while (i < rules.length) {
            if (isMeet(rules(i), str)) {
                return rules(i)
            }
            i += 1
        }
        null
    }

    def isContains(rule: String, str: String): Boolean = str.indexOf(rule) != -1

    def isNotContains(rule: String, str: String): Boolean = str.indexOf(rule) == -1

    def isStartWith(rule: String, str: String): Boolean = str.startsWith(rule)

    def isEndWith(rule: String, str: String): Boolean = str.endsWith(rule)

    def isRegex(rule: String, str: String): Boolean = {
        //str.matches(rule)
        val pattern: Pattern = Pattern.compile(rule)
        val matcher = pattern.matcher(str)
        matcher.find()
    }

    def isEquals(rule: String, str: String): Boolean = rule.equals(str)
}
