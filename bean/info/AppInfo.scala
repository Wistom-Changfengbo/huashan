package com.wangjia.bigdata.core.bean.info

import com.wangjia.bigdata.core.bean.common.ObjectBean

/**
  * 应用配置信息
  *
  * @param id         数据库ID
  * @param appId      应用ID
  * @param appType    应用类型：1、WEB 2、移动APP 3、微信小程序
  * @param child      0没有子产品 1有子产品
  * @param identifier 唯一标识码
  * @param accountId  帐号系统ID
  * @param appName    应用名
  * @param host       应用主域名
  * @param active     0、下线 1、上线
  * @param des        应用描述
  * @param addTime    添加的时间
  * @param companyId  公司ID
  */
case class AppInfo(id: Long,
                   appId: String,
                   appType: Int,
                   child: Int,
                   identifier: String,
                   accountId: Int,
                   appName: String,
                   host: String,
                   active: Int,
                   des: String,
                   addTime: Long,
                   companyId: Int
                  ) extends ObjectBean