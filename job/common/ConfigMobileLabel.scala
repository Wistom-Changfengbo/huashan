package com.wangjia.bigdata.core.job.common

/**
  * Created by Administrator on 2017/10/25.
  */
object ConfigMobileLabel {
    /**
      * 页面默认时间
      */
    val PAGE_DEFAULT_TIME = 10000

    /**
      * 页面最大时间
      */
    val PAGE_MAX_TIME = 10000

    /**
      * 页面事件权重
      */
    val EVENT_PAGE_WEIGHT = 1F

    /**
      * 重要自定义事件权重
      */
    val EVENT_IMPORTANCE_SELF_WEIGHT = 5F

    /**
      * 自定义标签 用户活跃度
      */
    val SELF_DEFINITION_LABEL_LIVENESS = "_sf_liveness"
}
