package com.wangjia.bigdata.core.job

import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.utils.Utils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark扩展类
  */
object SparkExtend {
    var sparkContext: SparkContext = null
    var streamingContext: StreamingContext = null

    var sparkConf: SparkConf = null
    var appName: String = "SparkJob"
    //var masterUrl: String = "local[4]"
    var masterUrl: String = Config.MASTER_HOST
    //打印日志级别
    var logLevel: String = Config.LOG_LEVEL
    // checkPoint 目录
    var checkPointDir: String = Config.CHECK_POINT_DIR

    //得到CheckPointDir
    def getCheckPointDir: String = {
        val sb = new StringBuilder
        sb.append(checkPointDir)
        if (!checkPointDir.endsWith("/"))
            sb.append("/")
        sb.append(appName)
        sb.append("/")
        sb.append(System.currentTimeMillis())
        sb.append("/")
        sb.toString()
    }

    //初始化
    def init(): Unit = {
        sparkConf = new SparkConf().setAppName(appName)
        //sparkConf.set("mapreduce.framework.name", "yarn")
        //sparkConf.set("yarn.resourcemanager.hostname", "node-1")
        //sparkConf.set("yarn.resourcemanager.admin.address", "node-1:8141")
        //sparkConf.set("yarn.resourcemanager.address", "node-1:8050")
        //sparkConf.set("yarn.resourcemanager.resource-tracker.address", "node-1:8025")
        //sparkConf.set("yarn.resourcemanager.scheduler.address", "node-1:8030")
        if (masterUrl != null && masterUrl.length > 1 && masterUrl != "NO") {
            sparkConf = sparkConf.setMaster(masterUrl)
        }
        sparkConf.set("spark.driver.allowMultipleContexts","true")
        sparkContext = new SparkContext(sparkConf)
        sparkContext.setLogLevel(logLevel)
        sparkContext.setCheckpointDir(getCheckPointDir)
        System.setProperty("es.set.netty.runtime.available.processors", "false")
//        sparkContext.setCheckpointDir("hdfs://ambari.am0.com:9000/temp1/checkPoint/")
//        System.setProperty("es.set.netty.runtime.available.processors", "false")
    }

    def init(appName: String): Unit = {
        this.appName = appName
        init()
    }

    def init(appName: String,
             masterUrl: String = "local[4]",
             logLevel: String = Config.LOG_LEVEL,
             checkPointDir: String = Config.CHECK_POINT_DIR): Unit = {
        this.appName = appName
        this.masterUrl = masterUrl
        this.logLevel = logLevel
        this.checkPointDir = checkPointDir
        init()
    }

    def clean(): Unit = {
        if (sparkContext != null) {
            sparkContext.stop()
            sparkContext = null
        }
        if (streamingContext != null) {
            streamingContext.stop()
            streamingContext = null
        }
    }

    //得到sparkContext
    def ctx: SparkContext = {
        if (sparkContext == null) {
            init()
        }
        sparkContext
    }

    //得到StreamingContext
    def ssc(appName: String, batchDuration: Duration): StreamingContext = {
        if (streamingContext != null)
            return streamingContext
        this.appName = appName
        this.sparkConf = new SparkConf().setAppName(this.appName)
        //sparkConf.set("spark.streaming.receiver.maxRate", "2")
        sparkConf.set("spark.streaming.backpressure.enabled", "true")
        //sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1")
        //sparkConf.set("mapreduce.framework.name", "yarn")
        //sparkConf.set("yarn.resourcemanager.hostname", "node-1")
        //sparkConf.set("yarn.resourcemanager.admin.address", "node-1:8141")
        //sparkConf.set("yarn.resourcemanager.address", "node-1:8050")
        //sparkConf.set("yarn.resourcemanager.resource-tracker.address", "node-1:8025")
        //sparkConf.set("yarn.resourcemanager.scheduler.address", "node-1:8030")
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
        if (masterUrl != null && masterUrl.length > 1 && masterUrl != "NO") {
            sparkConf = sparkConf.setMaster(masterUrl)
        }
        val _ssc = new StreamingContext(this.sparkConf, batchDuration)
        _ssc.checkpoint(getCheckPointDir)
        this.streamingContext = _ssc
        _ssc
    }
}
