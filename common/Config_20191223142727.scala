package com.wangjia.bigdata.core.common

import java.util.Properties

import com.wangjia.utils.HBaseUtils
import org.apache.hadoop.hbase.HBaseConfiguration

/**
  * Created by Administrator on 2017/2/28.
  */
object Config {
    //    System.setProperty("HADOOP_USER_NAME", "root")
    val DEBUG = {
        val os = System.getProperty("os.name")
        if (os != null && os.startsWith("Windows"))
            true
        else
            false
    }

    println(DEBUG)

    var CONFIG_PATH: String = ""
    var JDBC_PATH: String = ""
    if (DEBUG) {
        System.setProperty("HADOOP_USER_NAME", "root")
        CONFIG_PATH = "/debug/config.properties"
        JDBC_PATH = "/debug/jdbc.properties"
    } else {
        System.setProperty("HADOOP_USER_NAME", "hdfs")
        CONFIG_PATH = "/youngcloud2/config.properties"
        JDBC_PATH = "/youngcloud2/jdbc.properties"
        //CONFIG_PATH = "/test/config.properties"
        //JDBC_PATH = "/test/jdbc.properties"
    }
    private val prop: Properties = {
        val _prop: Properties = new Properties()
        _prop.load(this.getClass.getResourceAsStream(CONFIG_PATH))
        _prop
    }

    val CONFIG_PROP = prop

    //是否打印错误LOG
    val IS_PRINT_ERROR_LOG = prop.getProperty("IS_PRINT_ERROR_LOG").toBoolean

    //程序输出日志等级
    //"WARN" "DEBUG" "ERROR" "INFO"
    val LOG_LEVEL = prop.getProperty("LOG_LEVEL")

    //master host
    val MASTER_HOST = prop.getProperty("MASTER_HOST")

    //checkpoint数据存储目录
    val CHECK_POINT_DIR = prop.getProperty("CHECK_POINT_DIR")

    //ip地址文件
    val IP_LOCATION_FILE_PATH = prop.getProperty("IP_LOCATION_FILE_PATH")

    //字段分隔符
    val FIELD_SEPARATOR = prop.getProperty("FIELD_SEPARATOR")

    //一次访问间隔时间
    val INTERVAL_TIME = prop.getProperty("INTERVAL_TIME").toInt

    //一个批次的数量
    val BATCH_SIZE = 1000

    //更新配置表时间间隔
    val UPDATE_INFO_INTERVAL_TIME = 3600 * 1000

    //es的node节点
    val ES_NODES = prop.getProperty("ES_NODES")
    //端口号
    val ES_PORT = prop.getProperty("ES_PORT")
    //用户标签接口地址
    val TAG_HOST = prop.getProperty("TAG_HOST")

    //kafkaServers
    val BOOTSTRAP_SERVERS = prop.getProperty("BOOTSTRAP_SERVERS")

    println(prop.toString)

    //RedisUrl
    val REDIS_URL = prop.getProperty("REDIS_URL")
    val REDIS_PORT = prop.getProperty("REDIS_PORT").toInt
    val REDIS_DBINDEX = prop.getProperty("REDIS_DBINDEX").toInt

    //初始化Hase
    private val _conf = HBaseConfiguration.create()
    val hbaseMaster = Config.CONFIG_PROP.getProperty("ZOOKEEPER_HOSTS")
    if (hbaseMaster != null && hbaseMaster != "")
        _conf.set("hbase.master", hbaseMaster)
    _conf.set("zookeeper.znode.parent", "/hbase-unsecure")
//    if (!DEBUG)
//        _conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    _conf.set("hbase.zookeeper.quorum", Config.CONFIG_PROP.getProperty("ZOOKEEPER_HOSTS"))
    _conf.set("hbase.zookeeper.property.clientPort", Config.CONFIG_PROP.getProperty("ZOOKEEPER_CLIENT_PORT"))
    HBaseUtils.setConfiguration(_conf)
}