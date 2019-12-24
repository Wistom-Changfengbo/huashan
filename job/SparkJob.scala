package com.wangjia.bigdata.core.job


import com.wangjia.bigdata.core.common.Config
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.utils.HBaseUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/3/2.
  */
trait SparkJob extends Serializable {

  protected def initHBase(): Configuration = {
    if (Config.DEBUG) {
      return null
    }
    val _conf = HBaseConfiguration.create()
    val hbaseMaster = Config.CONFIG_PROP.getProperty("ZOOKEEPER_HOSTS")
    if (hbaseMaster != null && hbaseMaster != "")
      _conf.set("hbase.master", hbaseMaster)
    _conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    println(Config.CONFIG_PROP.getProperty("ZOOKEEPER_HOSTS"))
    _conf.set("hbase.zookeeper.quorum", Config.CONFIG_PROP.getProperty("ZOOKEEPER_HOSTS"))
    _conf.set("hbase.zookeeper.property.clientPort", Config.CONFIG_PROP.getProperty("ZOOKEEPER_CLIENT_PORT"))
    HBaseUtils.setConfiguration(_conf)
    _conf
  }

  protected def save2HBase[T](tbName: String, iter: Iterator[T], fun: (T) => Put): Int = {
    initHBase()
    var i: Int = 0
    var conn: ExTBConnection = null
    try {
      conn = new ExTBConnection
      val tb = conn.getTable(tbName)
      val puts = new ListBuffer[Put]
      iter.foreach(it => {
        val put = fun(it)
        if (put != null) {
          tb.addPut(put)
          i += 1
        }
      })
    } finally {
      if (conn != null)
        conn.close()
    }
    i
  }

  /**
    * 任务名(主类名)
    */
  var jobName: String = {
    this.getClass.getSimpleName
  }

  /**
    * 任务开始时间
    */
  val jobStartTime: Long = System.currentTimeMillis()

  /**
    * 初始化SparkContext
    */
  protected def init(): Unit = {
    SparkExtend.init(jobName)
  }

  /**
    * 任务入口方法
    *
    * @param args
    */
  protected def job(args: Array[String]): Unit = {}

  /**
    * 任务结束调用
    */
  protected def end(): Unit = {
    SparkExtend.clean()
  }

  /**
    * 任务类调度方法
    *
    * @param args
    */
  def run(args: Array[String]): Unit = {
    this.init()
    this.job(args: Array[String])
    this.end()
  }
}