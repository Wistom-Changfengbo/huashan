package com.wangjia.bigdata.core.job

import com.wangjia.bigdata.core.common.Config
import com.wangjia.utils.HBaseUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Created by Administrator on 2017/9/8.
  */
trait SparkStreamJob extends Serializable {
    //一个批次时间(S)
    protected var BATCH_DURATION_SECONDS: Long = 30L

    protected def initHBase(): Unit = {
        if (Config.DEBUG) {
            return
        }
        val _conf = HBaseConfiguration.create()
        val hbaseMaster = Config.CONFIG_PROP.getProperty("ZOOKEEPER_HOSTS")
        if (hbaseMaster != null && hbaseMaster != "")
            _conf.set("hbase.master", hbaseMaster)
        _conf.set("zookeeper.znode.parent", "/hbase-unsecure")
        _conf.set("hbase.zookeeper.quorum", Config.CONFIG_PROP.getProperty("ZOOKEEPER_HOSTS"))
        _conf.set("hbase.zookeeper.property.clientPort", Config.CONFIG_PROP.getProperty("ZOOKEEPER_CLIENT_PORT"))
        HBaseUtils.setConfiguration(_conf)
    }

    protected def readLineByKafka(ssc: StreamingContext, topics: Array[String], kafkaInfo: Map[String, Object]): DStream[String] = {
        val kafkaParams = mutable.Map[String, Object](
            "bootstrap.servers" -> Config.BOOTSTRAP_SERVERS,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        kafkaParams ++= kafkaInfo

        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )
        val dstream = stream.transform(rdd => {
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
            rdd
        })
        dstream.foreachRDD(x=>println(x))
        dstream.map(record => record.value)
    }

    protected def ssc: StreamingContext = {
        SparkExtend.ssc(this.getClass.getSimpleName, Seconds(BATCH_DURATION_SECONDS))
    }

    protected def init(): Unit = {
    }

    def run(args: Array[String]): Unit = {
        init()
        job(args)
        ssc.start()
        ssc.awaitTermination()
    }

    protected def job(args: Array[String]): Unit = {
    }
}
