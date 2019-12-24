package com.wangjia.bigdata.core.job.stream

import com.alibaba.fastjson.JSONObject
import com.wangjia.bigdata.core.bean.im.ImLogMsg
import com.wangjia.bigdata.core.handler.{LogClearHandler, LogClearStatus}
import com.wangjia.bigdata.core.job.SparkStreamJob
import com.wangjia.es.{EsTableName, ExEsConnection}
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.hbase.{ExHbase, HBaseConst, HBaseTableName}
import com.wangjia.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.dstream.DStream


/**
  * 探针实时统计在店用户数
  *
  * Created by Administrator on 2018/3/5.
  */
class IMStreamTrack extends SparkStreamJob {
    //一个批次时间(S)
    BATCH_DURATION_SECONDS = 20L

    override protected def job(args: Array[String]): Unit = {
        initHBase()
        HBaseUtils.createTable(HBaseTableName.IM_CHAT_MSG)

        val kafkaParams = Map[String, Object](
            "fetch.message.max.bytes" -> "10485760",
            "group.id" -> "wj_im_20180403"
        )
        val topics = Array("im20180403")

        val lineDs = readLineByKafka(ssc, topics, kafkaParams)

        val imBeanDS: DStream[ImLogMsg] = lineDs.map(x => LogClearHandler.clearImLog(x))
                .filter(_._1 == LogClearStatus.SUCCESS)
                .map(_._2.asInstanceOf[ImLogMsg])

        imBeanDS.foreachRDD(_.foreachPartition(iterator => {
            initHBase()
            val conn = new ExTBConnection
            val tb = conn.getTable(HBaseTableName.IM_CHAT_MSG)
            val esConn = new ExEsConnection
            val json = new JSONObject()
            iterator.foreach(b => {
                val rowkey = ExHbase.getIMKey(b.client, b.client_id, b.time)
                val put = new Put(Bytes.toBytes(rowkey))
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_IM_CUSTOMER, Bytes.toBytes(b.customer))
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_IM_TYPE, Bytes.toBytes(b.msgtype))
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_IM_FROM_TYPE, Bytes.toBytes(b.from_type))
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_IM_TIME, Bytes.toBytes(b.time))
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_IM_CONTENT, Bytes.toBytes(b.content))
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_IM_CLIENT_IP, Bytes.toBytes(b.client_ip))
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_IM_SOURCE, Bytes.toBytes(b.source))
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_IM_AVATAR, Bytes.toBytes(b.avatar))
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_IM_CLIENT_ID, Bytes.toBytes(b.client_id))
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_IM_CLIENT, Bytes.toBytes(b.client))
                tb.addPut(put)

                json.clear()
                json.put("customer", b.customer)
                json.put("type", b.msgtype)
                json.put("from_type", b.from_type)
                json.put("time", b.time)
                json.put("content", b.content)
                json.put("client_ip", b.client_ip)
                json.put("source", b.source)
                json.put("avatar", b.avatar)
                json.put("client_id", b.client_id)
                json.put("client", b.client)
                esConn.add(EsTableName.ES_INDEX_BIGDATA_IM, EsTableName.ES_TYPE_IM, rowkey, json.toJSONString)
            })
            tb.close()
            conn.close()
            esConn.close()
        }))
    }
}

object IMStreamTrack {
    def main(args: Array[String]) {
        val job = new IMStreamTrack
        job.run(args)
    }
}