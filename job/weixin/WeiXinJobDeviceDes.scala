package com.wangjia.bigdata.core.job.weixin

import java.util.regex.Pattern

import com.wangjia.bean.DeviceDes
import com.wangjia.bigdata.core.bean.info.AppInfo
import com.wangjia.bigdata.core.bean.weixin.WeixinLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.JAInfoUtils
import com.wangjia.builder.DeviceDesBuidler
import com.wangjia.common.{DeviceType, LogEventId, LogType}
import com.wangjia.es.{EsTableName, ExEsConnection}
import com.wangjia.hbase.HBaseTableName
import com.wangjia.hbase.conn.{ExTBConnection, ExTable}
import com.wangjia.utils.HBaseUtils
import org.apache.hadoop.hbase.client.{Get, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/4/27.
  */
class WeiXinJobDeviceDes extends EveryDaySparkJob {

    //提取手机号
    private val pattern = Pattern.compile("((13[0-9])|(14[5|7])|(15([0-3]|[5-9]))|(18[0,5-9])|(17[0-9]))\\d{8}")

    //应用信息
    private var mapAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null

    override protected def init(): Unit = {
        super.init()
        val sc = SparkExtend.ctx

        //加载广播应用信息
        val mapAppInfos = JAInfoUtils.loadAppInfos
        mapAppInfosBroadcast = sc.broadcast(mapAppInfos)
    }

    private def updateDeviceDes(uuid: String, _des: DeviceDes, logMsgs: List[WeixinLogMsg]): DeviceDes = {
        val des = if (_des == null) new DeviceDes() else _des
        //设备APP信息
        val appid2time: mutable.Map[String, Long] = mutable.Map[String, Long]()
        //设置基础信息
        val head = logMsgs.head
        var minTime: Long = head.time
        var maxTime: Long = head.time
        val acc: Int = 1
        var deviceType = DeviceType.DEVICE_WEIXIN
        logMsgs.foreach(b => {
            if (b.time < minTime)
                minTime = b.time
            if (b.time > maxTime)
                maxTime = b.time
            //APPID 最小时间
            if (appid2time.getOrDefault(b.appid, java.lang.Long.MAX_VALUE) > b.time)
                appid2time.put(b.appid, b.time)
            //线索
            if (b.logtype == LogType.EVENT && b.subtype == LogEventId.USER_COMMIT_CLUE) {
                val matcher = pattern.matcher(b.data)
                while (matcher.find()) {
                    des.addPhone(matcher.group(), b.time)
                }
            }
            //UserID
            val appInfo = mapAppInfosBroadcast.value.getOrElse(b.appid, null)
            if (appInfo != null) {
                if (appInfo.appType == DeviceType.DEVICE_WEIXIN)
                    deviceType = DeviceType.DEVICE_WEIXIN
                else if (appInfo.appType == DeviceType.DEVICE_ZFB)
                    deviceType = DeviceType.DEVICE_ZFB
                val userid = b.userid
                if (userid.length > 2) {
                    des.addUserId(appInfo.accountId + "#" + userid, b.time)
                }
            }
        })

        des.setFirstTime(minTime)
        des.setLastTime(maxTime)
        val deviceid: String = head.dStrInfo("ja_uuid")
        des.setDeviceId(deviceid)
        des.setUuid(head.uuid)
        des.setType(deviceType)
        val platform: String = head.dStrInfo("platform")
        des.addId("platform", platform, head.time)

        val model = head.dStrInfo("model", "")
        if (model.length > 0)
            des.setName(model)
        val system = head.dStrInfo("system", "")
        if (system.length > 0)
            des.setSystem(system)
        if (head.deviceInfo.containsKey("screenWidth"))
            des.setSw(head.dIntInfo("screenWidth"))
        if (head.deviceInfo.containsKey("screenHeight"))
            des.setSh(head.dIntInfo("screenHeight"))
        if (acc == 1)
            des.setAcc(acc)
        des.setbCookie(0)
        des.setbFlash(0)

        //微信唯一ID
        val unionid = head.dStrInfo("unionid", "")
        if (unionid.length > 0)
            des.addId("unionid", unionid, head.time)

        //添加APPID
        appid2time.foreach(x => des.addAppId(x._1, x._2))

        des
    }

    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.UUID_DEVICE_MSG)

        val sc = SparkExtend.ctx

        val linesRdd = sc.textFile(this.inPath)

        val beansRdd = linesRdd.map(BeanHandler.toWeiXin)
                .filter(x => x != null && x.uuid.length == 32)

        beansRdd.cache()
        beansRdd.checkpoint()

        val uuid2beansRdd: RDD[(String, Iterable[WeixinLogMsg])] = beansRdd.groupBy(_.uuid)
        val uuid2DeviceDesRdd: RDD[(String, DeviceDes)] = uuid2beansRdd.map(_._1).mapPartitions(iterator => {
            initHBase()
            val map = mutable.Map[String, DeviceDes]()
            val uuids = new ListBuffer[String]
            val gets = new ListBuffer[Get]

            val conn = new ExTBConnection
            val tb: Table = conn.getTable(HBaseTableName.UUID_DEVICE_MSG).getTable

            iterator.foreach(uuid => {
                uuids += uuid
                gets += new Get(Bytes.toBytes(uuid))
                if (uuids.size > Config.BATCH_SIZE) {
                    val results: Array[Result] = tb.get(gets)
                    var i = 0
                    val max = results.length
                    while (i < max) {
                        map += (uuids(i) -> DeviceDesBuidler.build(uuids(i), results(i)))
                        i += 1
                    }
                    uuids.clear()
                    gets.clear()
                }
            })

            if (uuids.nonEmpty) {
                val results: Array[Result] = tb.get(gets)
                var i = 0
                val max = results.length
                while (i < max) {
                    map += (uuids(i) -> DeviceDesBuidler.build(uuids(i), results(i)))
                    i += 1
                }
                uuids.clear()
                gets.clear()
            }

            conn.close()
            map.toIterator
        })

        val uuid2newDesRdd = uuid2DeviceDesRdd.join(uuid2beansRdd).map(bean => {
            val uuid = bean._1
            val des = bean._2._1
            val logMsgs = bean._2._2.toList
            val newDes = updateDeviceDes(uuid, des, logMsgs)
            (uuid, newDes)
        })

        uuid2newDesRdd.foreachPartition(iterator => {
            initHBase()
            val tbConn = new ExTBConnection
            val esConn = new ExEsConnection
            val tb: ExTable = tbConn.getTable(HBaseTableName.UUID_DEVICE_MSG)
            iterator.foreach(b => {
                tb.addPut(DeviceDesBuidler.deviceDes2Put(b._2))
                esConn.add(EsTableName.ES_INDEX_BIGDATA_DEVICE, EsTableName.ES_TYPE_DEVICE, b._1, DeviceDesBuidler.deviceDes2EsDoc(b._2))
            })
            tbConn.close()
            esConn.close()
        })
    }
}

object WeiXinJobDeviceDes {
    def main(args: Array[String]) {
        val job = new WeiXinJobDeviceDes()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
