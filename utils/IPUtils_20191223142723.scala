package com.wangjia.bigdata.core.utils

import java.io.InputStream

import com.wangjia.bigdata.core.bean.info.IpAddress
import com.wangjia.utils.JavaUtils
import com.wangjia.utils.ipip.IPAddressUtils

import scala.collection.mutable

/**
  * Created by Administrator on 2017/2/28.
  */
object IPUtils {

    private val iPAddressUtil: IPAddressUtils = loadIpAddressUtils()

    def loadFilterIPs(inputStream: InputStream): mutable.HashMap[String, Int] = {
        val filterIPs = new mutable.HashMap[String, Int]()
        Utils.readFileByLine(inputStream, (line: String) => {
            filterIPs.put(line.trim, 1)
            true
        })
        filterIPs
    }

    def isFilterIp(filterIPs: mutable.HashMap[String, Int], ip: String): Boolean = {
        filterIPs.get(ip) match {
            case None => false
            case Some(x) => true
            case _ => false
        }
    }

    def loadFilterArrayIPs(inputStream: InputStream): Array[String] = {
        val filterIPs = new mutable.ListBuffer[String]()
        Utils.readFileByLine(inputStream, (line: String) => {
            filterIPs += line.trim

            true
        })
        filterIPs.toArray
    }

    def isFilterIp(filterIPs: Array[String], ips: Array[String]): Boolean = {
        for (ip <- ips) {
            filterIPs.foreach(x => {
                if (ip.startsWith(x)) return true
            })
        }
        false
    }

    def loadIpAddressUtils(): IPAddressUtils = {
        val iPAddressUtil = new IPAddressUtils()
        iPAddressUtil.load(this.getClass.getResourceAsStream("/common/17monipdb.dat"))
        iPAddressUtil
    }

    def ip2Long(ip: String): Long = {
        JavaUtils.ip2Long(ip)
    }

    def getAddress(ip: String): IpAddress = {
        try {
            val adds = this.iPAddressUtil.find(ip)
            return IpAddress(adds)
        } catch {
            case e: Exception => e.printStackTrace(); println(ip)
        }
        IpAddress.NULL
    }
}

