package com.wangjia.bigdata.core.utils

import java.io._
import java.text.SimpleDateFormat
import java.util.{Date, Random}

import com.wangjia.bigdata.core.bean.info.SourceInfo
import com.wangjia.bigdata.core.common.Config

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/2/24.
  */
object Utils {

    /**
      * 判断两个Byte数据是否相等
      *
      * @param b1          第一个Bytep[]
      * @param startIndex1 第一个起始位置
      * @param b2          第二个Bytep[]
      * @param startIndex2 第二个起始位置
      * @param lenght      要比较的长度
      * @return 相等true
      */
    def isEquals(b1: Array[Byte], startIndex1: Int, b2: Array[Byte], startIndex2: Int, lenght: Int): Boolean = {
        if (b1.length - startIndex1 < lenght)
            return false
        if (b2.length - startIndex2 < lenght)
            return false
        var i = 0
        while (i < lenght) {
            if (b1(startIndex1 + i) != b2(startIndex2 + i))
                return false
            i += 1
        }
        true
    }

    /**
      * 按行读取文件
      *
      * @param path 文件路径
      * @param fun  每行处理方法
      */
    def readFileByLine(path: String, fun: (String) => Unit): Unit = {
        val br = new BufferedReader(new InputStreamReader(new FileInputStream(path)))
        var line: String = null
        while (true) {
            line = br.readLine()
            if (line != null)
                fun(line)
            else
                return
        }
    }

    def readFileByLine(inputStream: InputStream, fun: (String) => Boolean): Unit = {
        val br = new BufferedReader(new InputStreamReader(inputStream))
        var line: String = null
        while (true) {
            line = br.readLine()
            if (line != null)
                fun(line)
            else
                return
        }
    }

    def getkeyWords(url: String, sourceInfo: SourceInfo): String = {
        val key = sourceInfo.keyName + "="
        if (key.length == 1)
            return null
        val sIndex = url.indexOf(key) + key.length
        if (sIndex == -1)
            return null

        val eIndex = url.indexOf('&', sIndex)
        if (eIndex == -1)
            return null

        val str = url.substring(sIndex, eIndex)
        str
    }
}
