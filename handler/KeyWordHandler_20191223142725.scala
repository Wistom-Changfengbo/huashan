package com.wangjia.bigdata.core.handler

import java.net.URLDecoder
import java.util

import com.wangjia.utils.JavaUtils

/**
  * Created by Administrator on 2018/5/2.
  */
object KeyWordHandler {

    def url2KeyWord(sourceid: Int, url: String): (String, String) = {
        val kw = sourceid match {
            case 2 => baiduUrl2KeyWord(url)
            case 3 => sogouUrl2KeyWord(url)
            case 4 => soUrl2KeyWord(url)
            case 5 => bingUrl2KeyWord(url)
            case 6 => googleUrl2KeyWord(url)
            case 7 => yahooUrl2KeyWord(url)
            case 8 => baiduUrl2KeyWord(url)
            case 9 => smUrl2KeyWord(url)
            case _ => null
        }
        if (kw != null && !JavaUtils.isMessyCode(kw._2))
            return kw
        null
    }

    /**
      * 提取URL关键字
      *
      * @param url
      * @return
      */
    private def baiduUrl2KeyWord(url: String): (String, String) = {
        try {
            val request: util.Map[String, String] = JavaUtils.urlRequest(url)
            val ie: String = {
                val _ie: String = request.getOrDefault("ie", "UTF-8")
                _ie match {
                    case "utf-8" => "UTF-8"
                    case "UTF-8" => "UTF-8"
                    case "gbk" => "GBK"
                    case "utf8" => "UTF-8"
                    case "gb2312" => "GB2312"
                    case _ => _ie
                }
            }
            //            val ie: String = "UTF-8"
            if (request.containsKey("wd")) {
                //oq
                val kw = URLDecoder.decode(request.getOrDefault("wd", ""), ie).trim
                if (kw.nonEmpty)
                    return ("kw", kw)
            }
            if (request.containsKey("word")) {
                val kw = URLDecoder.decode(request.getOrDefault("word", ""), ie).trim
                if (kw.nonEmpty)
                    return ("word", kw)
            }
            if (request.containsKey("eqid")) {
                val kw = URLDecoder.decode(request.getOrDefault("eqid", ""), ie).trim
                if (kw.nonEmpty)
                    return ("eqid", kw)
            }
            null
        } catch {
            case e: Exception => null
        }
    }

    private def sogouUrl2KeyWord(url: String): (String, String) = {
        try {
            val request: util.Map[String, String] = JavaUtils.urlRequest(url)
            //keyword   query   htprequery
            val ie: String = {
                val _ie: String = request.getOrDefault("ie", "GB2312")
                _ie match {
                    case "utf-8" => "UTF-8"
                    case "UTF-8" => "UTF-8"
                    case "gbk" => "GBK"
                    case "utf8" => "UTF-8"
                    case "gb2312" => "GB2312"
                    case _ => _ie
                }
            }
            if (request.containsKey("keyword")) {
                val kw = URLDecoder.decode(request.getOrDefault("keyword", ""), ie).trim
                if (kw.nonEmpty)
                    return ("keyword", kw)
            }
            if (request.containsKey("query")) {
                val kw = URLDecoder.decode(request.getOrDefault("query", ""), ie).trim
                if (kw.nonEmpty)
                    return ("query", kw)
            }
            if (request.containsKey("htprequery")) {
                val kw = URLDecoder.decode(request.getOrDefault("htprequery", ""), ie).trim
                if (kw.nonEmpty)
                    return ("htprequery", kw)
            }
            null
        } catch {
            case e: Exception => null
        }
    }

    private def soUrl2KeyWord(url: String): (String, String) = {
        try {
            val request: util.Map[String, String] = JavaUtils.urlRequest(url)
            //q
            val ie: String = {
                val _ie: String = request.getOrDefault("ie", "UTF-8")
                _ie match {
                    case "utf-8" => "UTF-8"
                    case "UTF-8" => "UTF-8"
                    case "gbk" => "GBK"
                    case "utf8" => "UTF-8"
                    case "gb2312" => "GB2312"
                    case _ => _ie
                }
            }
            if (request.containsKey("q")) {
                val kw = URLDecoder.decode(request.getOrDefault("q", ""), ie).trim
                if (kw.nonEmpty)
                    return ("q", kw)
            }
            null
        } catch {
            case e: Exception => null
        }
    }

    private def bingUrl2KeyWord(url: String): (String, String) = {
        try {
            val request: util.Map[String, String] = JavaUtils.urlRequest(url)
            //q pq
            val ie: String = "UTF-8"
            if (request.containsKey("q")) {
                val kw = URLDecoder.decode(request.getOrDefault("q", ""), ie).trim
                if (kw.nonEmpty)
                    return ("q", kw)
            }
            if (request.containsKey("pq")) {
                val kw = URLDecoder.decode(request.getOrDefault("q", ""), ie).trim
                if (kw.nonEmpty)
                    return ("pq", kw)
            }
            null
        } catch {
            case e: Exception => null
        }

    }

    private def googleUrl2KeyWord(url: String): (String, String) = {
        null
    }

    private def yahooUrl2KeyWord(url: String): (String, String) = {
        null
    }

    private def smUrl2KeyWord(url: String): (String, String) = {
        try {
            val request: util.Map[String, String] = JavaUtils.urlRequest(url)
            //q pq
            val ie: String = "UTF-8"
            if (request.containsKey("q")) {
                val kw = URLDecoder.decode(request.getOrDefault("q", ""), ie).trim
                if (kw.nonEmpty)
                    return ("q", kw)
            }
            null
        } catch {
            case e: Exception => null
        }
    }

}
