package com.wangjia.bigdata.core.job

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.wangjia.bigdata.core.utils.JDBCBasicUtils
import com.wangjia.utils.JavaUtils

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
  * Created by Administrator on 2017/5/12.
  */
trait EveryDaySparkJob extends SparkJob {
    private var _parameter: mutable.Map[String, String] = null
    private var _logDate: Date = null

    /**
      * 此变量会写入JOB运行记录
      */
    protected var runLog: String = ""

    /**
      * 得到输入目录
      *
      * @return
      */
    def inPath: String = {
        this._parameter.getOrElse("input", "")
    }

    /**
      * 得到输入目录
      *
      * @return
      */
    def outPath: String = {
        this._parameter.getOrElse("output", "")
    }

    def logDate: Date = {
        if (_logDate != null)
            return _logDate
        val date = _parameter.getOrElse("date", "2018-12-19")
        setLogDate(date)
        this._logDate
    }

    def setLogDate(dateStr: String): Unit = {
        val parse = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH)
        val date = parse.parse(dateStr)
        this._logDate = date
    }

    def setLogDate(date: Date): Unit = {
        this._logDate = date
    }

    def setInPath(path: String): Unit = {
        this._parameter.put("input", path)
    }

    def setOutPath(path: String): Unit = {
        this._parameter.put("output", path)
    }

    /**
      * 初化字段 默认初始化输入头和日期，不同初始化可重载
      *
      * @param array {输入头,日期}
      */
    protected def initField(array: Array[String]): Unit = {
        this._parameter = JavaUtils.parseParameter(array)
        println(this._parameter)
    }

    /**
      * 得到参数的值
      *
      * @param key      参数Key
      * @param defValue 参数默认值
      * @return
      */
    def getParameterOrElse(key: String, defValue: String): String = {
        this._parameter.getOrElse(key, defValue)
    }

    /**
      * 设置运行日志，日志会写入MySql
      *
      * @param log
      */
    protected def setRunLog(log: String): Unit = {
        this.runLog = log
    }

    /**
      * 保存任务记录
      */
    protected def saveJobRecord(): Unit = {
        val start = this.jobStartTime
        val end = System.currentTimeMillis()
        val time = end - start
        val name = if (this._parameter.contains("platform")) this._parameter.getOrElse("platform", "") + "-" + this.jobName else this.jobName
        val inPath = this.inPath
        val outPath = this.outPath
        val logDate = this.logDate

        JDBCBasicUtils.insert("sql_insert_log_job_run", pstmt => {
            pstmt.setString(1, name)
            pstmt.setDate(2, new java.sql.Date(start))
            pstmt.setTimestamp(3, new Timestamp(start))
            pstmt.setTimestamp(4, new Timestamp(end))
            pstmt.setLong(5, time)
            pstmt.setDate(6, new java.sql.Date(logDate.getTime))
            pstmt.setString(7, inPath)
            pstmt.setString(8, outPath)
            pstmt.setString(9, runLog)
        })
    }

    override final def run(args: Array[String]): Unit = {
        initField(args)
        super.run(args)
        saveJobRecord()
    }
}
