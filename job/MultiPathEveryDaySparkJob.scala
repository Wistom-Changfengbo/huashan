package com.wangjia.bigdata.core.job

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.wangjia.bigdata.core.utils.JDBCBasicUtils
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by Administrator on 2017/5/12.
  */
trait MultiPathEveryDaySparkJob[T] extends SparkJob {
    private var _inPathTemplate: String = ""

    private var _inPaths: mutable.HashSet[(String, String)] = new mutable.HashSet[(String, String)]

    private val _action: mutable.HashMap[String, (String) => RDD[T]] = new mutable.HashMap[String, (String) => RDD[T]]

    /**
      * 此变量会写入JOB运行记录
      */
    protected var runLog: String = ""

    /**
      * 产生日志的日期
      */
    protected var logDate: Date = null

    def setLogDate(day: String): Unit = {
        val parse = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH)
        val date = parse.parse(day)
        this.logDate = date
    }

    def setLogDate(day: Date): Unit = {
        this.logDate = day
    }

    def setInPathTemplate(temp: String) = {
        this._inPathTemplate = temp
    }

    def setInPathHeadAndLogDate(temp: String, day: String): Unit = {
        setInPathTemplate(temp)
        setLogDate(day)

        val heads = temp.split(';')

        heads.foreach(x => {
            val ts = x.split('|')
            if (ts.size == 1)
                _inPaths += (("", ts(0)))
            else
                _inPaths += ((ts(0), ts(1)))
        })
    }

    def setInPathHeadAndLogDate(args: Array[String]): Unit = {
        setInPathHeadAndLogDate(args(0), args(1))
    }

    def getInPaths: mutable.HashSet[(String, String)] = this._inPaths

    /**
      * 初化字段 默认初始化输入头和日期，不同初始化可重载
      *
      * @param array {输入头,日期}
      */
    protected def initField(array: Array[String]): Unit = {
        val len = array.length
        if (len < 2) {
            throw new RuntimeException("parameter number error")
        }
        this.setInPathHeadAndLogDate(array)
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
        val name = this.jobName
        var inPath = ""
        this._inPaths.foreach(x => {
            inPath += x._2 + ";"
        })
        inPath = inPath.substring(0, inPath.length - 1)

        val outPath = ""
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

    def setAction(key: String, fun: (String) => RDD[T]): Unit = {
        _action.put(key, fun)
    }

    def action(): RDD[T] = {
        var sumRdd: RDD[T] = null
        this._inPaths.foreach(x => {
            val fun = _action.getOrElse(x._1, null)
            if (fun != null) {
                println(x._2)
                val rdd: RDD[T] = fun(x._2)
                if (sumRdd == null)
                    sumRdd = rdd
                else
                    sumRdd = sumRdd.union(rdd)
            }
        })
        sumRdd
    }
}
