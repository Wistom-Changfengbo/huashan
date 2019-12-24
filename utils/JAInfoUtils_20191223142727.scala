package com.wangjia.bigdata.core.utils

import java.io.InputStream
import java.util.Date

import com.wangjia.bigdata.core.bean.info.{SemUserInfo, _}
import com.wangjia.handler.item.{ItemPattern, MatchItemHandler}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by Administrator on 2017/3/16.
  */
object JAInfoUtils {

    /**
      * 加载应用配置信息
      *
      * @return 应用配置信息Map集合
      */
    def loadAppInfos: mutable.Map[String, AppInfo] = {
        val map = mutable.Map[String, AppInfo]()
        val conn = JDBCBasicUtils.getConn("info")
        val sql = JDBCBasicUtils.getSql("sql_read_app_info")

        JDBCBasicUtils.read(conn, sql, rs => {
            while (rs.next()) {
                val id = rs.getInt("id")
                val appid = rs.getString("appid")
                val apptype = rs.getInt("type")
                val child = rs.getInt("child")
                val identifier = rs.getString("identifier")
                val accountid = rs.getInt("accountid")
                val name = rs.getString("name")
                val host = rs.getString("host")
                val active = rs.getInt("active")
                val des = rs.getString("des")
                val addtime = rs.getLong("addtime")
                val companyId = rs.getInt("company_id")
                map(appid) = AppInfo(id, appid, apptype, child, identifier, accountid, name, host, active, des, addtime, companyId)
            }
        }, null)
        map
    }

    /**
      * 加载帐号系统配置信息
      *
      * @return 号系统配置信息Map集合
      */
    def loadAccountInfos: mutable.Map[Int, AccountInfo] = {
        val map = mutable.Map[Int, AccountInfo]()
        val conn = JDBCBasicUtils.getConn("info")
        val sql = JDBCBasicUtils.getSql("sql_account_info")

        JDBCBasicUtils.read(conn, sql, rs => {
            while (rs.next()) {
                val id = rs.getInt("id")
                val rtype = rs.getInt("type")
                val rules = rs.getString("rules")
                val name = rs.getString("name")
                val des = rs.getString("des")
                map(id) = AccountInfo(id, rtype, rules, name, des)
            }
        }, null)
        map
    }

    /**
      * 加载源信息
      *
      * @return
      */
    def loadSourceInfos: Array[SourceInfo] = {
        val list = ListBuffer[SourceInfo]()
        val conn = JDBCBasicUtils.getConn("info")
        val sql = JDBCBasicUtils.getSql("sql_read_source_info")

        JDBCBasicUtils.read(conn, sql, rs => {
            while (rs.next()) {
                val id = rs.getInt("id")
                val name = rs.getString("name")
                val _type = rs.getInt("type")
                val rules = rs.getString("rules")
                val keyname = rs.getString("keyname")
                val weight = rs.getInt("weight")
                val active = rs.getInt("active")
                list += SourceInfo(id, name, _type, rules, keyname, weight, active)
            }
        }, null)

        list.sortWith(_.weight > _.weight).toArray
    }

    /**
      * 通过应用ID加载页面信息
      *
      * @param appId 应用ID
      * @return
      */
    def loadPageInfoByAppId(appId: String): Array[PageInfo] = {
        val list = ListBuffer[PageInfo]()
        val conn = JDBCBasicUtils.getConn("info")
        val sql = JDBCBasicUtils.getSql("sql_read_page_rules_by_appid")

        JDBCBasicUtils.read(conn, sql, rs => {
            while (rs.next()) {
                val id = rs.getInt("id")
                val appid = rs.getString("appid")
                val childid = rs.getInt("childid")
                val name = rs.getString("name")
                val _type = rs.getInt("type")
                val rules = rs.getString("rules")
                val loose = rs.getInt("loose")
                val weight = rs.getInt("weight")
                val active = rs.getInt("active")

                if (loose == 1)
                    list += PageInfo(id, appid, childid, name, _type, rules, loose, Int.MaxValue, active)
                else
                    list += PageInfo(id, appid, childid, name, _type, rules, loose, weight, active)
            }
        }, Array(appId))

        list.sortWith(_.weight > _.weight).toArray
    }

    /**
      * 通过应用信息加载页面信息
      *
      * @param mapAppInfo
      * @return
      */
    def loadPageInfoByAppInfos(mapAppInfo: mutable.Map[String, AppInfo]): mutable.Map[String, Array[PageInfo]] = {
        val map = mutable.Map[String, Array[PageInfo]]()
        for ((k, v) <- mapAppInfo) {
            if (v.active == 1) {
                map(v.appId) = loadPageInfoByAppId(v.appId)
            }
        }
        map
    }

    /**
      * 通过应用ID加载子产品信息
      *
      * @param appId 应用ID
      * @return
      */
    def loadAppChildInfoByAppId(appId: String): Array[AppChildInfo] = {
        val list = ListBuffer[AppChildInfo]()
        val conn = JDBCBasicUtils.getConn("info")
        val sql = JDBCBasicUtils.getSql("sql_read_app_child_info_by_appid")

        JDBCBasicUtils.read(conn, sql, rs => {
            while (rs.next()) {
                val id = rs.getInt("id")
                val appid = rs.getString("appid")
                val name = rs.getString("name")
                val _type = rs.getInt("type")
                val rules = rs.getString("rules")
                val weight = rs.getInt("weight")
                val active = rs.getInt("active")
                list += AppChildInfo(id, appid, name, _type, rules, weight, active)
            }
        }, Array(appId))

        list.sortWith(_.weight > _.weight).toArray
    }

    /**
      * 通过应用信息加载子产品信息
      *
      * @param mapAppInfo
      * @return
      */
    def loadAppChildInfoByAppInfos(mapAppInfo: mutable.Map[String, AppInfo]): mutable.Map[String, Array[AppChildInfo]] = {
        val map = mutable.Map[String, Array[AppChildInfo]]()
        for ((k, v) <- mapAppInfo) {
            if (v.active == 1 && v.child == 1) {
                map(v.appId) = loadAppChildInfoByAppId(v.appId)
            }
        }
        map
    }

    /**
      * 加载用户标签配置
      *
      * @return
      */
    def loadUserLabelInfo: mutable.Map[Int, ListBuffer[UserLabel]] = {
        val map = mutable.Map[Int, ListBuffer[UserLabel]]()
        val conn = JDBCBasicUtils.getConn("info")
        val sql = JDBCBasicUtils.getSql("sql_read_user_label_info")

        JDBCBasicUtils.read(conn, sql, rs => {
            while (rs.next()) {
                val id = rs.getInt("id")
                val pid = rs.getInt("pid")
                val name = rs.getString("name")
                val _type = rs.getInt("type")
                val rules = rs.getString("rules")
                val des = rs.getString("des")
                val companyId = rs.getInt("company_id")
                if (!map.contains(companyId)) {
                    map.put(companyId, new ListBuffer[UserLabel]())
                }
                val list: ListBuffer[UserLabel] = map.getOrElse(companyId, null)
                list += UserLabel(id, pid, _type, rules, name, des, companyId)
            }
        }, null)
        map
    }

    /**
      * 加载探针类型分析
      *
      * @return
      */
    def loadProbeInfo: mutable.Map[String, ProbeInfo] = {
        val map = mutable.Map[String, ProbeInfo]()
        val conn = JDBCBasicUtils.getConn("info")
        val sql = JDBCBasicUtils.getSql("sql_probe_info")

        JDBCBasicUtils.read(conn, sql, rs => {
            while (rs.next()) {
                val uuid = rs.getString("uuid")
                val probeid = rs.getString("probeid")
                val signal = rs.getInt("signal")
                val ftype = rs.getString("type")
                val active = rs.getInt("active")
                val name = rs.getString("name")
                val jd = rs.getDouble("jd")
                val wd = rs.getDouble("wd")
                val venueId = rs.getInt("venue_id")
                map(uuid) = ProbeInfo(uuid, probeid, signal, ftype, name, active, jd, wd, venueId)
            }
        }, null)
        map
    }


    /**
      * 加载MAC地址与手机品牌对应关系
      *
      * @return
      */
    def loadMac2PhoneBrand: mutable.Map[String, String] = {
        val asStream: InputStream = this.getClass.getResourceAsStream("/common/mac2phoneBrand")
        val map = mutable.Map[String, String]()
        Source.fromInputStream(asStream).getLines().foreach(line => {
            val fileds = line.split('|')
            if (fileds.size == 2 && fileds(0).length == 6)
                map(fileds(0)) = fileds(1)
        })
        map
    }

    /**
      * 加载图片属性配置
      *
      * @return
      */
    def loadPhotoAttributeInfo: mutable.Map[String, String] = {
        val map = mutable.Map[String, String]()
        val conn = JDBCBasicUtils.getConn("info")
        val sql = JDBCBasicUtils.getSql("sql_read_photo_attribute_info")

        JDBCBasicUtils.read(conn, sql, rs => {
            while (rs.next()) {
                val _type = rs.getString("type")
                val id = rs.getInt("id")
                val name = rs.getString("name")
                map(_type + id) = name
            }
        }, null)
        map
    }

    /**
      * 加载推荐物品简析规则
      *
      * @return
      */
    def loadItemRulesInfo: MatchItemHandler = {
        val handler: MatchItemHandler = new MatchItemHandler
        val conn = JDBCBasicUtils.getConn("info")
        val sql = JDBCBasicUtils.getSql("sql_read_recommend_item_rules_info")

        JDBCBasicUtils.read(conn, sql, rs => {
            while (rs.next()) {
                val id = rs.getInt("id")
                val appid = rs.getString("appid")
                val company_id = rs.getInt("company_id")
                val item_type_id = rs.getInt("item_type_id")
                val star_level = rs.getFloat("star_level")
                val behavior_type = rs.getInt("behavior_type")
                val behavior_rule_type = rs.getInt("behavior_rule_type")
                val behavior_rule = rs.getString("behavior_rule")
                val data_rule_type = rs.getInt("data_rule_type")
                val data_rule = rs.getString("data_rule")
                val weight = rs.getInt("weight")
                val des = rs.getString("des")
                val active = rs.getInt("active")
                val addtime = rs.getLong("addtime")

                val rule = new ItemPattern(id, appid, company_id, item_type_id, star_level, behavior_type, behavior_rule_type, behavior_rule, data_rule_type, data_rule, weight, des, active, addtime)
                handler.addItemMatcher(rule)
            }
        }, null)
        handler
    }

    /**
      * 加载AppLabel的属性。
      *
      * @return
      */
    def loadAppLabelList: mutable.ListBuffer[(String, Int, String, String)] = {
        val list = mutable.ListBuffer[(String, Int, String, String)]()
        val conn = JDBCBasicUtils.getConn("info")
        val sql = JDBCBasicUtils.getSql("sql_read_app_label")

        JDBCBasicUtils.read(conn, sql, rs => {
            while (rs.next()) {
                val platform = rs.getString("platform")
                val tag_id = rs.getInt("tag_id")
                val pkg_name = rs.getString("pkg_name")
                val tag_name = rs.getString("tag_name")
                list += ((pkg_name, tag_id, tag_name, platform))
            }
        }, null)
        list
    }

    /**
      * 加载线索表ja_data_user_clue的去重后的deviceid信息
      *
      * @return
      */
    def loadUserClueDeviceIdList(date: Date): ListBuffer[(String, Int)] = {
        val list = mutable.ListBuffer[(String, Int)]()
        JDBCBasicUtils.read("sql_read_user_clue", rs => {
            while (rs.next()) {
                val deviceid = rs.getString("deviceid")
                list += ((deviceid, 1))
            }
        }, scala.Array[Object](date))
        list
    }


    /**
      * 加载Sem 用户配置
      *
      * @return
      */
    def loadSemUserInfo(): ListBuffer[SemUserInfo] = {
        val conn = JDBCBasicUtils.getConn("info")
        val sql = JDBCBasicUtils.getSql("sql_sem_info")
        val list = ListBuffer[SemUserInfo]()
        JDBCBasicUtils.read(conn, sql, rs => {
            while (rs.next()) {
                val id = rs.getInt("id")
                val appid = rs.getString("appid")
                val sourceid = rs.getString("sourceid")
                val username = rs.getString("username")
                val password = rs.getString("password")
                val token = rs.getString("token")
                val target = rs.getString("target")
                list += SemUserInfo(id, appid, sourceid, username, password, token, target)
            }
        }, null)
        list
    }

    /**
      * 加载EQID 用户配置
      *
      * @return
      */
    def loadEqidUserInfo(): mutable.Map[String, EqidUserInfo] = {
        val conn = JDBCBasicUtils.getConn("info")
        val sql = JDBCBasicUtils.getSql("sql_eqid_info")
        val map = mutable.Map[String, EqidUserInfo]()
        JDBCBasicUtils.read(conn, sql, rs => {
            while (rs.next()) {
                val id = rs.getInt("id")
                val appid = rs.getString("appid")
                val sourceid = rs.getString("sourceid")
                val username = rs.getString("username")
                val accesskey = rs.getString("access_key")
                val secretkey = rs.getString("secret_key")
                val target = rs.getString("target")
                map += ((appid, EqidUserInfo(id, appid, sourceid, username, accesskey, secretkey, target)))
            }
        }, null)
        map
    }
}
