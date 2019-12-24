package com.wangjia.bigdata.core.bean.im

/**
  * Created by Administrator on 2018/3/29.
  * IM日志对象
  *
  * @param ja_version JA版本号
  * @param client_id  租户ID
  * @param client     游客ID
  * @param customer   客服ID
  * @param msgtype    消息类型 chat文本消息 img图片消息 welcome欢迎语
  * @param from_type  发送端类型 1游客 2客服
  * @param time       服务器时间
  * @param localTime  客户端时间
  * @param content    消息体
  * @param client_ip  游客IP
  * @param source     页面
  * @param avatar     发送者头像
  */
case class ImLogMsg(ja_version: String,
                    client_id: String,
                    client: String,
                    customer: String,
                    msgtype: String,
                    from_type: Int,
                    time: Long,
                    localTime: Long,
                    content: String,
                    client_ip: String,
                    source: String,
                    avatar: String)