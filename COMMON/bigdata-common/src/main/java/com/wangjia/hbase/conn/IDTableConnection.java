package com.wangjia.hbase.conn;

import com.wangjia.hbase.HBaseTableName;
import com.wangjia.hbase.conn.HBaseConnection;
import com.wangjia.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Table;

/**
 * Created by Administrator on 2017/4/19.
 */
public class IDTableConnection extends HBaseConnection {
    protected Table tbCookie2uuid = null;
    protected Table tbIosIdfa2uuid = null;
    protected Table tbAndroidImei2uuid = null;
    protected Table tbAndroidSystemId2uuid = null;
    protected Table tbMac2uuid = null;
    protected Table tbUuid2deviceId = null;
    protected Table tbUserId2uuid = null;
    protected Table tbIp2uuid = null;
    protected Table tbUuid2visit = null;


    public IDTableConnection() {
        super();
        tbCookie2uuid = HBaseUtils.getTable(conn, HBaseTableName.COOKIE_UUID);
        tbIosIdfa2uuid = HBaseUtils.getTable(conn, HBaseTableName.IOS_IDFA_UUID);
        tbAndroidImei2uuid = HBaseUtils.getTable(conn, HBaseTableName.ANDROID_IMEI_UUID);
        tbAndroidSystemId2uuid = HBaseUtils.getTable(conn, HBaseTableName.ANDROID_SYSTEMID_UUID);
        tbMac2uuid = HBaseUtils.getTable(conn, HBaseTableName.MAC_UUID);
        tbUuid2deviceId = HBaseUtils.getTable(conn, HBaseTableName.UUID_DEVICEID);
        tbUserId2uuid = HBaseUtils.getTable(conn, HBaseTableName.USERID_UUID);
        tbIp2uuid = HBaseUtils.getTable(conn, HBaseTableName.IP_UUID);
        tbUuid2visit = HBaseUtils.getTable(conn, HBaseTableName.UUID_VISIT);
    }

    @Override
    public void close() {
        HBaseUtils.closeConn(conn,
                tbCookie2uuid,
                tbIosIdfa2uuid,
                tbAndroidImei2uuid,
                tbAndroidSystemId2uuid,
                tbMac2uuid,
                tbUuid2deviceId,
                tbUserId2uuid,
                tbIp2uuid,
                tbUuid2visit);
    }

    public Table getTbCookie2uuid() {
        return tbCookie2uuid;
    }

    public Table getTbIosIdfa2uuid() {
        return tbIosIdfa2uuid;
    }

    public Table getTbAndroidImei2uuid() {
        return tbAndroidImei2uuid;
    }

    public Table getTbAndroidSystemId2uuid() {
        return tbAndroidSystemId2uuid;
    }

    public Table getTbMac2uuid() {
        return tbMac2uuid;
    }

    public Table getTbUuid2deviceId() {
        return tbUuid2deviceId;
    }

    public Table getTbUserId2uuid() {
        return tbUserId2uuid;
    }

    public Table getTbIp2uuid() {
        return tbIp2uuid;
    }

    public Table getTbUuid2visit() {
        return tbUuid2visit;
    }
}
