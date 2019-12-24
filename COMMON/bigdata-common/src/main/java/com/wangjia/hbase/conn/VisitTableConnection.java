package com.wangjia.hbase.conn;

import com.wangjia.hbase.HBaseTableName;
import com.wangjia.hbase.conn.HBaseConnection;
import com.wangjia.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Table;

/**
 * Created by Administrator on 2017/4/27.
 */
public class VisitTableConnection extends HBaseConnection {
    protected Table tbUuid2visit = null;
    protected Table tbUuid2visitdes = null;
    protected Table tbUuid2applist = null;
    protected Table tbUuid2devicemsg = null;


    public VisitTableConnection() {
        super();
        tbUuid2visit = HBaseUtils.getTable(conn, HBaseTableName.UUID_VISIT);
        tbUuid2visitdes = HBaseUtils.getTable(conn, HBaseTableName.UUID_VISIT_DES);
        tbUuid2applist = HBaseUtils.getTable(conn, HBaseTableName.UUID_APPLIST);
        tbUuid2devicemsg = HBaseUtils.getTable(conn, HBaseTableName.UUID_DEVICE_MSG);
    }

    @Override
    public void close() {
        HBaseUtils.closeConn(conn, tbUuid2visit, tbUuid2visitdes, tbUuid2applist, tbUuid2devicemsg);
    }

    public Table getTbUuid2visit() {
        return tbUuid2visit;
    }

    public Table getTbUuid2visitdes() {
        return tbUuid2visitdes;
    }

    public Table getTbUuid2applist() {
        return tbUuid2applist;
    }

    public Table getTbUuid2devicemsg() {
        return tbUuid2devicemsg;
    }
}
