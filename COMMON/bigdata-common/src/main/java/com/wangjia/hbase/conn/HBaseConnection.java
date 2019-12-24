package com.wangjia.hbase.conn;

import com.wangjia.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Created by Administrator on 2017/4/19.
 */
public class HBaseConnection {
    protected Connection conn = null;

    public HBaseConnection() {
        conn = HBaseUtils.getConn();
    }

    public Connection getConn() {
        return conn;
    }

    public void close() {
        HBaseUtils.closeConn(conn);
    }
}
