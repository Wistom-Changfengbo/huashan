package com.wangjia.hbase.conn;

import com.wangjia.hbase.HBaseTableName;
import com.wangjia.hbase.conn.HBaseConnection;
import com.wangjia.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Table;

/**
 * Created by Administrator on 2017/5/9.
 */
public class GPSTableConnection extends HBaseConnection {
    protected Table tbUuid2gps = null;

    public GPSTableConnection() {
        super();
        tbUuid2gps = HBaseUtils.getTable(conn, HBaseTableName.UUID_DEVICE_GPS);
    }

    @Override
    public void close() {
        HBaseUtils.closeConn(conn, tbUuid2gps);
    }

    public Table getTbUuid2gps() {
        return tbUuid2gps;
    }

}
