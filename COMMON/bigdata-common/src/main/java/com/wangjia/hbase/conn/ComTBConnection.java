package com.wangjia.hbase.conn;

import com.wangjia.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * HBaseTable通用连接
 */
public class ComTBConnection extends HBaseConnection {
    protected Map<String, Table> tbMap = new HashMap<>();

    public ComTBConnection() {
        super();
    }

    @Override
    public void close() {
        for (Map.Entry<String, Table> entry : tbMap.entrySet()) {
            HBaseUtils.closeTable(entry.getValue());
        }
        HBaseUtils.closeConn(conn);
    }

    /**
     * 通过表名得到表
     *
     * @param tableName 表名
     * @return
     */
    public Table getTable(String tableName) {
        Table tb = tbMap.get(tableName);
        if (tb == null) {
            tb = HBaseUtils.getTable(conn, tableName);
            tbMap.put(tableName, tb);
        }
        return tb;
    }

    /**
     * 通过表名关闭表
     *
     * @param tableName 表名
     * @return
     */
    public void closeTable(String tableName) {
        Table tb = tbMap.get(tableName);
        if (tb == null) {
            return;
        }
        HBaseUtils.closeTable(tb);
        tbMap.remove(tableName);
    }
}