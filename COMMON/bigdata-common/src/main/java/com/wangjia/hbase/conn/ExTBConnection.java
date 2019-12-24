package com.wangjia.hbase.conn;

import com.wangjia.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 扩展Table的连接
 */
public class ExTBConnection extends HBaseConnection {
    protected Map<String, ExTable> tbMap = new HashMap<>();

    public ExTBConnection() {
        super();
    }

    @Override
    public void close() {
        for (Map.Entry<String, ExTable> entry : tbMap.entrySet()) {
            entry.getValue().close();
        }
        HBaseUtils.closeConn(conn);
    }

    /**
     * 通过表名得到表
     *
     * @param tableName 表名
     * @return
     */
    public ExTable getTable(String tableName) {
        ExTable exTb = tbMap.get(tableName);
        if (exTb == null) {
            Table tb = HBaseUtils.getTable(conn, tableName);
            exTb = new ExTable(tb);
            tbMap.put(tableName, exTb);
        }
        return exTb;
    }

    /**
     * 通过表名关闭表
     *
     * @param tableName 表名
     * @return
     */
    public void closeTable(String tableName) {
        ExTable exTb = tbMap.get(tableName);
        if (exTb == null) {
            return;
        }
        exTb.close();
        tbMap.remove(tableName);
    }

    public void addPut(String tableName, Put put) {
        ExTable exTb = getTable(tableName);
        exTb.addPut(put);
    }

    public void addPut(String tableName, List<Put> puts) {
        ExTable exTb = getTable(tableName);
        exTb.addPut(puts);
    }

    public void flush(){
        for (Map.Entry<String, ExTable> entry : tbMap.entrySet()) {
            entry.getValue().flush();
        }
    }
}