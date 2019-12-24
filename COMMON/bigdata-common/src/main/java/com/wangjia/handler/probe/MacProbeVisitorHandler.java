package com.wangjia.handler.probe;

import com.wangjia.hbase.conn.HbaseConn;
import com.wangjia.hbase.HBaseTableName;
import com.wangjia.hbase.conn.ExTBConnection;
import com.wangjia.hbase.conn.ExTable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MacProbeVisitorHandler implements HbaseConn{
    private ExTBConnection conn = null;
    private ExTable _tb = null;

    public MacProbeVisitorHandler() {
        conn = new ExTBConnection();
        _tb = conn.getTable(HBaseTableName.MAC_PROBE_VISIT);
    }

    public MacProbeVisitorHandler(ExTBConnection conn) {
        this.conn = conn;
    }

    @Override
    public void close() {
        conn.close();
    }


    public Map<String, String> findVenueInfoByMac(String mac) throws IOException {
        Get rowId = new Get(Bytes.toBytes(mac));
        Result rs = _tb.getTable().get(rowId);
        Map<String, String> retValue = new HashMap<>();

        List<Cell> cs = rs.listCells();
        for (Cell cell : cs) {
            String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));
            Long value = Bytes.toLong(CellUtil.cloneValue(cell));
            retValue.put(qualifier, Long.toString(value));
        }

        return retValue;
    }
}
