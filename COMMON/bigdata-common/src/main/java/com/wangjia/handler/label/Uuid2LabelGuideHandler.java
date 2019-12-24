package com.wangjia.handler.label;

import com.wangjia.hbase.conn.HbaseConn;
import com.wangjia.hbase.HBaseConst;
import com.wangjia.hbase.HBaseTableName;
import com.wangjia.hbase.conn.ExTBConnection;
import com.wangjia.hbase.conn.ExTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class Uuid2LabelGuideHandler implements HbaseConn{

    private ExTBConnection conn = null;
    private ExTable _tb = null;

    public Uuid2LabelGuideHandler() {
        conn = new ExTBConnection();
        _tb = conn.getTable(HBaseTableName.UUID_LABEL_GUIDE);
    }

    public Uuid2LabelGuideHandler(ExTBConnection conn) {
        this.conn = conn;
    }

    @Override
    public void close() {
        conn.close();
    }


    /**
     *
     * @param UUids
     * @return
     * @throws IOException
     */
    public LinkedList<String> findLabelGuideByUUids(Set<String> UUids) throws IOException {
        LinkedList<Get> listGet = new LinkedList<>();
        LinkedList<String> list = new LinkedList<>();
        for (String uuid : UUids) {
            listGet.add(new Get(Bytes.toBytes(uuid)));
        }
        Result[] results = _tb.getTable().get(listGet);
        for (Result rs: results) {
            String msg = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG));
            if (msg == null) {
                continue;
            }
            list.add(msg);
        }
        return list;
    }
}
