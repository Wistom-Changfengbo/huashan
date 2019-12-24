package com.wangjia.handler.gps;


import com.wangjia.hbase.conn.HbaseConn;
import com.wangjia.hbase.HBaseConst;
import com.wangjia.hbase.HBaseTableName;
import com.wangjia.hbase.conn.ComTBConnection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;


public class LabelGPSHandler implements HbaseConn{
    private ComTBConnection conn = null;

    protected Table tbUuid2Labelgps = null;

    public LabelGPSHandler() {
        conn = new ComTBConnection();
        tbUuid2Labelgps = conn.getTable(HBaseTableName.UUID_LABEL_GPS);
    }

    @Override
    public void close() {
        conn.close();
    }


    /**
     * @param UUids
     * @return
     * @throws IOException
     */
    public List<String> getLabelgps(Set<String> UUids) throws IOException {

        List<Get> listGet = new LinkedList<>();
        List<String> list = new LinkedList<>();

        for (String uuid : UUids) {
            listGet.add(new Get(Bytes.toBytes(uuid)));
        }

        Result[] results = tbUuid2Labelgps.get(listGet);
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
