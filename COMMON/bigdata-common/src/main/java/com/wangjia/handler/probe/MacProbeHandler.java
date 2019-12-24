package com.wangjia.handler.probe;

import com.wangjia.hbase.HBaseConst;
import com.wangjia.hbase.HBaseTableName;
import com.wangjia.hbase.conn.ExTBConnection;
import com.wangjia.hbase.conn.ExTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Administrator on 2017/9/20.
 */
public class MacProbeHandler {
    private ExTBConnection conn = null;
    private ExTable _tb = null;

    public MacProbeHandler() {
        conn = new ExTBConnection();
        _tb = conn.getTable(HBaseTableName.MAC_PROBE);
    }

    public MacProbeHandler(ExTBConnection conn) {
        this.conn = conn;
    }

    public void close() {
        conn.close();
    }

    public List<ProbeDto> findMac(List<ProbeDto> pds) {
        List<Get> listGet = new LinkedList<>();
        for (int i = 0; i < pds.size(); i++) {
            listGet.add(new Get(Bytes.toBytes(pds.get(i).getMac())));
        }
        try {
            Result[] results = _tb.getTable().get(listGet);
            Result rs = null;
            ProbeDto pd = null;
            byte[] key = null;
            for (int i = 0; i < pds.size(); i++) {
                rs = results[i];
                pd = pds.get(i);
                key = Bytes.toBytes(pd.getVenueId() + "");
                if (!rs.containsColumn(HBaseConst.BYTES_CF1, key)) {
                    Put put = new Put(Bytes.toBytes(pd.getMac()));
                    put.addColumn(HBaseConst.BYTES_CF1, key, Bytes.toBytes(pd.getTime()));
                    _tb.addPut(put);
                    pd.setFirstTime(pd.getTime());
                } else {
                    long time = Bytes.toLong(rs.getValue(HBaseConst.BYTES_CF1, key));
                    if (time > pd.getTime()) {
                        Put put = new Put(Bytes.toBytes(pd.getMac()));
                        put.addColumn(HBaseConst.BYTES_CF1, key, Bytes.toBytes(pd.getTime()));
                        _tb.addPut(put);
                        pd.setFirstTime(pd.getTime());
                    } else {
                        pd.setFirstTime(time);
                    }
                }
            }
            _tb.put();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return pds;
    }

}
