package com.wangjia.handler.gps;

import com.wangjia.common.Config;
import com.wangjia.hbase.ExHbase;
import com.wangjia.hbase.HBaseConst;
import com.wangjia.hbase.HBaseTableName;
import com.wangjia.hbase.conn.ComTBConnection;
import com.wangjia.hbase.conn.GPSTableConnection;
import com.wangjia.hbase.conn.HbaseConn;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Created by Administrator on 2017/5/9.
 */
public class GPSHandler implements HbaseConn{
    private ComTBConnection conn = null;

    protected Table tbUuid2gps = null;

    public GPSHandler() {
        conn = new ComTBConnection();
        tbUuid2gps = conn.getTable(HBaseTableName.UUID_DEVICE_GPS);
    }

    @Override
    public void close() {
        conn.close();
    }

    private String result2GpsMsg(Result rs) {
        try {
            if (rs.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG)) {
                return Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, List<String>> findGpsByUUIDs(List<String> uuids) {
        try {
            byte[] minKey = null;
            byte[] maxKey = null;

            long newT = System.currentTimeMillis();
            long minT = newT - Config.SEARCH_TIME_RANGE;

            int maxNum = 20;
            if (uuids.size() < 10) {
                maxNum = 100;
            } else if (uuids.size() < 50) {
                maxNum = 30;
            } else if (uuids.size() < 100) {
                maxNum = 20;
            }
            int num = 0;
            String gpsMsg = "";

            minT = 0;
            maxNum = Integer.MAX_VALUE;

            Map<String, List<String>> gpsMap = new HashMap<>();
            List<String> list = null;

            for (String uuid : uuids) {
                minKey = Bytes.toBytes(ExHbase.getGPSKey(uuid, newT));
                maxKey = Bytes.toBytes(ExHbase.getGPSKey(uuid, minT));
                Scan scan = new Scan();
                scan.setStartRow(minKey);
                scan.setStopRow(maxKey);
                //scan.setBatch(num);

                ResultScanner results = tbUuid2gps.getScanner(scan);
                Iterator<Result> it = results.iterator();

                list = new LinkedList<>();
                Result rs;
                num = 0;
                while (num++ < maxNum && it.hasNext()) {
                    rs = it.next();
                    gpsMsg = result2GpsMsg(rs);
                    if (gpsMsg != null) {
                        list.add(gpsMsg);
                    }
                }
                if (list.size() > 0) {
                    gpsMap.put(uuid, list);
                }
            }
            return gpsMap;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
