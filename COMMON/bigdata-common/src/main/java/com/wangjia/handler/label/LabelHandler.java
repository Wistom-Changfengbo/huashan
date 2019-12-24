package com.wangjia.handler.label;

import com.wangjia.bean.Label;
import com.wangjia.hbase.conn.HbaseConn;
import com.wangjia.hbase.HBaseTableName;
import com.wangjia.hbase.conn.ComTBConnection;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Created by Administrator on 2017/5/15.
 */
public class LabelHandler implements HbaseConn {
    private ComTBConnection conn = null;

    protected Table tbUuid2label = null;

    private int newDay = 0;

    public LabelHandler() {
        conn = new ComTBConnection();
        tbUuid2label = conn.getTable(HBaseTableName.UUID_LABEL_SUM);
    }


    private void toMap(Map<String, Label> maps, String lstr) {
        String[] fields = lstr.split("`");
        try {
            String id = fields[0];
            int day = Integer.parseInt(fields[2]);
            float value = Float.parseFloat(fields[3]);
            Label label = maps.get(id);
            if (label == null) {
                label = new Label(id, fields[1], this.newDay, 0);
                maps.put(id, label);
            }
            label.addValue(day, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 得到UUID集合
     *
     * @param uuid     UUID
     * @param startDay 开始天数
     * @param endDay   结束天数
     * @return
     */
    public Set<String> findLabel(String uuid, int startDay, int endDay, Map<String, Label> maps) {
        this.newDay = endDay;
        try {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(uuid + startDay));
            scan.setStopRow(Bytes.toBytes(uuid + endDay));

            ResultScanner scanner = tbUuid2label.getScanner(scan);
            Iterator<Result> it = scanner.iterator();
            Result rs;

            while (it.hasNext()) {
                rs = it.next();
                KeyValue[] keyvalues = rs.raw();
                String value = "";
                for (KeyValue kv : keyvalues) {
                    value = Bytes.toString(kv.getValue());
                    toMap(maps, value);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, Label> findLabel(List<String> uuids, int startDay, int endDay) {
        Map<String, Label> maps = new HashMap<>();
        for (String uuid : uuids) {
            findLabel(uuid, startDay, endDay, maps);
        }
        return maps;
    }

    @Override
    public void close() {
        conn.close();
    }
}
