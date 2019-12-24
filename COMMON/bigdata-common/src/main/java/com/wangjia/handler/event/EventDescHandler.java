package com.wangjia.handler.event;

import com.wangjia.bean.EventDesc;
import com.wangjia.hbase.ExHbase;
import com.wangjia.hbase.HBaseTableName;
import com.wangjia.hbase.conn.ComTBConnection;
import com.wangjia.hbase.conn.HbaseConn;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class EventDescHandler implements HbaseConn {

    private ComTBConnection conn = null;

    protected Table tbEventDesc = null;

    public EventDescHandler() {
        conn = new ComTBConnection();
        tbEventDesc = conn.getTable(HBaseTableName.EVENT_DES);
    }

    @Override
    public void close() {
        conn.close();
    }


    public List<EventDesc> getEventDesc(String appid, String etype, String sub_type, long startTime, long endTime) {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(ExHbase.getEventDesKey(appid, etype, sub_type, endTime)));
        scan.setStopRow(Bytes.toBytes(ExHbase.getEventDesKey(appid, etype, sub_type, startTime)));

        List<EventDesc> eventDescList = new LinkedList<>();
        try {
            ResultScanner scanner = tbEventDesc.getScanner(scan);
            String deviceid = "", ip = "", msg = "", ref = "", url = "", userid = "", uuid = "";
            long time = 0;
            for(Result result:scanner){
                List<Cell> cs=result.listCells();
                for(Cell cell:cs){
                    String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));
                    if (qualifier.equals("time")) {
                        time = Bytes.toLong(CellUtil.cloneValue(cell));
                    } else {
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        switch (qualifier) {
                            case "deviceid":
                                deviceid = value;
                                break;
                            case "ip":
                                ip = value;
                                break;
                            case "msg":
                                msg = value;
                                break;
                            case "ref":
                                ref = value;
                                break;
                            case "url":
                                url = value;
                                break;
                            case "userid":
                                userid = value;
                                break;
                            case "uuid":
                                uuid = value;
                                break;
                        }
                    }
                }
                EventDesc eventDesc = new EventDesc(deviceid, ip, msg, ref, time, url, userid, uuid);
                eventDescList.add(eventDesc);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return eventDescList;
    }
}
