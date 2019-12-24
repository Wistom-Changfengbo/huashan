package com.wangjia.handler.url;

import com.wangjia.hbase.conn.HbaseConn;
import com.wangjia.hbase.HBaseConst;
import com.wangjia.hbase.HBaseTableName;
import com.wangjia.hbase.conn.ComTBConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Administrator on 2017/5/11.
 */
public class UrlHandler implements HbaseConn{
    private ComTBConnection conn = null;

    protected Table tbUrl2browselog = null;

    public UrlHandler() {
        conn = new ComTBConnection();
        tbUrl2browselog = conn.getTable(HBaseTableName.URL_BROWSE_LOG);
    }

    @Override
    public void close() {
        conn.close();
    }

    /**
     * 得到UUID集合
     *
     * @param appid           APPID
     * @param urlMd5          URL的MD5
     * @param startTimeMillis 开始时间戳
     * @param endTimeMillis   结束时间戳
     * @return
     */
    public List<String> findBrowseLog(String appid, String urlMd5, long startTimeMillis, long endTimeMillis) {
        try {
            String minKey = urlMd5 + appid + startTimeMillis;
            String maxKey = urlMd5 + appid + (endTimeMillis + 1);

            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(minKey));
            scan.setStopRow(Bytes.toBytes(maxKey));

            ResultScanner scanner = tbUrl2browselog.getScanner(scan);
            Iterator<Result> it = scanner.iterator();
            Result rs;

            List<String> logs = new LinkedList<>();

            while (it.hasNext()) {
                rs = it.next();
                if (rs.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG)) {
                    logs.add(Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG)));
                }
            }
            return logs;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
