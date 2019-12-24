package com.wangjia.handler.usercenter;

import com.wangjia.hbase.ExHbase;
import com.wangjia.hbase.HBaseConst;
import com.wangjia.hbase.HBaseTableName;
import com.wangjia.hbase.conn.ComTBConnection;
import com.wangjia.hbase.conn.HbaseConn;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Administrator on 2017/5/23.
 */
public class UserCenterHandler implements HbaseConn{
    private ComTBConnection conn = null;

    protected Table tbUUID2DeviceMsg = null;
    protected Table tbUserId2UUID = null;
    protected Table tbUserCenter = null;

    public UserCenterHandler() {
        conn = new ComTBConnection();
        tbUUID2DeviceMsg = conn.getTable(HBaseTableName.UUID_DEVICE_MSG);
        tbUserId2UUID = conn.getTable(HBaseTableName.USERID_UUID);
        tbUserCenter = conn.getTable(HBaseTableName.USERCENTER);
    }

    @Override
    public void close() {
        conn.close();
    }

    public List<String> findUUIDByUCID(String ucId) {
        try {
            Get get = new Get(Bytes.toBytes(ucId));
            Result rs = tbUserCenter.get(get);
            List<String> uuids = new LinkedList<>();

            KeyValue[] keyvalues = rs.raw();
            for (KeyValue kv : keyvalues) {
                String uuid = Bytes.toString(kv.getQualifier());
                if (uuid.length() == 21) {
                    uuids.add(uuid.substring(5));
                }
            }
            return uuids;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<String> findUUIDByUUID(String uuid) {
        try {
            Get get = new Get(Bytes.toBytes(uuid));
            Result rs = tbUUID2DeviceMsg.get(get);
            if (rs.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_UCID)) {
                String ucid = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_UCID));
                return findUUIDByUCID(ucid);
            }
            List uuids = new LinkedList<>();
            uuids.add(uuid);
            return uuids;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<String> findUUIDByUserId(String userId, int accountId) {
        try {
            Get get = new Get(Bytes.toBytes(ExHbase.getUserKey(userId, accountId)));
            Result rs = tbUserId2UUID.get(get);
            if (rs.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_UCID)) {
                String ucid = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_UCID));
                return findUUIDByUCID(ucid);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
