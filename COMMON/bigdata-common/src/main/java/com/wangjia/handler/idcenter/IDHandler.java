package com.wangjia.handler.idcenter;

import com.wangjia.hbase.ExHbase;
import com.wangjia.hbase.HBaseConst;
import com.wangjia.hbase.HBaseTableName;
import com.wangjia.hbase.conn.ComTBConnection;
import com.wangjia.hbase.conn.HbaseConn;
import com.wangjia.hbase.conn.IDTableConnection;
import com.wangjia.utils.DeviceIDUtils;
import com.wangjia.utils.JavaUtils;
import com.wangjia.utils.UUID;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Created by Administrator on 2017/4/19.
 */
public class IDHandler implements HbaseConn{
    private ComTBConnection conn = null;

    public IDHandler() {
        conn = new ComTBConnection();
    }

    public IDHandler(ComTBConnection conn) {
        this.conn = conn;
    }

    @Override
    public void close() {
        conn.close();
    }

    /**
     * 保存到HBase
     *
     * @param keys   rowkey
     * @param values 值
     * @param tb     HBase 表
     * @param cf     列簇
     * @param cn     列名
     * @return 插入的值
     */
    private List<String> save(List<String> keys, List<String> values, Table tb, byte[] cf, byte[] cn) {
        List<Put> listPut = new LinkedList<>();
        int size = values.size();
        Put put;
        for (int i = 0; i < size; i++) {
            put = new Put(Bytes.toBytes(keys.get(i)));
            put.addColumn(cf, cn, Bytes.toBytes(values.get(i)));
            listPut.add(put);
        }
        try {
            tb.put(listPut);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return values;
    }

    /**
     * 保存ID
     *
     * @param ids  id集合
     * @param type id的类型 1、cookie 2、ios idfa 3、android imei 4、android systemid 5、android imei and systemid
     * @return
     */
    public List<String> addUUID(List<String> ids, int type) {
        List<String> uuids = UUID.randomUUID(ids.size());
        switch (type) {
            case IDType.ID_COOKIE: {
                save(ids, uuids, conn.getTable(HBaseTableName.COOKIE_UUID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_V);
                save(uuids, ids, conn.getTable(HBaseTableName.UUID_DEVICEID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_COOKIE);
                break;
            }
            case IDType.ID_IOS_IDFA: {
                save(ids, uuids, conn.getTable(HBaseTableName.IOS_IDFA_UUID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_V);
                save(uuids, ids, conn.getTable(HBaseTableName.UUID_DEVICEID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_IOS_IDFA);
                break;
            }
            case IDType.ID_ANDROID_IMEI: {
                save(ids, uuids, conn.getTable(HBaseTableName.ANDROID_IMEI_UUID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_V);
                save(uuids, ids, conn.getTable(HBaseTableName.UUID_DEVICEID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_ANDROID_IMEI);
                break;
            }
            case IDType.ID_ANDROID_SYSTEMID: {
                save(ids, uuids, conn.getTable(HBaseTableName.ANDROID_SYSTEMID_UUID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_V);
                save(uuids, ids, conn.getTable(HBaseTableName.UUID_DEVICEID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_ANDROID_SYSTEMID);
                break;
            }
            case IDType.ID_ANDROID_IMEI_SYSTEMID: {
                List<String> imeis = new LinkedList<>();
                List<String> imuuids = new LinkedList<>();
                List<String> systemids = new LinkedList<>();
                List<String> syuuids = new LinkedList<>();
                String id, imei, systemid;
                for (int i = 0; i < ids.size(); i++) {
                    id = ids.get(i);
                    imei = DeviceIDUtils.getAndroidImei(id);
                    systemid = DeviceIDUtils.getAndroidSystemId(id);

                    if (imei != null) {
                        imeis.add(imei);
                        imuuids.add(uuids.get(i));
                    }
                    if (systemid != null) {
                        systemids.add(systemid);
                        syuuids.add(uuids.get(i));
                    }
                }
                save(imeis, imuuids, conn.getTable(HBaseTableName.ANDROID_IMEI_UUID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_V);
                save(imuuids, imeis, conn.getTable(HBaseTableName.UUID_DEVICEID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_ANDROID_IMEI);
                save(systemids, syuuids, conn.getTable(HBaseTableName.ANDROID_SYSTEMID_UUID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_V);
                save(syuuids, systemids, conn.getTable(HBaseTableName.UUID_DEVICEID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_ANDROID_SYSTEMID);
                break;
            }
        }
        return uuids;
    }

    private List<String> find(List<String> keys, Table tb, byte[] cf, byte[] cn) {
        List<String> values = new LinkedList<>();
        List<Get> listGet = new LinkedList<>();
        int size = keys.size();
        Get get;
        for (int i = 0; i < size; i++) {
            get = new Get(Bytes.toBytes(keys.get(i)));
            listGet.add(get);
        }
        try {
            Result[] results = tb.get(listGet);
            Result r;
            byte[] bs;
            for (int i = 0; i < size; i++) {
                r = results[i];
                bs = r.getValue(cf, cn);
                if (bs == null) {
                    values.add(HBaseConst.NULL_STRING_VALUE);
                } else {
                    values.add(Bytes.toString(bs));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return values;
    }

    public List<String> findUUID(List<String> ids, int type) {
        List<String> uuids = null;
        switch (type) {
            case IDType.ID_COOKIE: {
                uuids = find(ids, conn.getTable(HBaseTableName.COOKIE_UUID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_V);
                break;
            }
            case IDType.ID_IOS_IDFA: {
                uuids = find(ids, conn.getTable(HBaseTableName.IOS_IDFA_UUID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_V);
                break;
            }
            case IDType.ID_ANDROID_IMEI: {
                uuids = find(ids, conn.getTable(HBaseTableName.ANDROID_IMEI_UUID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_V);
                break;
            }
            case IDType.ID_ANDROID_SYSTEMID: {
                uuids = find(ids, conn.getTable(HBaseTableName.ANDROID_SYSTEMID_UUID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_V);
                break;
            }
            case IDType.ID_ANDROID_IMEI_SYSTEMID: {
                List<String> imeis = new LinkedList<>();
                List<String> systemids = new LinkedList<>();
                String id, imei, systemid;
                for (int i = 0; i < ids.size(); i++) {
                    id = ids.get(i);
                    imei = DeviceIDUtils.getAndroidImei(id);
                    systemid = DeviceIDUtils.getAndroidSystemId(id);

                    if (imei != null) {
                        imeis.add(imei);
                    } else {
                        imeis.add(HBaseConst.NULL_STRING_VALUE);
                    }
                    if (imei == null && systemid != null) {
                        systemids.add(systemid);
                    } else {
                        systemids.add(HBaseConst.NULL_STRING_VALUE);
                    }
                }

                List<String> iuuids = find(imeis, conn.getTable(HBaseTableName.ANDROID_IMEI_UUID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_V);
                List<String> suuids = find(systemids, conn.getTable(HBaseTableName.ANDROID_SYSTEMID_UUID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_V);
                uuids = new LinkedList<>();
                for (int i = 0; i < iuuids.size(); i++) {
                    if (iuuids.get(i) != HBaseConst.NULL_STRING_VALUE)
                        uuids.add(iuuids.get(i));
                    else
                        uuids.add(suuids.get(i));
                }
                List<String> fimeis = new LinkedList<>();
                List<String> fiuuids = new LinkedList<>();
                for (int i = 0; i < iuuids.size(); i++) {
                    if (imeis.get(i) != HBaseConst.NULL_STRING_VALUE
                            && iuuids.get(i) == HBaseConst.NULL_STRING_VALUE
                            && suuids.get(i) != HBaseConst.NULL_STRING_VALUE) {
                        fimeis.add(imeis.get(i));
                        fiuuids.add(suuids.get(i));
                    }
                }

                if (fimeis.size() > 0) {
                    save(fimeis, fiuuids, conn.getTable(HBaseTableName.ANDROID_IMEI_UUID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_V);
                    save(fiuuids, fimeis, conn.getTable(HBaseTableName.UUID_DEVICEID), HBaseConst.BYTES_CF1, HBaseConst.BYTES_ANDROID_IMEI);
                }

                break;
            }
        }
        return uuids;
    }

    public List<String> getFreeKey(List<String> keys, List<String> values) {
        List<String> errorKeys = new LinkedList<>();
        for (int i = 0; i < keys.size(); i++) {
            if (values.get(i) == HBaseConst.NULL_STRING_VALUE)
                errorKeys.add(keys.get(i));
        }
        return errorKeys;
    }

    public String findUUID(String id, int type) {
        String uuid = null;
        try {
            switch (type) {
                case IDType.ID_COOKIE: {
                    Result rs = conn.getTable(HBaseTableName.COOKIE_UUID).get(new Get(Bytes.toBytes(id)));
                    if (rs.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_V))
                        uuid = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_V));
                    break;
                }
                case IDType.ID_IOS_IDFA: {
                    Result rs = conn.getTable(HBaseTableName.IOS_IDFA_UUID).get(new Get(Bytes.toBytes(id)));
                    if (rs.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_V))
                        uuid = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_V));
                    break;
                }
                case IDType.ID_ANDROID_IMEI: {
                    Result rs = conn.getTable(HBaseTableName.ANDROID_IMEI_UUID).get(new Get(Bytes.toBytes(id)));
                    if (rs.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_V))
                        uuid = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_V));
                    break;
                }
                case IDType.ID_ANDROID_SYSTEMID: {
                    Result rs = conn.getTable(HBaseTableName.ANDROID_SYSTEMID_UUID).get(new Get(Bytes.toBytes(id)));
                    if (rs.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_V))
                        uuid = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_V));
                    break;
                }
                case IDType.ID_ANDROID_IMEI_SYSTEMID: {
                    String imei = DeviceIDUtils.getAndroidImei(id);
                    if (imei != null) {
                        Result rs = conn.getTable(HBaseTableName.ANDROID_IMEI_UUID).get(new Get(Bytes.toBytes(imei)));
                        if (rs.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_V))
                            uuid = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_V));
                        break;
                    }
                    String systemid = DeviceIDUtils.getAndroidSystemId(id);
                    if (systemid != null) {
                        Result rs = conn.getTable(HBaseTableName.ANDROID_SYSTEMID_UUID).get(new Get(Bytes.toBytes(systemid)));
                        if (rs.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_V))
                            uuid = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_V));
                        break;
                    }
                    break;
                }
                case IDType.ID_MAC: {
                    Result rs = conn.getTable(HBaseTableName.MAC_UUID).get(new Get(Bytes.toBytes(id)));
                    if (rs.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_V))
                        uuid = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_V));
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return uuid;
    }

    private Result getResult(String id, int type) {
        Result rs = null;
        try {
            switch (type) {
                case IDType.ID_COOKIE: {
                    rs = conn.getTable(HBaseTableName.COOKIE_UUID).get(new Get(Bytes.toBytes(id)));
                    break;
                }
                case IDType.ID_IOS_IDFA: {
                    rs = conn.getTable(HBaseTableName.IOS_IDFA_UUID).get(new Get(Bytes.toBytes(id)));
                    break;
                }
                case IDType.ID_ANDROID_IMEI: {
                    rs = conn.getTable(HBaseTableName.ANDROID_IMEI_UUID).get(new Get(Bytes.toBytes(id)));
                    break;
                }
                case IDType.ID_ANDROID_SYSTEMID: {
                    rs = conn.getTable(HBaseTableName.ANDROID_SYSTEMID_UUID).get(new Get(Bytes.toBytes(id)));
                    break;
                }
                case IDType.ID_ANDROID_IMEI_SYSTEMID: {
                    String imei = DeviceIDUtils.getAndroidImei(id);
                    if (imei != null) {
                        rs = conn.getTable(HBaseTableName.ANDROID_IMEI_UUID).get(new Get(Bytes.toBytes(imei)));
                        break;
                    }
                    String systemid = DeviceIDUtils.getAndroidSystemId(id);
                    if (systemid != null) {
                        rs = conn.getTable(HBaseTableName.ANDROID_SYSTEMID_UUID).get(new Get(Bytes.toBytes(systemid)));
                        break;
                    }
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rs;
    }

    public Set<String> findUUIDByUserId(String userId, int accountId) {
        Set<String> set = new HashSet<>();

        try {
            Get get = new Get(Bytes.toBytes(ExHbase.getUserKey(userId, accountId)));
            Result rs = conn.getTable(HBaseTableName.USERID_UUID).get(get);
            KeyValue[] keyvalues = rs.raw();
            String uuid = "";
            for (KeyValue kv : keyvalues) {
                uuid = Bytes.toString(kv.getQualifier());
                if (uuid.length() == 16) {
                    set.add(uuid);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return set;
    }


    public Set<String> findUUIDByUserIds(Set<String> userIds) {
        Set<String> set = new HashSet<>();

        List<Get> search = new LinkedList<>();

        try {
            String[] strArray = null;
            for (String s : userIds) {
                strArray = s.split("\\|");
                search.add(new Get(Bytes.toBytes(ExHbase.getUserKey(strArray[0], Integer.parseInt(strArray[1])))));
            }

            Result[] rst = conn.getTable(HBaseTableName.USERID_UUID).get(search);
            for (Result rs : rst)  {
                KeyValue[] keyvalues = rs.raw();
                String uuid = "";
                for (KeyValue kv : keyvalues) {
                    uuid = Bytes.toString(kv.getQualifier());
                    if (uuid.length() == 16) {
                        set.add(uuid);
                    }
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

        return set;
    }

    public Set<String> findUUIDByIP(String ip) {
        Set<String> set = new HashSet<>();

        try {
            long iplong = JavaUtils.ip2Long(ip);
            if (iplong == 0)
                return set;
            String ipStr = JavaUtils.ip2HexString(iplong);
            Get get = new Get(Bytes.toBytes(ipStr));
            Result rs = conn.getTable(HBaseTableName.IP_UUID).get(get);
            if (rs.isEmpty())
                return set;
            KeyValue[] keyvalues = rs.raw();
            String uuid = "";
            for (KeyValue kv : keyvalues) {
                uuid = Bytes.toString(kv.getQualifier());
                if (uuid.length() == 16) {
                    set.add(uuid);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return set;
    }

    public Set<String> findUUIDByMac(String mac) {
        Set<String> set = new HashSet<>();
        return set;
    }

}
