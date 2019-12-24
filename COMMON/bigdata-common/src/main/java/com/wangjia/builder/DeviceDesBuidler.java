package com.wangjia.builder;

import com.alibaba.fastjson.JSONObject;
import com.wangjia.bean.DeviceDes;
import com.wangjia.hbase.HBaseConst;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * Created by Administrator on 2017/11/9.
 */
public class DeviceDesBuidler {

    private static final String SKEY_FIRST_TIME = "firstTime";
    private static final byte[] BKEY_FIRST_TIME = Bytes.toBytes(SKEY_FIRST_TIME);
    private static final String SKEY_LAST_TIME = "lastTime";
    private static final byte[] BKEY_LAST_TIME = Bytes.toBytes(SKEY_LAST_TIME);
    private static final String SKEY_DEVICEID = "deviceId";
    private static final byte[] BKEY_DEVICEID = Bytes.toBytes(SKEY_DEVICEID);
    private static final String SKEY_UUID = "uuid";
    private static final byte[] BKEY_UUID = Bytes.toBytes(SKEY_UUID);
    private static final String SKEY_DTYPE = "dType";
    private static final byte[] BKEY_DTYPE = Bytes.toBytes(SKEY_DTYPE);
    private static final String SKEY_NAME = "name";
    private static final byte[] BKEY_NAME = Bytes.toBytes(SKEY_NAME);
    private static final String SKEY_SYSTEM = "system";
    private static final byte[] BKEY_SYSTEM = Bytes.toBytes(SKEY_SYSTEM);
    private static final String SKEY_SW = "sw";
    private static final byte[] BKEY_SW = Bytes.toBytes(SKEY_SW);
    private static final String SKEY_SH = "sh";
    private static final byte[] BKEY_SH = Bytes.toBytes(SKEY_SH);
    private static final String SKEY_ACC = "acc";
    private static final byte[] BKEY_ACC = Bytes.toBytes(SKEY_ACC);
    private static final String SKEY_BCOOKIE = "bCookie";
    private static final byte[] BKEY_BCOOKIE = Bytes.toBytes(SKEY_BCOOKIE);
    private static final String SKEY_BFLASH = "bFlash";
    private static final byte[] BKEY_BFLASH = Bytes.toBytes(SKEY_BFLASH);

    public static DeviceDes build(String uuid) {
        DeviceDes des = new DeviceDes();
        des.setUuid(uuid);
        return des;
    }

    public static DeviceDes build(String uuid, Result rs) {
        DeviceDes des = new DeviceDes();
        if (rs.isEmpty()) {
            des.setUuid(uuid);
            return des;
        }

        Map<String, Long> ids = des.getIds();
        String sKey;
        byte[] bKey, bValue;
        KeyValue[] keyvalues = rs.raw();

        for (KeyValue kv : keyvalues) {
            bKey = kv.getQualifier();
            sKey = new String(bKey);
            bValue = kv.getValue();
            if (bKey[0] == '_') {
                ids.put(sKey, Bytes.toLong(bValue));
                continue;
            }
            switch (sKey) {
                case SKEY_FIRST_TIME:
                    des.setFirstTime(Bytes.toLong(bValue));
                    break;
                case SKEY_LAST_TIME:
                    des.setLastTime(Bytes.toLong(bValue));
                    break;
                case SKEY_DEVICEID:
                    des.setDeviceId(Bytes.toString(bValue));
                    break;
                case SKEY_UUID:
                    des.setUuid(Bytes.toString(bValue));
                    break;
                case SKEY_DTYPE:
                    des.setType(Bytes.toInt(bValue));
                    break;
                case SKEY_NAME:
                    des.setName(Bytes.toString(bValue));
                    break;
                case SKEY_SYSTEM:
                    des.setSystem(Bytes.toString(bValue));
                    break;
                case SKEY_SW:
                    des.setSw(Bytes.toInt(bValue));
                    break;
                case SKEY_SH:
                    des.setSh(Bytes.toInt(bValue));
                    break;
                case SKEY_ACC:
                    des.setAcc(Bytes.toInt(bValue));
                    break;
                case SKEY_BCOOKIE:
                    des.setbCookie(Bytes.toInt(bValue));
                    break;
                case SKEY_BFLASH:
                    des.setbFlash(Bytes.toInt(bValue));
                    break;
            }
        }

        return des;
    }

    public static Put deviceDes2Put(DeviceDes des) {
        Put put = new Put(Bytes.toBytes(des.getUuid()));
        put.addColumn(HBaseConst.BYTES_CF1, BKEY_FIRST_TIME, Bytes.toBytes(des.getFirstTime()));
        put.addColumn(HBaseConst.BYTES_CF1, BKEY_LAST_TIME, Bytes.toBytes(des.getLastTime()));
        put.addColumn(HBaseConst.BYTES_CF1, BKEY_DEVICEID, Bytes.toBytes(des.getDeviceId()));
        put.addColumn(HBaseConst.BYTES_CF1, BKEY_UUID, Bytes.toBytes(des.getUuid()));
        put.addColumn(HBaseConst.BYTES_CF1, BKEY_DTYPE, Bytes.toBytes(des.getType()));
        put.addColumn(HBaseConst.BYTES_CF1, BKEY_NAME, Bytes.toBytes(des.getName()));
        put.addColumn(HBaseConst.BYTES_CF1, BKEY_SYSTEM, Bytes.toBytes(des.getSystem()));
        put.addColumn(HBaseConst.BYTES_CF1, BKEY_SW, Bytes.toBytes(des.getSw()));
        put.addColumn(HBaseConst.BYTES_CF1, BKEY_SH, Bytes.toBytes(des.getSh()));
        put.addColumn(HBaseConst.BYTES_CF1, BKEY_ACC, Bytes.toBytes(des.getAcc()));
        put.addColumn(HBaseConst.BYTES_CF1, BKEY_BCOOKIE, Bytes.toBytes(des.getbCookie()));
        put.addColumn(HBaseConst.BYTES_CF1, BKEY_BFLASH, Bytes.toBytes(des.getbFlash()));

        for (Map.Entry<String, Long> item : des.getIds().entrySet()) {
            put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes(item.getKey()), Bytes.toBytes(item.getValue()));
        }

        return put;
    }

    public static String deviceDes2EsDoc(DeviceDes des) {
        Map<String, Object> map = new HashMap<>();
        map.put(SKEY_FIRST_TIME, des.getFirstTime());
        map.put(SKEY_LAST_TIME, des.getLastTime());
        map.put(SKEY_DEVICEID, des.getDeviceId());
        map.put(SKEY_DTYPE, des.getType());
        map.put(SKEY_ACC, des.getAcc());

        Map<String, Long> ids = des.getIds();
        String str[];
        List<String> phones = new LinkedList<>();
        List<String> userids = new LinkedList<>();
        List<String> appids = new LinkedList<>();

        for (Map.Entry<String, Long> item : ids.entrySet()) {
            str = item.getKey().split("#");
            if (str[0].equals("_did") && str.length >= 3) {
                map.put(str[1], str[2]);
            } else if (str[0].equals("_phone") && str.length >= 2) {
                phones.add(str[1]);
            } else if (str[0].equals("_userid") && str.length >= 3) {
                userids.add(str[1] + "#" + str[2]);
            } else if (str[0].equals("_appid") && str.length >= 2) {
                appids.add(str[1]);
            }
        }
        if (phones.size() > 0) {
            map.put("phones", phones);
        }
        if (userids.size() > 0) {
            map.put("userids", userids);
        }
        if (appids.size() > 0) {
            map.put("appids", appids);
        }

        JSONObject jsonObject = new JSONObject(map);
        return jsonObject.toJSONString();
    }
}
