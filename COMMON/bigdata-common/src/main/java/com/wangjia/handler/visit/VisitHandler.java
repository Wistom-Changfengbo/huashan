package com.wangjia.handler.visit;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wangjia.bean.*;
import com.wangjia.builder.DeviceDesBuidler;
import com.wangjia.common.Config;
import com.wangjia.common.DeviceType;
import com.wangjia.hbase.ExHbase;
import com.wangjia.hbase.HBaseConst;
import com.wangjia.hbase.conn.HbaseConn;
import com.wangjia.hbase.conn.VisitTableConnection;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;

import java.io.IOException;
import java.util.*;

/**
 * Created by Administrator on 2017/4/27.
 */
public class VisitHandler implements HbaseConn {

    private VisitTableConnection conn = null;

    protected Table tbUuid2visit = null;
    protected Table tbUuid2visitdes = null;
    protected Table tbUuid2applist = null;
    protected Table tbUuid2devicemsg = null;

    public VisitHandler() {
        conn = new VisitTableConnection();

        tbUuid2visit = conn.getTbUuid2visit();
        tbUuid2visitdes = conn.getTbUuid2visitdes();
        tbUuid2applist = conn.getTbUuid2applist();
        tbUuid2devicemsg = conn.getTbUuid2devicemsg();
    }

    @Override
    public void close() {
        conn.close();
    }

    private void result2VisitBean(Result rs, List<Visit> objs) {
        if (rs.isEmpty())
            return;
        try {
            int platform = Bytes.toInt(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_PLATFORM));
            String appid = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_APPID));
            String deviceId = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_DEVICEID));
            String uuid = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_UUID));
            String ip = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_IP));
            String ref = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_REF));
            String address = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_ADDRESS));
            long start = Bytes.toLong(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_START));
            long time = Bytes.toLong(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_TIME));
            int num = Bytes.toInt(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_NUM));
            int eNum = Bytes.toInt(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_ENUM));
            String version = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_VERSION));
            String net = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_NET));
            String eventIds = "";
            if (rs.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_EVENTIDS)) {
                eventIds = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_EVENTIDS));
            }

            Visit visit = new Visit();
            visit.setMainKey(new String(rs.getRow()));
            visit.setPlatform(platform);
            visit.setAppid(appid);
            visit.setDeviceId(deviceId);
            visit.setUuid(uuid);
            visit.setIp(ip);
            visit.setRef(ref);
            visit.setAddress(address);
            visit.setStart(start);
            visit.setTime(time);
            visit.setNum(num);
            visit.seteNum(eNum);
            visit.setVersion(version);
            visit.setNet(net);
            visit.setEventIds(eventIds);
            objs.add(visit);
        } catch (Exception e) {
        }
    }

    public List<Visit> findVisit(List<String> uuids) {
        List<Visit> objs = new LinkedList<>();
        try {
            long maxT = System.currentTimeMillis();
            long minT = maxT - Config.SEARCH_TIME_RANGE;

            int maxNum = 2;
            if (uuids.size() < 10) {
                maxNum = 10;
            } else if (uuids.size() < 50) {
                maxNum = 3;
            } else if (uuids.size() < 100) {
                maxNum = 2;
            }

            maxNum = Integer.MAX_VALUE;
            minT = 0;


            int num = 0;
            for (String uuid : uuids) {
                String maxKey = ExHbase.getVisitKey("9999", uuid, minT);
                String minKey = ExHbase.getVisitKey("0000", uuid, maxT);

                Scan scan = new Scan();
                scan.setStartRow(Bytes.toBytes(minKey));
                scan.setStopRow(Bytes.toBytes(maxKey));
                //scan.setBatch(num);

                ResultScanner results = tbUuid2visit.getScanner(scan);
                Iterator<Result> it = results.iterator();

                Result rs;
                num = 0;
                while (num++ < maxNum && it.hasNext()) {
                    rs = it.next();
                    result2VisitBean(rs, objs);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return objs;
    }

    public List<Visit> findVisitByRowKey(List<String> rowkeys) {
        List<Visit> objs = new LinkedList<>();
        if (rowkeys == null || rowkeys.size() <= 0)
            return objs;
        List<Get> gets = new LinkedList<>();
        for (String rowkey : rowkeys)
            gets.add(new Get(Bytes.toBytes(rowkey)));

        try {
            Result[] results = tbUuid2visit.get(gets);
            Result rs;
            for (int i = 0; i < results.length; i++) {
                rs = results[i];
                result2VisitBean(rs, objs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return objs;

    }

    public String findVisitDes(String appid, String uuid, long time) {
        try {
            String key = ExHbase.getVisitDesKey(appid, uuid, time);
            Result rs = tbUuid2visitdes.get(new Get(Bytes.toBytes(key)));
            if (rs.isEmpty())
                return null;
            return Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, AppDesc> findAppList(List<String> uuids) {
        Map<String, String> maps = new HashMap<>();
        Map<String, AppDesc> stringAppDescMap = new HashMap<>();

        try {
            List<Get> getList = new LinkedList<>();
            for (String str : uuids) {
                getList.add(new Get(Bytes.toBytes(str)));
            }
            Result[] results = tbUuid2applist.get(getList);

            Result rs;

            for (int i = 0; i < results.length; i++) {
                rs = results[i];
                AppDesc appDesc = new AppDesc();
                List<AppDesc.Operation> operations = new LinkedList<>();
                List<Cell> cs = rs.listCells();

                if (cs == null) {
                    continue;
                }

                String rowKey = null;
                for (Cell cell : cs) {

                    if (cell == null) {
                        continue;
                    }

                    String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));  //取到修饰名
                    rowKey = Bytes.toString(CellUtil.cloneRow(cell));  //取行键
                    long timestamp = cell.getTimestamp();  //取到时间戳
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));  //取到族列
                    String value = Bytes.toString(CellUtil.cloneValue(cell));  //取到值

                    AppDesc.Operation operation = new AppDesc.Operation();

//                    System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " +
//                            timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);

                    //对app应用的操作

                    if (qualifier.startsWith("_ope_")) {

                        JSONObject jsonObject = JSON.parseObject(value);
                        Set<String> keys = jsonObject.keySet();
                        for (String key : keys) {
                            //操作
                            if (key == "opes") {
                                List<AppDesc.Op> ops = JSON.parseArray(jsonObject.get(key) + "", AppDesc.Op.class);
                                operation.setOps(ops);
                                operations.add(operation);
                            } else {
                                //时间
                                Long time = Long.parseLong(jsonObject.get(key) + "");
                                operation.setTime(time);
                            }

                        }
                    }
                    //applist
                    else if (qualifier.startsWith("msg")) {
                        String applistStr = Bytes.toString(CellUtil.cloneValue(cell));  //取到值
                        List<AppDesc.AppKv> app = new LinkedList<>();
                        JSONObject jsonObject = JSON.parseObject(applistStr);
                        Set<String> packageNames = jsonObject.keySet();
                        for (String packageName : packageNames) {
                            if (packageName != null) {
                                AppDesc.AppKv appkv = new AppDesc.AppKv();
                                String value1 = jsonObject.get(packageName) + "";
                                appkv.setAppName(value1);
                                appkv.setPackageName(packageName);
                                app.add(appkv);
                            }
                        }

                        appDesc.setApplist(app);
                    }
                    //创建时间
                    else if (qualifier.startsWith("firstTime")) {
                        Long createTime = Bytes.toLong(CellUtil.cloneValue(cell));  //取到值

                        appDesc.setFirstTime(createTime);
                    }
                    //最后处理时间
                    else if (qualifier.startsWith("lastTime")) {
                        Long lastChangeTime = Bytes.toLong(CellUtil.cloneValue(cell));  //取到值

                        appDesc.setLastTime(lastChangeTime);
                    }
                }
                Collections.sort(operations);
                appDesc.setOperations(operations);
                stringAppDescMap.put(rowKey, appDesc);



                if (rs.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG)) {
                    String appListStr = Bytes.toString(rs.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG));
                    maps.put(uuids.get(i), appListStr);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stringAppDescMap;
    }

    private DeviceDes result2DeviceMsg(Result rs, String uuid) {
        if (rs.isEmpty())
            return null;

        return DeviceDesBuidler.build(uuid, rs);
    }

    public Map<String, DeviceDes> findDeviceMsg(List<String> uuids) {
        try {
            List<Get> getList = new LinkedList<>();
            for (String str : uuids) {
                getList.add(new Get(Bytes.toBytes(str)));
            }
            Result[] results = tbUuid2devicemsg.get(getList);
            Result rs;
            DeviceDes msg = null;
            Map<String, DeviceDes> maps = new HashMap<>();
            for (int i = 0; i < results.length; i++) {
                rs = results[i];
                if (rs.isEmpty()) {
                    continue;
                }
                msg = result2DeviceMsg(rs, uuids.get(i));
                if (msg != null) {
                    maps.put(uuids.get(i), msg);
                }
            }
            return maps;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
