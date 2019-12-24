package com.wangjia.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.omg.PortableInterceptor.INACTIVE;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2017/4/6.
 */
public final class HBaseUtils {
    private static Configuration conf = null;

    public static Configuration getConfiguration() {
        return conf;
    }

    public static final class HColumnDes {
        protected String name;
        protected Integer outTime;
        protected Integer maxVersions;
        protected Integer minVersions;

        public HColumnDes(String name) {
            this.name = name;
        }

        public HColumnDes(String name, Integer outTime) {
            this.name = name;
            this.outTime = outTime;
        }

        public HColumnDes(String name, Integer outTime, Integer maxVersions) {
            this.name = name;
            this.outTime = outTime;
            this.maxVersions = maxVersions;
        }

        public HColumnDes(String name, Integer outTime, Integer maxVersions, Integer minVersions) {
            this.name = name;
            this.outTime = outTime;
            this.maxVersions = maxVersions;
            this.minVersions = minVersions;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getOutTime() {
            return outTime;
        }

        public void setOutTime(Integer outTime) {
            this.outTime = outTime;
        }

        public Integer getMaxVersions() {
            return maxVersions;
        }

        public void setMaxVersions(Integer maxVersions) {
            this.maxVersions = maxVersions;
        }

        public Integer getMinVersions() {
            return minVersions;
        }

        public void setMinVersions(Integer minVersions) {
            this.minVersions = minVersions;
        }
    }

    public static void setConfiguration(Configuration conf) {
        HBaseUtils.conf = conf;
    }

    public static Connection getConn() {
        try {
            return ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static Connection getPConn(int nThreads) {
        try {
            ExecutorService executor = Executors.newFixedThreadPool(nThreads);
            return ConnectionFactory.createConnection(conf, executor);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static Connection getPConn() {
        return getPConn(10);
    }

    public static void closeConn(Connection conn) {
        if (conn == null)
            return;
        try {
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void closeTable(Table tb) {
        if (tb == null)
            return;
        try {
            tb.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void closeConn(Connection conn, Table tb, Table... tbs) {
        closeTable(tb);
        for (Table t : tbs) {
            closeTable(t);
        }
        closeConn(conn);
    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     */
    public static boolean isExist(String tableName) {
        Connection conn = null;
        try {
            conn = getConn();
            Admin hAdmin = conn.getAdmin();
            return hAdmin.tableExists(TableName.valueOf(tableName));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            closeConn(conn);
        }
    }

    //创建数据库表
    public static void createTable(String tableName, Collection<HColumnDes> hcs) {
        Connection conn = null;
        try {
            conn = getConn();
            // 新建一个数据库管理员
            Admin hAdmin = conn.getAdmin();
            if (!hAdmin.tableExists(TableName.valueOf(tableName))) {
                // 新建一个students表的描述
                HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
                HColumnDescriptor cfDes;
                for (HColumnDes hc : hcs) {
                    cfDes = new HColumnDescriptor(hc.name);
                    if (hc.outTime != null)
                        cfDes.setTimeToLive(hc.outTime);
                    if (hc.maxVersions != null)
                        cfDes.setMaxVersions(hc.maxVersions);
                    if (hc.minVersions != null)
                        cfDes.setMinVersions(hc.minVersions);
                    tableDesc.addFamily(cfDes);
                }
                // 根据配置好的描述建表
                hAdmin.createTable(tableDesc);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            closeConn(conn);
        }
    }

    public static void createTable(String tableName, HColumnDes des) {
        ArrayList<HColumnDes> hcs = new ArrayList<>();
        hcs.add(des);
        createTable(tableName, hcs);
    }

    public static void createTable(String tableName) {
        createTable(tableName, new HColumnDes("cf1"));
    }

    public static void createTable(String tableName, int removeTime) {
        createTable(tableName, new HColumnDes("cf1", removeTime));
    }

    public static void createTable(String tableName, int removeTime, int maxVersions) {
        createTable(tableName, new HColumnDes("cf1", removeTime, maxVersions));
    }

    //得到表
    public static Table getTable(Connection conn, String tableName) {
        try {
            return conn.getTable(TableName.valueOf(tableName));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    //删除数据库表
    public static void deleteTable(String tableName) {
        Connection conn = null;
        try {
            conn = getConn();
            Admin hAdmin = conn.getAdmin();
            if (hAdmin.tableExists(TableName.valueOf(tableName))) {
                hAdmin.disableTable(TableName.valueOf(tableName));
                hAdmin.deleteTable(TableName.valueOf(tableName));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            closeConn(conn);
        }
    }
}