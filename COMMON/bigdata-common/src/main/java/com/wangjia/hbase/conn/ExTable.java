package com.wangjia.hbase.conn;

import com.wangjia.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Administrator on 2017/5/19.
 */
public class ExTable {
    private Table tb;

    private List<Put> puts = new LinkedList<>();
    private List<Delete> dels = new LinkedList<>();

    public ExTable(Table tb) {
        this.tb = tb;
    }

    public void addPut(Put put) {
        puts.add(put);
        if (puts.size() >= 1000) {
            this.put();
        }
    }

    public void addPut(List<Put> puts) {
        this.puts.addAll(puts);
        if (puts.size() >= 1000) {
            this.put();
        }
    }

    public void put() {
        try {
            if (puts.size() > 0)
                tb.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
        puts.clear();
    }

    public void put(Put put) {
        puts.add(put);
        this.put();
    }

    public void addDelete(Delete del) {
        dels.add(del);
        if (dels.size() >= 1000) {
            this.delete();
        }
    }

    public void addDelete(List<Delete> dels) {
        this.dels.addAll(dels);
        if (dels.size() >= 1000) {
            this.delete();
        }
    }

    public void delete() {
        try {
            if (dels.size() > 0)
                tb.delete(dels);
        } catch (IOException e) {
            e.printStackTrace();
        }
        dels.clear();
    }

    public void delete(Delete del) {
        dels.add(del);
        this.delete();
    }

    public void clear() {
        if (puts.size() > 0) {
            put();
        }
        if (dels.size() > 0) {
            delete();
        }
    }

    public void flush(){
        clear();
    }

    public void close() {
        clear();
        HBaseUtils.closeTable(tb);
        tb = null;
    }

    public Table getTable() {
        return tb;
    }

}
