package com.wangjia.handler.label;

import com.wangjia.hbase.conn.HbaseConn;
import com.wangjia.hbase.HBaseTableName;
import com.wangjia.hbase.conn.ExTBConnection;
import com.wangjia.hbase.conn.ExTable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class Uuid2LabelSumHandler implements HbaseConn{

    private ExTBConnection conn = null;
    private ExTable _tb = null;

    public Uuid2LabelSumHandler() {
        conn = new ExTBConnection();
        _tb = conn.getTable(HBaseTableName.UUID_LABEL_SUM);
    }

    public Uuid2LabelSumHandler(ExTBConnection conn) {
        this.conn = conn;
    }

    @Override
    public void close() {
        conn.close();
    }


    /**
     *
     * @param UUids
     * @return
     * @throws IOException
     */
    public List<String> findLabelSumByUUids(Set<String> UUids, int companyId) throws IOException {

        List<Get> listGet = new LinkedList<>();
        List<String> list = new LinkedList<>();

        for (String uuid : UUids) {
            listGet.add(new Get(Bytes.toBytes(uuid)));
        }

        Result[] results = _tb.getTable().get(listGet);
        for (Result rs: results) {

            List<Cell> cs = rs.listCells();

            if (cs == null) {
                continue;
            }

            for(Cell cell : cs){
                String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));
                //检查公司权限
                if (companyId > 0 && qualifier.indexOf(companyId) == -1) {
                    continue;
                }

                String value = Bytes.toString(CellUtil.cloneValue(cell));
                list.add(value);
            }
        }

        return list;
    }
}
