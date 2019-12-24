package com.wangjia.handler.userprofile;

import com.wangjia.hbase.HBaseTableName;
import com.wangjia.bean.V2UserProfileRet;
import com.wangjia.utils.HBaseUtils;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class UserProfileHandler {

    private Connection conn = null;

    public UserProfileHandler() {
        conn = HBaseUtils.getPConn();
    }

    public List<V2UserProfileRet> getUserProfileByUUid(Iterable<String> UUids) throws IOException {
        List<Get> listGet = new LinkedList<>();
        Table tbUuid2userProfile = null;

        //待返回的数据
        List<V2UserProfileRet> userProfileRetLinkedList = new LinkedList<>();

        try {
            tbUuid2userProfile = HBaseUtils.getTable(conn, HBaseTableName.UUID_LABEL_APPLIST);
            for (String uuid : UUids) {
                listGet.add(new Get(Bytes.toBytes(uuid)));
            }
            Result[] results = tbUuid2userProfile.get(listGet);

            for (Result result : results) {
                if (result.isEmpty()) {
                    continue;
                }

                userProfileRetLinkedList.add(V2UserProfileRet.build(result));
            }
        } finally {
            if (tbUuid2userProfile != null) {
                tbUuid2userProfile.close();
            }
        }

        return userProfileRetLinkedList;
    }
}
