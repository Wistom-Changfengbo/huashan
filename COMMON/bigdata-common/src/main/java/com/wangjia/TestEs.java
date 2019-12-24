package com.wangjia;

import com.wangjia.es.EsConnection;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017/8/28.
 */
public class TestEs {
    public static void main1(String[] args) throws IOException {
        EsConnection conn = new EsConnection();

        User user = new User();
        user.setId(1);
        user.setName("zl22");
        user.setPostDate(new Date());
        user.setMessage("vnvnvnvnvnvnvnvn");

        DocWriteResponse.Result add = conn.update("index", "user", "1", user);

//        GetResponse getFields = conn.get("index", "user", "1");
//        System.out.println(getFields.getField("message"));
//        System.out.println(getFields.getFields());
//        System.out.println(getFields.getSource());

//        List<Map> query = conn.query("index", "user");
//        System.out.println(query);
//
//        GetResponse getFields = conn.get("index", "user", "1");
//        System.out.println(getFields.getSource());
    }

//    public static void main(String[] args) {
//        EsConnection conn = new EsConnection();
//        SearchResponse searchResponse = conn.getClient()
//                .prepareSearch("bigdata")
//                .setTypes("visit")
//
//                .setSearchType(SearchType.QUERY_THEN_FETCH)
//                .setQuery(QueryBuilders.matchQuery("ref", "百度"))
//                .setFetchSource(new String[]{"uuid"}, null)
//                .setSize(3000)
//                .execute()
//                .actionGet();
//        for (SearchHit hit : searchResponse.getHits()) {
//            Map map = hit.getSource();
//            System.out.println(map);
//            System.out.println(hit.getId());
//        }
//    }
}
