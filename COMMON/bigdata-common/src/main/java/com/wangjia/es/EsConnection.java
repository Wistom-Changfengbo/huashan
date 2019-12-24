package com.wangjia.es;

import com.alibaba.fastjson.JSON;
import com.wangjia.utils.EsUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by Administrator on 2017/8/28.
 */
public class EsConnection {
    private static final TransportClient _def_client = EsUtils.getEsClient();

    protected TransportClient client;

    public static final class EsBean {
        protected int handleType;
        protected String index;
        protected String type;
        protected String id;
        protected Object data;

        public EsBean(String index, String type, String id, Object data) {
            this.index = index;
            this.type = type;
            this.id = id;
            this.data = data;
        }

        public EsBean(String index, String type, String id) {
            this.index = index;
            this.type = type;
            this.id = id;
            this.data = null;
        }

        public EsBean() {
            this.index = null;
            this.type = null;
            this.id = null;
            this.data = null;
        }

        public int getHandleType() {
            return handleType;
        }

        public void setHandleType(int handleType) {
            this.handleType = handleType;
        }

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Object getData() {
            return data;
        }

        public void setData(Object data) {
            this.data = data;
        }
    }

    public static String jsonStr(Object obj) {
        if (obj instanceof String)
            return (String) obj;
        return JSON.toJSONString(obj);
    }

    public EsConnection() {
        this.client = _def_client;
    }

    public EsConnection(TransportClient client) {
        this.client = client;
    }

    public DocWriteResponse.Result add(EsBean bean) {
        return add(bean.index, bean.type, bean.id, bean.data);
    }

    public DocWriteResponse.Result add(String index, String type, String id, Object data) {
        try {
            String str = jsonStr(data);
            IndexResponse response = client.prepareIndex(index, type, id)
                    .setSource(str, XContentType.JSON)
                    .get();
            return response.getResult();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public DocWriteResponse.Result delete(EsBean bean) {
        return delete(bean.index, bean.type, bean.id);
    }

    public DocWriteResponse.Result delete(String index, String type, String id) {
        DeleteResponse response = client.prepareDelete(index, type, id)
                .get();
        return response.getResult();
    }

    public UpdateResponse.Result update(EsBean bean) {
        return update(bean.index, bean.type, bean.id, bean.data);
    }

    public UpdateResponse.Result update(String index, String type, String id, Object data) {
        try {
            String str = jsonStr(data);
            UpdateRequest updateRequest = new UpdateRequest(index, type, id)
                    .doc(str, XContentType.JSON);
            UpdateResponse updateResponse = client.update(updateRequest)
                    .get();
            return updateResponse.getResult();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public GetResponse get(EsBean bean) {
        return get(bean.index, bean.type, bean.id);
    }

    public GetResponse get(String index, String type, String id) {
        GetResponse response = client.prepareGet(index, type, id)
                .setOperationThreaded(false)
                .get();
        return response;
    }

    public Map<String, Map<String, Object>> query(String index, String type, String[] source, Map<String, Object> datas, int size) {
        Map<String, Map<String, Object>> msgs = new HashMap<>();
        SearchRequestBuilder searchRequestBuilder = client
                .prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setFetchSource(source, null);

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        for (Map.Entry<String, Object> entry : datas.entrySet())
            queryBuilder.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
        searchRequestBuilder.setQuery(queryBuilder);

        SearchResponse searchResponse = searchRequestBuilder
                .setSize(size)
                .execute()
                .actionGet();

        for (SearchHit hit : searchResponse.getHits()) {
            msgs.put(hit.getId(), hit.getSource());
        }
        return msgs;
    }

    public void close() {
        if (client != null && client != _def_client) {
            client.close();
            client = null;
        }
    }

    public void setClient(TransportClient client) {
        this.client = client;
    }

    protected QueryBuilder toTermQuery(String fieldName, Object obj) {
//        if (obj instanceof List) {
//            List<String> list = (List<String>) obj;
//            StringBuilder sb = new StringBuilder();
//            for (int i = 0; i < list.size(); i++) {
//                sb.append(list.get(i));
//                if (i < list.size() - 1)
//                    sb.append(" OR ");
//            }
//            return QueryBuilders.simpleQueryStringQuery(sb.toString()).field(fieldName);
//        }
//        return QueryBuilders.matchPhraseQuery(fieldName, obj);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (obj instanceof List) {
            List<String> list = (List<String>) obj;
            for (int i = 0; i < list.size(); i++) {
                boolQueryBuilder.should(QueryBuilders.matchPhraseQuery(fieldName, list.get(i)));
            }
        } else {
            boolQueryBuilder.should(QueryBuilders.matchPhraseQuery(fieldName, obj));
        }
        return boolQueryBuilder;
    }

    public static void main(String[] args) {
        SearchRequestBuilder searchRequestBuilder = _def_client
                .prepareSearch("v9bigdata-device")
                .setTypes("v9device")
                .setSearchType(SearchType.QUERY_THEN_FETCH);
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();

        BoolQueryBuilder should1= QueryBuilders.boolQuery().must(QueryBuilders.termQuery("phones", "13269160050"))
                   .must(QueryBuilders.rangeQuery("dType").gt(1));

        BoolQueryBuilder should2 = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("userids", "1#6015487"))
                .must(QueryBuilders.rangeQuery("dType").gt(1));

//        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("deviceId", "862096031466456#fea32d32928d769a");
        searchRequestBuilder.setQuery(should1);
        String[] strings =null;
//                {"deviceId","dType"};
        SearchResponse searchResponse = searchRequestBuilder
                .setFetchSource(strings, null)
                .setSize(10)
                .execute()
                .actionGet();

        for (SearchHit hit : searchResponse.getHits()) {
            System.out.println(hit.getSource());
        }

    }

}
