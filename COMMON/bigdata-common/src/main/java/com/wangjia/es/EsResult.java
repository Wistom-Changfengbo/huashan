package com.wangjia.es;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;

public class EsResult extends EsConnection {
    private SearchRequestBuilder searchRequestBuilder = null;

    private void initBuilder(String index, String type, String[] source) {
        searchRequestBuilder = client
                .prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setFetchSource(source, null);
    }


    public Set<String> getAllData(String index, String type, String[] source, Map<String, Object> datas) {
        initBuilder(index, type, source);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        for (Map.Entry<String, Object> entry : datas.entrySet())
            queryBuilder.must(toTermQuery(entry.getKey(), entry.getValue()));

        searchRequestBuilder.setQuery(queryBuilder);
        Set<String> listRowkey = new HashSet<>();
        SearchResponse searchResponse = searchRequestBuilder
                .addSort("time", SortOrder.DESC)
                .setSize(10000)
                .execute()
                .actionGet();

        for (SearchHit hit : searchResponse.getHits()) {
            Object ip = hit.getSource().get("ip");
            if (ip != null)
                listRowkey.add((String) ip);
        }
        return listRowkey;
    }

    public void getCount(String index, String type, String[] source, Map<String, Object> datas) {
        initBuilder(index, type, source);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        for (Map.Entry<String, Object> entry : datas.entrySet())
            queryBuilder.must(toTermQuery(entry.getKey(), entry.getValue()));
        searchRequestBuilder.setQuery(queryBuilder);

        AggregationBuilder aggregation =
                AggregationBuilders.terms("agg").field("ip.keyword")
                        .subAggregation(
                                AggregationBuilders.cardinality("stragg").field("ip.keyword")
                        );

        SearchResponse searchResponse = searchRequestBuilder.addSort("time", SortOrder.DESC).setSize(10000).addAggregation(
                aggregation
        ).execute().actionGet();


        Terms terms = searchResponse.getAggregations().get("agg");
        for (Terms.Bucket b : terms.getBuckets()) {
            System.out.println(b.getAggregations().get("stragg").toString());
//            System.out.println( "1 " + b.getKey());
//            System.out.println( "2 " + b.getDocCount());
        }
    }


    public long userAccessCount(String index, String type, String[] source, Map<String, Object> datas) {
        initBuilder(index, type, source);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        for (Map.Entry<String, Object> entry : datas.entrySet())
            queryBuilder.must(toTermQuery(entry.getKey(), entry.getValue()));

        searchRequestBuilder.setQuery(queryBuilder);
        SearchResponse response = searchRequestBuilder.get();
        return response.getHits().getTotalHits();//获得记录条数
    }
}
