package com.wangjia.es;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wangjia.bean.EsLabelBean;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;

/**
 * Created by Cfb on 2017/9/7.
 */
public abstract class EsPageNestedQuery<T> extends EsConnection {
    private String nestField = "";
    private String nestSort = "";
    private static final String COMMA = ".";

    public static final class NestRequest {
        private String index;
        private String type;
        private String id;

        private String path;
        private String searchField;
        private String sortField;
        private int pageIndex;
        private int size;
        private String[] source;
        private Object data;


        public String getIndex() {
            return index;
        }

        public NestRequest setIndex(String index) {
            this.index = index;
            return this;
        }

        public String getType() {
            return type;
        }

        public NestRequest setType(String type) {
            this.type = type;
            return this;
        }

        public String getId() {
            return id;
        }

        public NestRequest setId(String id) {
            this.id = id;
            return this;
        }

        public String getPath() {
            return path;
        }

        public NestRequest setPath(String path) {
            this.path = path;
            return this;
        }

        public String getSearchField() {
            return searchField;
        }

        public NestRequest setSearchField(String searchField) {
            this.searchField = searchField;
            return this;
        }

        public String getSortField() {
            return sortField;
        }

        public NestRequest setSortField(String sortField) {
            this.sortField = sortField;
            return this;
        }

        public int getPageIndex() {
            return pageIndex;
        }

        public NestRequest setPageIndex(int pageIndex) {
            this.pageIndex = pageIndex;
            return this;
        }

        public int getSize() {
            return size;
        }

        public NestRequest setSize(int size) {
            this.size = size;
            return this;
        }

        public String[] getSource() {
            return source;
        }

        public NestRequest setSource(String[] source) {
            this.source = source;
            return this;
        }

        public Object getData() {
            return data;
        }

        public NestRequest setData(Object data) {
            this.data = data;
            return this;
        }
    }

    protected SearchRequestBuilder searchRequestBuilder = null;


    protected int judgeSearchFrom(int pageIndex, int size) {
        int from = 0;
        //得出起始位置
        int temp = (pageIndex - 1) * size;
        from = temp < 0 ? 0 : temp;
        return from;
    }

    //如果不添加FetchSource 就默认把所有的_source都拿过来。如果添加了，就拿出source里东西
    protected void initBuilderNest(String index, String type, String[] source, String path, String field, String sort) {
        nestField = transToNestField(path, field);
        nestSort = transToNestField(path, sort);
        searchRequestBuilder = client
                .prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        if (source != null) {
            searchRequestBuilder.setFetchSource(source, null);
        }
    }

    public EsPageBean getPageDataNest(EsPageNestedQuery.NestRequest request) {
        return this.getPageDataNest(request.index, request.type, request.pageIndex, request.size, request.source, request.path, request.searchField, request.sortField, request.data);
    }

    public EsPageBean getPageDataNest(String index, String type, int pageIndex, int size, String[] source, String path, String searchField, String sortField, Object data) {
        initBuilderNest(index, type, source, path, searchField, sortField);
        if (data instanceof String) {
            String matchValue = String.valueOf(data);
            BoolQueryBuilder must = QueryBuilders.boolQuery().must(QueryBuilders.matchPhraseQuery(nestField, matchValue));
            FieldSortBuilder fieldSortBuilder =
                    SortBuilders.fieldSort(nestSort)
                            .sortMode(SortMode.MAX)
                            .setNestedPath(path).setNestedFilter(must).order(SortOrder.DESC);
            QueryBuilder queryBuilder =
                    QueryBuilders.nestedQuery(path, must, ScoreMode.None);
            searchRequestBuilder.setQuery(queryBuilder)
                    .addSort(fieldSortBuilder);
            return result(pageIndex, judgeSearchFrom(pageIndex, size), size, path, matchValue, searchField, sortField);
        } else {
            return null;
        }
    }

    private EsPageBean result(int page, int from, int size, String path, String matchValue, String field, String sortField) {
        EsPageBean bean = new EsPageBean<>();
        List listRowkey = new LinkedList<>();
        SearchResponse searchResponse = searchRequestBuilder
                .setFrom(from)
                .setSize(size)
                .execute()
                .actionGet();
        for (SearchHit hit : searchResponse.getHits()) {
            String id = hit.getId();
            String sourceAsString = hit.getSourceAsString();
            JsonParser jsonParser = new JsonParser();
            JsonArray label = jsonParser.parse(sourceAsString).getAsJsonObject().get(path).getAsJsonArray();
            HashMap<String, Integer> labelMap = new HashMap<>();
            int labelSize = label.size();
            int i = 0;
            while (i < labelSize) {
                JsonObject tagObject = label.get(i++).getAsJsonObject();
                String tag = tagObject.get(field).getAsString();
                int weight = tagObject.get(sortField).getAsInt();
//                if (tag.equals(matchValue)) {
//                    int weight = tagObject.get(sortField).getAsInt();
//                    labelMap.put(tag, weight);
//                }
                labelMap.put(tag, weight);
            }
            listRowkey.add(new EsLabelBean(id, labelMap));
        }
        long totalHits = searchResponse.getHits().getTotalHits();
        long totalPage = (long) (Math.ceil((double) totalHits / (double) size));
        bean.setTotal(totalHits);
        bean.setTotalPage(totalPage);
        bean.setSize(size);
        bean.setDatas(listRowkey);
        bean.setNum(listRowkey.size());
        return bean;
    }

    private String transToNestField(String path, String value) {
        return path.concat(COMMA).concat(value);
    }

    protected abstract T builderObject(SearchHit hit);

}
