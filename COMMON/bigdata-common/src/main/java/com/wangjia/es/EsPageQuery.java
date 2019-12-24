package com.wangjia.es;

import com.wangjia.bean.search.UserRequestParam;
import com.wangjia.utils.JavaUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by Cfb on 2017/9/7.
 */
public abstract class EsPageQuery<T> extends EsConnection {
    public static final class Request {
        protected String index;
        protected String type;
        protected String id;

        protected int pageIndex;
        protected int size;
        protected String[] source;

        protected Map<String, Object> datas;

        public String getIndex() {
            return index;
        }

        public Request setIndex(String index) {
            this.index = index;
            return this;
        }

        public String getType() {
            return type;
        }

        public Request setType(String type) {
            this.type = type;
            return this;
        }

        public String getId() {
            return id;
        }

        public Request setId(String id) {
            this.id = id;
            return this;
        }

        public int getPageIndex() {
            return pageIndex;
        }

        public Request setPageIndex(int pageIndex) {
            this.pageIndex = pageIndex;
            return this;
        }

        public int getSize() {
            return size;
        }

        public Request setSize(int size) {
            this.size = size;
            return this;
        }

        public String[] getSource() {
            return source;
        }

        public Request setSource(String[] source) {
            this.source = source;
            return this;
        }

        public Map<String, Object> getDatas() {
            return datas;
        }

        public Request setDatas(Map<String, Object> datas) {
            this.datas = datas;
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
    protected void initBuilder(String index, String type, String[] source) {
        searchRequestBuilder = client
                .prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        if (source != null) {
            searchRequestBuilder.setFetchSource(source, null);
        }
    }

    /**
     * @param index     库名
     * @param type      表名
     * @param pageIndex 第几页
     * @param size      页面大小
     * @param source    返回数据源Key
     * @param name      查询Key
     * @param data      查询Value
     * @return
     */
    public EsPageBean<T> getPageData(String index, String type, int pageIndex, int size, String[] source, String name, Object data) {
        initBuilder(index, type, source);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(toTermQuery(name, data));
        searchRequestBuilder.setQuery(queryBuilder);
        return result(pageIndex, judgeSearchFrom(pageIndex, size), size);
    }

    public EsPageBean<T> getRangePageData(String index, String type, int pageIndex, int size, String[] source, Map<String, Object> datas, SortOrder sort) {
        initBuilder(index, type, source);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        for (Map.Entry<String, Object> entry : datas.entrySet()) {
            queryBuilder.must(toTermQuery(entry.getKey(), entry.getValue()));
        }

        searchRequestBuilder.setQuery(queryBuilder);

        return result(pageIndex, judgeSearchFrom(pageIndex, size), size, sort);
    }


    /**
     * // TODO: 2018/4/25  用于用户搜索自定义查询条件
     *
     * @param index
     * @param type
     * @param pageIndex
     * @param size
     * @param source
     * @param datas
     * @return
     */
    public EsPageBean<T> getPageDataByCustomSearch(String index, String type, int pageIndex, int size, String[] source, Map<String, Object> datas) {
        initBuilder(index, type, source);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        for (Map.Entry<String, Object> entry : datas.entrySet()) {
            if (entry.getKey().equals("uuid")) {
//                searchRequestBuilder.setQuery(QueryBuilders.idsQuery("_id").addIds((String) entry.getValue()));
                queryBuilder.must(termQuery("_id", entry.getValue()));
                searchRequestBuilder.setQuery(queryBuilder);
            } else {
                queryBuilder.must(termQuery(entry.getKey(), entry.getValue()));
                searchRequestBuilder.setQuery(queryBuilder);
            }
        }

//        searchRequestBuilder.setQuery(QueryBuilders.rangeQuery("sex").gt(0));
        Map<String, SortOrder> order = new HashMap<>();
//        order.put("consume", SortOrder.DESC);

        return resultCustomSearch(pageIndex, judgeSearchFrom(pageIndex, size), size, order);
    }


    public EsPageBean<T> getPageDataByRequestBean(String index, String type, String[] source, UserRequestParam search) {
        initBuilder(index, type, source);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());

        boolQueryBuilder.must(QueryBuilders.existsQuery("apptagkey"));
        boolQueryBuilder.must(QueryBuilders.existsQuery("usertagkey"));
        boolQueryBuilder.must(QueryBuilders.existsQuery("appnamelist"));
        boolQueryBuilder.must(QueryBuilders.existsQuery("userlabelkey"));

        //首次时间
        if (search.getMinFirsttime() > 0 && search.getMaxFirsttime() > 0) {
            boolQueryBuilder.must(QueryBuilders.rangeQuery("firsttime")
                    .gte(search.getMinFirsttime())
                    .lte(search.getMaxFirsttime())
            );
        }

        //App标签
        if ((search.getAppTag() != null) && (search.getAppTag().length() > 0)) {
            boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("apptagkey", search.getAppTag()));
        }

        //liveness
        if ((search.getMinLiveness() > 0) && (search.getMaxLiveness() > 0)) {
            boolQueryBuilder.must(QueryBuilders.rangeQuery("liveness")
                    .gte(search.getMinLiveness())
                    .lte(search.getMaxLiveness())
            );
        }

        //bclue
        if (search.getBclue() != -10) {
            if (search.getBclue() == 1) {
                boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("bclue", true));
            } else if (search.getBclue() == -1) {
                boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("bclue", false));
            }
        }

        //pagenum
        if ((search.getMinPageNum() > 0) && (search.getMaxPageNum() > 0)) {
            boolQueryBuilder.must(QueryBuilders.rangeQuery("pagenum")
                    .gte(search.getMinPageNum())
                    .lte(search.getMaxLiveness())
            );
        }

        //平台
        if ((search.getPlatform() != null) && (search.getPlatform().length() > 0)) {
            boolQueryBuilder.must(QueryBuilders.termQuery("platform", search.getPlatform()));
        }

        //appId
        if (search.getAppId() > 0) {
            boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("appids", search.getAppId()));
        }

        //消费水平 分段
        if (search.getMinConsume() >= 0 && search.getMaxConsume() > 0) {
            boolQueryBuilder.must(QueryBuilders.rangeQuery("consumenum")
                    .gte(search.getMinConsume())
                    .lte(search.getMaxConsume())
            );
        }


        //活跃天数
        if (search.getMinActiveDayNum() > 0 && search.getMaxActiveDayNum() > 0) {
            boolQueryBuilder.must(QueryBuilders.rangeQuery("activedaynum")
                    .gte(search.getMinActiveDayNum())
                    .lte(search.getMaxActiveDayNum())
            );
        }


        //包名
        if ((search.getAppPkgList() != null) && (search.getAppPkgList().length() > 0)) {
            boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("apppkglist", search.getAppPkgList()));
        }


        //地址
        if ((search.getAddress()) != null && (search.getAddress().length() > 0)) {
            boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("address", search.getAddress()));
        }


        //sex
        if (search.getSex() != 0) {
            if (search.getSex() > 0) {
                boolQueryBuilder.must(QueryBuilders.rangeQuery("sex").gt(0));
            }

            if (search.getSex() < 0) {
                boolQueryBuilder.must(QueryBuilders.rangeQuery("sex").lt(0));
            }
        }


        //活跃时长  staytimesum
        if (search.getMinStayTimeSum() > 0 && search.getMaxStayTimeSum() > 0) {
            boolQueryBuilder.must(QueryBuilders.rangeQuery("staytimesum")
                    .gte(search.getMinStayTimeSum())
                    .lte(search.getMaxStayTimeSum())
            );
        }

        //设备
        if ((search.getDeviceid() != null) && (search.getDeviceid().length() > 0)) {
            boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("deviceid", search.getDeviceid()));
        }


        //最近7天
//        if (search.getMinr7ActiveDayNum() > 0 && search.getMaxr7ActiveDayNum() > 0) {
//            boolQueryBuilder.must(QueryBuilders.rangeQuery("staytimesum")
//                    .gte(search.getMinr7ActiveDayNum())
//                    .lte(search.getMaxr7ActiveDayNum())
//            );
//        }

        //ip
        if ((search.getIp() != null) && (search.getIp().length() > 0)) {
            boolQueryBuilder.must(QueryBuilders.termQuery("ips", search.getIp()));
        }


        //浏览标签
        if ((search.getUserLabelKey() != null) && (search.getUserLabelKey().length() > 0)) {
            boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("userlabelkey", search.getUserLabelKey()));
        }


        //最后时间
        if (search.getMinLastTime() > 0 && search.getMaxLastTime() > 0) {
            boolQueryBuilder.must(QueryBuilders.rangeQuery("lasttime")
                    .gte(search.getMinLastTime())
                    .lte(search.getMaxLastTime())
            );
        }

        //appnamelist
        if ((search.getAppNameList() != null) && (search.getAppNameList().length() > 0)) {
            boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("appnamelist", search.getAppNameList()));
        }

        //用户标签
        if ((search.getUserTag() != null) && (search.getUserTag().length() > 0)) {
            boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("usertagkey", search.getUserTag()));
        }


        //平台
        if ((search.getPlatform() != null) && (search.getPlatform().length() > 0)) {
            boolQueryBuilder.must(QueryBuilders.termQuery("platform", search.getPlatform()));
        }


        //事件ID
        if ((search.getEventIdKey() != null) && (search.getEventIdKey().length() > 0)) {
            boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("eventidkey", search.getEventIdKey()));
        }

        //事件值
        if (search.getEventIdValue() > 0) {
            boolQueryBuilder.must(QueryBuilders.termQuery("eventidvalue", search.getEventIdValue()));
        }

        //设备名
        if ((search.getDeviceName() != null) && (search.getDeviceName().length() > 0)) {
            boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("devicename", search.getDeviceName()));
        }

        searchRequestBuilder.setQuery(boolQueryBuilder);

        //排序
        Map<String, SortOrder> order = new HashMap<>();
        SortOrder sort = SortOrder.DESC;

        if (search.getSort() > 0) {
            sort = SortOrder.ASC;
        }

        if (search.getKey() != null && search.getKey().length() > 0) {
            order.put(search.getKey(), sort);
        }

        return resultCustomSearch(search.getPage(), judgeSearchFrom(search.getPage(), search.getPageSize()), search.getPageSize(), order);
    }


    /**
     * 自定义页面
     *
     * @param page
     * @param from
     * @param size
     * @param order
     * @return
     */
    private EsPageBean<T> resultCustomSearch(int page, int from, int size, Map<String, SortOrder> order) {
        EsPageBean<T> bean = new EsPageBean<>();
        List<T> sources = new LinkedList<>();
        Iterator<String> iter = order.keySet().iterator();

        while (iter.hasNext()) {
            String key = iter.next();
            SortOrder value = order.get(key);

            searchRequestBuilder.addSort(key, value);
        }

        SearchResponse searchResponse = searchRequestBuilder
                .setFrom(from)
                .setSize(size)
                .execute()
                .actionGet();

        for (SearchHit hit : searchResponse.getHits()) {
            sources.add(builderObject(hit));
        }

        long totalHits = searchResponse.getHits().getTotalHits();
        long totalPage = (long) (Math.ceil((double) totalHits / (double) size));
        bean.setTotal(totalHits);
        bean.setTotalPage(totalPage);
        bean.setSize(size);
        bean.setDatas(sources);
        return bean;
    }


    public EsPageBean<T> getPageData(String index, String type, int pageIndex, int size, String[] source, Map<String, Object> datas) {
        initBuilder(index, type, source);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        for (Map.Entry<String, Object> entry : datas.entrySet()) {
            queryBuilder.must(toTermQuery(entry.getKey(), entry.getValue()));
        }
        System.out.println("最终的map===>");
        datas.forEach((k, v) -> {
            System.out.println("key==" + k + ",value=" + v);
        });
        System.out.println("===>最终的map");
        searchRequestBuilder.setQuery(queryBuilder);
        System.out.println(searchRequestBuilder);

        return result(pageIndex, judgeSearchFrom(pageIndex, size), size);
    }


    public EsPageBean<T> getPageData(Request request) {
        return getPageData(request.index, request.type, request.pageIndex, request.size, request.source, request.datas);
    }

    private EsPageBean<T> result(int page, int from, int size) {
        EsPageBean<T> bean = new EsPageBean<>();
        List<T> listRowkey = new LinkedList<>();
        SearchResponse searchResponse = searchRequestBuilder
                .setFrom(from)
                .addSort("time", SortOrder.DESC)
                .setSize(size)
                .execute()
                .actionGet();
        for (SearchHit hit : searchResponse.getHits()) {
            listRowkey.add(builderObject(hit));
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


    private EsPageBean<T> result(int page, int from, int size, SortOrder sort) {
        EsPageBean<T> bean = new EsPageBean<>();
        List<T> listRowkey = new LinkedList<>();
        SearchResponse searchResponse = searchRequestBuilder
                .setFrom(from)
                .addSort("time", sort)
                .setSize(size)
                .execute()
                .actionGet();
        for (SearchHit hit : searchResponse.getHits()) {
            listRowkey.add(builderObject(hit));
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

    //精准过滤  匹配，无得分赋值。
    public List<T> getPageDataFilter(String index, String type, String[] source, Map<String, Object> datas) {
        long l = System.currentTimeMillis();
//        System.out.println(o-l);
        List<T> listRowkey = new LinkedList<>();
        initBuilder(index, type, source);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        for (Map.Entry<String, Object> entry : datas.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof List) {
                List<String> list = (List<String>) value;
                for (int i = 0; i < list.size(); i++) {
                    queryBuilder.filter(QueryBuilders.termsQuery(entry.getKey(), list.get(i)));
                }
            } else {
                queryBuilder.filter(QueryBuilders.termsQuery(entry.getKey(), value));
            }
        }
        QueryBuilder qb = QueryBuilders.constantScoreQuery(queryBuilder);
        SearchResponse searchResponse = searchRequestBuilder.setQuery(qb)
                .setSize(10000)
                .execute()
                .actionGet();
        for (SearchHit hit : searchResponse.getHits()) {
//            SearchHitField userids = hit.getField("userids");
//            System.out.println(userids.getValue().toString());
//            if (null!=userids.getValue()) {
//                System.out.println(userids);
//            }
            listRowkey.add(builderObject(hit));
        }
        long o = System.currentTimeMillis();
        System.out.println(o - l + "---");
        return listRowkey;
    }

    public static void main(String[] args) {
        EsPageQuery esPageQuery = new EsPageQuery() {
            @Override
            protected Object builderObject(SearchHit hit) {
                return hit.getSource();
            }
        };
        Map<String, Object> datas = new HashMap<>();
        LinkedList<String> appids = new LinkedList<>();
        appids.add("1204");
        appids.add("1206");
        if (appids.size() > 0) {
            datas.put("appid", appids);
        }
//
        datas.put("platform", 2);


        List<String> eventList = new LinkedList<>();
        eventList.add("");
        if (eventList.size() > 0) {
            datas.put("events.keyword", eventList);
        }
        Set<String> uuids = new HashSet<>();
        uuids.add("0868bb0cb90eab34be537bcfbc2fd61f");
        datas.put("uuid", JavaUtils.set2List(uuids));

        EsPageBean pageData = esPageQuery.getPageData(EsTableName.ES_INDEX_BIGDATA_VISIT, EsTableName.ES_TYPE_VISIT, 1, 10, null, datas);
        System.out.println(pageData);
    }

    protected abstract T builderObject(SearchHit hit);

}
