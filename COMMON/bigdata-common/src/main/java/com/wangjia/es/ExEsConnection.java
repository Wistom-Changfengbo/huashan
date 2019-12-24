package com.wangjia.es;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by Administrator on 2017/8/28.
 */
public class ExEsConnection extends EsConnection {
    private static final int MAX_SIZE = 500;

    private LinkedList<EsBean> beans = new LinkedList<>();


    @Override
    public DocWriteResponse.Result add(EsBean bean) {
        bean.handleType = EsConst.ES_HANDLE_ADD;
        beans.addLast(bean);
        if (beans.size() > MAX_SIZE) {
            flush();
        }
        return null;
    }

    @Override
    public DocWriteResponse.Result add(String index, String type, String id, Object data) {
        EsBean bean = new EsBean(index, type, id, data);
        return add(bean);
    }

    @Override
    public DocWriteResponse.Result delete(EsBean bean) {
        bean.handleType = EsConst.ES_HANDLE_DEL;
        beans.addLast(bean);
        if (beans.size() > MAX_SIZE) {
            flush();
        }
        return null;
    }

    @Override
    public DocWriteResponse.Result delete(String index, String type, String id) {
        EsBean bean = new EsBean(index, type, id);
        return delete(bean);
    }

    @Override
    public DocWriteResponse.Result update(EsBean bean) {
        bean.handleType = EsConst.ES_HANDLE_UPDATE;
        beans.addLast(bean);
        if (beans.size() > MAX_SIZE) {
            flush();
        }
        return null;
    }

    @Override
    public DocWriteResponse.Result update(String index, String type, String id, Object data) {
        EsBean bean = new EsBean(index, type, id, data);
        return update(bean);
    }

    public void flush() {
        bulk();
    }

    @Override
    public void close() {
        flush();
        super.close();
    }

    private boolean bulk() {
        if (beans.isEmpty())
            return true;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        while (!beans.isEmpty()) {
            EsBean bean = beans.removeFirst();
            switch (bean.handleType) {
                case EsConst.ES_HANDLE_ADD: {
                    bulkRequest.add(client.prepareIndex(bean.index, bean.type, bean.id)
                            .setSource(jsonStr(bean.data), XContentType.JSON)
                    );
                    break;
                }
                case EsConst.ES_HANDLE_DEL: {
                    bulkRequest.add(client.prepareDelete(bean.index, bean.type, bean.id));
                    break;
                }
                case EsConst.ES_HANDLE_UPDATE: {
                    bulkRequest.add(client.prepareUpdate(bean.index, bean.type, bean.id)
                            .setDoc(jsonStr(bean.data), XContentType.JSON));
                    break;
                }
            }
        }
        BulkResponse bulkItemResponses = bulkRequest.get();
        return !bulkItemResponses.hasFailures();
    }

    private boolean bulk2() {
        if (beans.isEmpty())
            return true;
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                        System.out.println(request);
                    }

                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        System.out.println(response);
                    }

                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        System.out.println(request.toString());
                    }
                })
                .setBulkActions(1000)
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
        while (!beans.isEmpty()) {
            EsBean bean = beans.removeFirst();
            switch (bean.handleType) {
                case EsConst.ES_HANDLE_ADD: {
                    bulkProcessor.add(new IndexRequest(bean.index, bean.type, bean.id)
                            .source(jsonStr(bean.data), XContentType.JSON)
                    );
                    break;
                }
                case EsConst.ES_HANDLE_DEL: {
                    bulkProcessor.add(new DeleteRequest(bean.index, bean.type, bean.id));
                    break;
                }
                case EsConst.ES_HANDLE_UPDATE: {
                    bulkProcessor.add(new UpdateRequest(bean.index, bean.type, bean.id)
                            .doc(jsonStr(bean.data), XContentType.JSON));
                    break;
                }
            }
        }

        return true;
    }

}
