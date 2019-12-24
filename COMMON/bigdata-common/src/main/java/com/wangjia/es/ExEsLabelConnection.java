package com.wangjia.es;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.*;

/**
 * Created by Administrator on 2017/8/28.
 */
public class ExEsLabelConnection extends EsConnection {
    private static final int MAX_SIZE = 500;
    //    private XContentBuilder builder = null;
    private LinkedList<EsBean> beans = new LinkedList<>();
    private long addTime;

    public ExEsLabelConnection() {

    }

    public ExEsLabelConnection(long addTime) {
        this.addTime = addTime;
    }

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
            XContentBuilder builder = null;
            try {
                builder = XContentFactory.jsonBuilder();
            } catch (IOException e) {
                e.printStackTrace();
            }
            EsBean bean = beans.removeFirst();
            ArrayList<WeakHashMap<String, Object>> listLas = new ArrayList<>();
            try {
                if (bean.data instanceof Map) {
                    Map data = (Map) bean.data;
                    for (Object label : data.keySet()) {
                        int value = (int) data.get(label);
                        String key = String.valueOf(label);
                        WeakHashMap<String, Object> labelMap = new WeakHashMap<>();
                        labelMap.put("tag", key);
                        labelMap.put("weight", value);
                        listLas.add(labelMap);
                    }
                }

                builder.startObject().field("uuid", bean.getId()).field("label", listLas).field("addTime", addTime).endObject();
            } catch (IOException e) {
                e.printStackTrace();

            }
            bulkRequest.add(client.prepareIndex(bean.index, bean.type, bean.id)
                    .setSource(builder));
        }

        BulkResponse bulkItemResponses = bulkRequest.get();
        return !bulkItemResponses.hasFailures();
    }

}
