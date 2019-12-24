package com.wangjia.utils;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Administrator on 2018/3/27.
 */
public final class EsUtils {

    public static TransportClient getEsClient() {
        try {
            Settings settings = Settings.builder()
                    .put("cluster.name", "elasticsearch")
                    .put("transport.type", "netty3")
                    .put("http.type", "netty3")
                    .build();
            TransportClient client = new PreBuiltTransportClient(settings);
            //client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.100.34"), 9300));
            // client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.100.35"), 9300));
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
            return client;
        } catch (UnknownHostException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
