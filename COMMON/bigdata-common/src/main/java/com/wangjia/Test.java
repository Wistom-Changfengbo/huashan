package com.wangjia;

import com.jcraft.jsch.jce.MD5;
import com.wangjia.utils.MD5Utils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created by Administrator on 2017/7/6.
 */
public class Test {

    public static final class AAA{
        protected String aa;
        protected Integer a;
    }


    public static void aaa() throws IOException {
        Properties _prop = new Properties();
        _prop.load(Test.class.getResourceAsStream("youngcloud/config.properties"));

        String zookeeper_hosts = _prop.getProperty("ZOOKEEPER_HOSTS");
        System.out.println(zookeeper_hosts);
    }


    static ZooKeeper zk = null;

    public static void delPath(String path) throws Exception {
        List<String> paths = zk.getChildren(path, false);
        for (String p : paths) {
            delPath(path + "/" + p);
            System.out.println(path + "/" + p);
        }
        for (String p : paths) {
            zk.delete(path + "/" + p, -1);
        }
    }

    public static void main(String[] args) throws Exception {



        long l = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++)
            MD5Utils.MD5("1234454654895613216548965458778961");
        System.out.println(System.currentTimeMillis() - l);
        System.out.println(MD5Utils.MD5("1234454654895613216548965458778961"));

        AAA aaa = new AAA();
        System.out.println(aaa.a == null);
    }


}


