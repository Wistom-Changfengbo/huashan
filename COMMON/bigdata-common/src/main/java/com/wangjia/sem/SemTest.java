package com.wangjia.sem;

import com.wangjia.bean.sem.SemKeyWordInfo;
import com.wangjia.bean.sem.SemKeyWordRelateInfo;
import com.wangjia.sem.ReportService;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;

/**
 * Created by Cfb on 2018/5/16.
 */
public class SemTest {
    public static void main(String[] args) throws UnsupportedEncodingException {
        String format =String.format("hello?pid=%s&cid=%s","莉莉娅","gongsiid");
        System.out.println(format);
        System.out.println();
//        ReportService reportService = new ReportService("1","1001",new String("菲特盟德".getBytes(),"utf-8"), "Jiajuol2018", "4bad25ddf0f647e340283c6516456435", "");
//        ReportService reportService = new ReportService("9","111",new String("三亚东易日盛装饰".getBytes(),"utf-8"), "Dyrs13379994999", "85700b88bca8f6a9e5e26d7971c1040c", "");
        ReportService reportService = new ReportService("9","3026",new String("榆次博尔装饰".getBytes(),"utf-8"), "BEzs2018", "971cdfc5dbfffb362e9dfb7fc2837715", "");
//        ReportService reportService = new ReportService("2","1001",new String("东易整体家居".getBytes(),"utf-8"), "Jd1N99", "a3a83365dd2ef1aebcad8e25f3410702", "");
//        ReportService reportService = new ReportService("2","3015",new String("银川链家装饰".getBytes(),"utf-8"), "YCljzs2018", "c9512218c8c9d9884c5de8cdfb16c4bd", "");
        List<SemKeyWordInfo> list = reportService.getRealTimePairData(Collections.emptyList(),12,5,15,"2018-09-01 00:00:00","2018-09-30 23:59:59");
        List<SemKeyWordRelateInfo> list1 = reportService.getRealTimeQueryData(Collections.emptyList(),0,12,5,6,"2018-12-01 00:00:00","2018-12-19 23:59:59");
        System.out.println(list.size());
        System.out.println(list1.size());
//        list.forEach((x) ->
//        {
//            System.out.println(x);s
//
////            System.out.println("用户名:" + x.getPairInfo(0));
////            System.out.println("推广计划:" + x.getPairInfo(1));
////            System.out.println("推广单元:" + x.getPairInfo(2));
////            System.out.println("推广关键字:" + x.getPairInfo(3));
////            System.out.println("创意标题:" + x.getPairInfo(4));
////            System.out.println("创意描述一:" + x.getPairInfo(5));
////            System.out.println("创意描述二:" + x.getPairInfo(6));
////            System.out.println("创意显示url:" + x.getPairInfo(7));
////            System.out.println("时间:" + x.getDate());
////            System.out.println("展示次数"+x.getKPI(0));
////            System.out.println("点击次数"+x.getKPI(1));
////            System.out.println("花费"+x.getKPI(2));
////            System.out.println("平均点击价格"+x.getKPI(3));
////            System.out.println("点击率"+x.getKPI(4));
////            System.out.println("每千人成本"+x.getKPI(5));
////            System.out.println("转化"+x.getKPI(6));
//        });
    }
}
