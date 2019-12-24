package com.wangjia.sem;


import com.baidu.drapi.autosdk.core.CommonService;
import com.baidu.drapi.autosdk.core.ResHeader;
import com.baidu.drapi.autosdk.core.ResHeaderUtil;
import com.baidu.drapi.autosdk.core.ServiceFactory;
import com.baidu.drapi.autosdk.exception.ApiException;
import com.baidu.drapi.autosdk.sms.service.*;
import com.wangjia.bean.sem.SemKeyWordInfo;
import com.wangjia.bean.sem.SemKeyWordRelateInfo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Cfb on 2018/5/16.
 */
public class ReportService {
    private CommonService factory;
    private com.baidu.drapi.autosdk.sms.service.ReportService reportService;
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss");
    private static String SUCCESS = "success";
    private static List<String> PERFORMANCE_REALTIMEQUERY = Arrays.asList(new String[]{
            "impression", "click", "cost", "ctr"});
    private static List<String> PERFORMANCE_REALTIMEPAIR = Arrays.asList(new String[]{
            "impression", "click", "cost", "cpc", "ctr", "cpm",
            "conversion"});
    private String appid;
    private String sourceid;

    public ReportService(String sourceid, String appid, String username, String password, String token, String target) {
        try {
            this.appid = appid;
            this.sourceid = sourceid;
            factory = ServiceFactory.getInstance(username, password, token, target);
            reportService = factory.getService(com.baidu.drapi.autosdk.sms.service.ReportService.class);
        } catch (ApiException e) {
            e.printStackTrace();
        }
    }

    public List<SemKeyWordInfo> getRealTimePairData(List<String> performanceData, int levelOfDetails, int unitOfTime, int reportType, String start, String end) {
        List<SemKeyWordInfo> semList = new ArrayList<>();
        GetRealTimePairDataRequest request = new GetRealTimePairDataRequest();
        ReportRequestType type = new ReportRequestType();
        if (null == performanceData || performanceData.isEmpty()) {
            type.setPerformanceData(PERFORMANCE_REALTIMEPAIR);
        } else {
            type.setPerformanceData(performanceData);
        }
        type.setLevelOfDetails(levelOfDetails);
        type.setUnitOfTime(unitOfTime);
        type.setReportType(reportType);

        try {
            type.setStartDate(simpleDateFormat.parse(start));
            type.setEndDate(simpleDateFormat.parse(end));

            request.setRealTimePairRequestType(type);
            GetRealTimePairDataResponse response = reportService
                    .getRealTimePairData(request);
            ResHeader rheader = null;

            rheader = ResHeaderUtil.getResHeader(reportService, true);

            if (SUCCESS.equals(rheader.getDesc()) && rheader.getStatus() == 0) {
                List<RealTimePairResultType> datas = response.getData();
                datas.forEach((x) ->
                {
                    SemKeyWordInfo semKeyWordInfo = new SemKeyWordInfo();
                    semKeyWordInfo.setAppid(appid);
                    semKeyWordInfo.setSourceid(sourceid);
                    semKeyWordInfo.setKeywordId(x.getKeywordId());
                    semKeyWordInfo.setCreativeId(x.getCreativeId());
                    semKeyWordInfo.setUsername(x.getPairInfo(0));
                    semKeyWordInfo.setCampaignName(x.getPairInfo(1));
                    semKeyWordInfo.setAdgroup(x.getPairInfo(2));
                    semKeyWordInfo.setKeyword(x.getPairInfo(3));
                    semKeyWordInfo.setCreativeTitle(x.getPairInfo(4));
                    semKeyWordInfo.setCreativeDesc1(x.getPairInfo(5));
                    semKeyWordInfo.setCreativeDesc2(x.getPairInfo(6));
                    semKeyWordInfo.setCreativeUrl(x.getPairInfo(7));
                    semKeyWordInfo.setDisplay(Integer.parseInt(x.getKPI(0)));
                    semKeyWordInfo.setClick(Integer.parseInt(x.getKPI(1)));
                    semKeyWordInfo.setCost(Float.parseFloat(x.getKPI(2)));
                    semKeyWordInfo.setCostAvg(Float.parseFloat(x.getKPI(3)));
                    semKeyWordInfo.setClickRate(Float.parseFloat(x.getKPI(4)));
                    semKeyWordInfo.setCpm(Float.parseFloat(x.getKPI(5)));
                    semKeyWordInfo.setConversion(Integer.parseInt(x.getKPI(6)));
                    semKeyWordInfo.setTimeStamp(x.getDate());
                    semList.add(semKeyWordInfo);
                });
                return semList;
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }

    public List<SemKeyWordRelateInfo> getRealTimeQueryData(List<String> performanceData, int device,
                                                           int levelOfDetails, int unitOfTime, int reportType, String start, String end) {
        List<SemKeyWordRelateInfo> semList = new ArrayList<>();
        GetRealTimeQueryDataRequest request = new GetRealTimeQueryDataRequest();
        ReportRequestType type = new ReportRequestType();
        if (null == performanceData || performanceData.isEmpty()) {
            type.setPerformanceData(PERFORMANCE_REALTIMEQUERY);
        } else {
            type.setPerformanceData(performanceData);
        }
        type.setDevice(device);
        type.setLevelOfDetails(levelOfDetails);
        type.setUnitOfTime(unitOfTime);
        type.setReportType(reportType);
        try {
            type.setStartDate(simpleDateFormat.parse(start));
            type.setEndDate(simpleDateFormat.parse(end));

            request.setRealTimeQueryRequestType(type);
            GetRealTimeQueryDataResponse response = reportService
                    .getRealTimeQueryData(request);
            ResHeader rheader = null;

            rheader = ResHeaderUtil.getResHeader(reportService, true);

            if (SUCCESS.equals(rheader.getDesc()) && rheader.getStatus() == 0) {
                List<RealTimeQueryResultType> datas = response.getData();
                datas.forEach((x) ->
                {
                    SemKeyWordRelateInfo semKeyWordRelateInfo = new SemKeyWordRelateInfo();
                    semKeyWordRelateInfo.setAppid(appid);
                    semKeyWordRelateInfo.setSourceid(sourceid);
                    semKeyWordRelateInfo.setQuery(x.getQuery());
                    semKeyWordRelateInfo.setKeywordId(x.getKeywordId());
                    semKeyWordRelateInfo.setCreativeId(x.getCreativeId());
                    semKeyWordRelateInfo.setDisplay(Integer.parseInt(x.getKPI(0)));
                    semKeyWordRelateInfo.setClick(Integer.parseInt(x.getKPI(1)));
                    semKeyWordRelateInfo.setCost(Float.parseFloat(x.getKPI(2)));
                    semKeyWordRelateInfo.setClickRate(Float.parseFloat(x.getKPI(3)));
                    semKeyWordRelateInfo.setTimeStamp(x.getDate());
                    semList.add(semKeyWordRelateInfo);
                });
                return semList;
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }


}

