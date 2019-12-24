package com.wangjia.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Administrator on str.add("2017/9/13.
 */
public class DateUtils {
    private static final Object lock = new Object();
    private static ThreadLocal<SimpleDateFormat> myFms = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat();
        }
    };

    private static final Map<Integer, Boolean> mapDayOff = new HashMap<>();

    private static void initDayOff() {
        mapDayOff.put(17168, true);
        mapDayOff.put(17169, true);
        mapDayOff.put(17174, true);
        mapDayOff.put(17175, true);
        mapDayOff.put(17181, true);
        mapDayOff.put(17182, true);
        mapDayOff.put(17188, true);
        mapDayOff.put(17194, true);
        mapDayOff.put(17195, true);
        mapDayOff.put(17196, true);
        mapDayOff.put(17197, true);
        mapDayOff.put(17198, true);
        mapDayOff.put(17199, true);
        mapDayOff.put(17200, true);
        mapDayOff.put(17203, true);
        mapDayOff.put(17209, true);
        mapDayOff.put(17210, true);
        mapDayOff.put(17216, true);
        mapDayOff.put(17217, true);
        mapDayOff.put(17223, true);
        mapDayOff.put(17224, true);
        mapDayOff.put(17230, true);
        mapDayOff.put(17231, true);
        mapDayOff.put(17237, true);
        mapDayOff.put(17238, true);
        mapDayOff.put(17244, true);
        mapDayOff.put(17245, true);
        mapDayOff.put(17251, true);
        mapDayOff.put(17252, true);
        mapDayOff.put(17259, true);
        mapDayOff.put(17260, true);
        mapDayOff.put(17261, true);
        mapDayOff.put(17265, true);
        mapDayOff.put(17266, true);
        mapDayOff.put(17272, true);
        mapDayOff.put(17273, true);
        mapDayOff.put(17279, true);
        mapDayOff.put(17280, true);
        mapDayOff.put(17286, true);
        mapDayOff.put(17287, true);
        mapDayOff.put(17288, true);
        mapDayOff.put(17293, true);
        mapDayOff.put(17294, true);
        mapDayOff.put(17300, true);
        mapDayOff.put(17301, true);
        mapDayOff.put(17307, true);
        mapDayOff.put(17308, true);
        mapDayOff.put(17315, true);
        mapDayOff.put(17316, true);
        mapDayOff.put(17317, true);
        mapDayOff.put(17321, true);
        mapDayOff.put(17322, true);
        mapDayOff.put(17328, true);
        mapDayOff.put(17329, true);
        mapDayOff.put(17335, true);
        mapDayOff.put(17336, true);
        mapDayOff.put(17342, true);
        mapDayOff.put(17343, true);
        mapDayOff.put(17349, true);
        mapDayOff.put(17350, true);
        mapDayOff.put(17356, true);
        mapDayOff.put(17357, true);
        mapDayOff.put(17363, true);
        mapDayOff.put(17364, true);
        mapDayOff.put(17370, true);
        mapDayOff.put(17371, true);
        mapDayOff.put(17377, true);
        mapDayOff.put(17378, true);
        mapDayOff.put(17384, true);
        mapDayOff.put(17385, true);
        mapDayOff.put(17391, true);
        mapDayOff.put(17392, true);
        mapDayOff.put(17398, true);
        mapDayOff.put(17399, true);
        mapDayOff.put(17405, true);
        mapDayOff.put(17406, true);
        mapDayOff.put(17412, true);
        mapDayOff.put(17413, true);
        mapDayOff.put(17419, true);
        mapDayOff.put(17420, true);
        mapDayOff.put(17426, true);
        mapDayOff.put(17427, true);
        mapDayOff.put(17433, true);
        mapDayOff.put(17434, true);
        mapDayOff.put(17441, true);
        mapDayOff.put(17442, true);
        mapDayOff.put(17443, true);
        mapDayOff.put(17444, true);
        mapDayOff.put(17445, true);
        mapDayOff.put(17446, true);
        mapDayOff.put(17447, true);
        mapDayOff.put(17448, true);
        mapDayOff.put(17454, true);
        mapDayOff.put(17455, true);
        mapDayOff.put(17461, true);
        mapDayOff.put(17462, true);
        mapDayOff.put(17468, true);
        mapDayOff.put(17469, true);
        mapDayOff.put(17475, true);
        mapDayOff.put(17476, true);
        mapDayOff.put(17482, true);
        mapDayOff.put(17483, true);
        mapDayOff.put(17489, true);
        mapDayOff.put(17490, true);
        mapDayOff.put(17496, true);
        mapDayOff.put(17497, true);
        mapDayOff.put(17503, true);
        mapDayOff.put(17504, true);
        mapDayOff.put(17510, true);
        mapDayOff.put(17511, true);
        mapDayOff.put(17517, true);
        mapDayOff.put(17518, true);
        mapDayOff.put(17524, true);
        mapDayOff.put(17525, true);
        mapDayOff.put(17531, true);
        mapDayOff.put(17532, true);

    }

    static {
        initDayOff();
    }

    private static void formatDayOff() throws ParseException {
        List<String> str = new LinkedList<>();
        str.add("2017-01-01");
        str.add("2017-01-02");
        str.add("2017-01-07");
        str.add("2017-01-08");
        str.add("2017-01-14");
        str.add("2017-01-15");
        str.add("2017-01-21");
        str.add("2017-01-27");
        str.add("2017-01-28");
        str.add("2017-01-29");
        str.add("2017-01-30");
        str.add("2017-01-31");
        str.add("2017-02-01");
        str.add("2017-02-02");
        str.add("2017-02-05");
        str.add("2017-02-11");
        str.add("2017-02-12");
        str.add("2017-02-18");
        str.add("2017-02-19");
        str.add("2017-02-25");
        str.add("2017-02-26");
        str.add("2017-03-04");
        str.add("2017-03-05");
        str.add("2017-03-11");
        str.add("2017-03-12");
        str.add("2017-03-18");
        str.add("2017-03-19");
        str.add("2017-03-25");
        str.add("2017-03-26");
        str.add("2017-04-02");
        str.add("2017-04-03");
        str.add("2017-04-04");
        str.add("2017-04-08");
        str.add("2017-04-09");
        str.add("2017-04-15");
        str.add("2017-04-16");
        str.add("2017-04-22");
        str.add("2017-04-23");
        str.add("2017-04-29");
        str.add("2017-04-30");
        str.add("2017-05-01");
        str.add("2017-05-06");
        str.add("2017-05-07");
        str.add("2017-05-13");
        str.add("2017-05-14");
        str.add("2017-05-20");
        str.add("2017-05-21");
        str.add("2017-05-28");
        str.add("2017-05-29");
        str.add("2017-05-30");
        str.add("2017-06-03");
        str.add("2017-06-04");
        str.add("2017-06-10");
        str.add("2017-06-11");
        str.add("2017-06-17");
        str.add("2017-06-18");
        str.add("2017-06-24");
        str.add("2017-06-25");
        str.add("2017-07-01");
        str.add("2017-07-02");
        str.add("2017-07-08");
        str.add("2017-07-09");
        str.add("2017-07-15");
        str.add("2017-07-16");
        str.add("2017-07-22");
        str.add("2017-07-23");
        str.add("2017-07-29");
        str.add("2017-07-30");
        str.add("2017-08-05");
        str.add("2017-08-06");
        str.add("2017-08-12");
        str.add("2017-08-13");
        str.add("2017-08-19");
        str.add("2017-08-20");
        str.add("2017-08-26");
        str.add("2017-08-27");
        str.add("2017-09-02");
        str.add("2017-09-03");
        str.add("2017-09-09");
        str.add("2017-09-10");
        str.add("2017-09-16");
        str.add("2017-09-17");
        str.add("2017-09-23");
        str.add("2017-09-24");
        str.add("2017-10-01");
        str.add("2017-10-02");
        str.add("2017-10-03");
        str.add("2017-10-04");
        str.add("2017-10-05");
        str.add("2017-10-06");
        str.add("2017-10-07");
        str.add("2017-10-08");
        str.add("2017-10-14");
        str.add("2017-10-15");
        str.add("2017-10-21");
        str.add("2017-10-22");
        str.add("2017-10-28");
        str.add("2017-10-29");
        str.add("2017-11-04");
        str.add("2017-11-05");
        str.add("2017-11-11");
        str.add("2017-11-12");
        str.add("2017-11-18");
        str.add("2017-11-19");
        str.add("2017-11-25");
        str.add("2017-11-26");
        str.add("2017-12-02");
        str.add("2017-12-03");
        str.add("2017-12-09");
        str.add("2017-12-10");
        str.add("2017-12-16");
        str.add("2017-12-17");
        str.add("2017-12-23");
        str.add("2017-12-24");
        str.add("2017-12-30");
        str.add("2017-12-31");

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        for (int i = 0; i < str.size(); i++) {
            Date d = df.parse(str.get(i));
            System.out.println(JavaUtils.timeMillis2DayNum(d.getTime()));
        }
    }

    public static boolean isDayOff(int dayNum) {
        return mapDayOff.getOrDefault(dayNum, false);
    }
}
