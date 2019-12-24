package com.wangjia.handler.gps;

import com.alibaba.fastjson.JSON;
import com.wangjia.utils.DateUtils;
import com.wangjia.utils.JavaUtils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by Administrator on 2017/11/1.
 */
public class GPSGraphHandler {

    private static final Double[] NIGHT_HOUR_WEIGHT = new Double[24];

    static {
        NIGHT_HOUR_WEIGHT[0] = 1.0;
        NIGHT_HOUR_WEIGHT[1] = 1.0;
        NIGHT_HOUR_WEIGHT[2] = 1.0;
        NIGHT_HOUR_WEIGHT[3] = 1.0;
        NIGHT_HOUR_WEIGHT[4] = 1.0;
        NIGHT_HOUR_WEIGHT[5] = 1.0;
        NIGHT_HOUR_WEIGHT[6] = 1.0;
        NIGHT_HOUR_WEIGHT[7] = 0.4;
        NIGHT_HOUR_WEIGHT[8] = 0.1;
        NIGHT_HOUR_WEIGHT[9] = -0.1;
        NIGHT_HOUR_WEIGHT[10] = -1.0;
        NIGHT_HOUR_WEIGHT[11] = -1.0;
        NIGHT_HOUR_WEIGHT[12] = -1.0;
        NIGHT_HOUR_WEIGHT[13] = -1.0;
        NIGHT_HOUR_WEIGHT[14] = -1.0;
        NIGHT_HOUR_WEIGHT[15] = -1.0;
        NIGHT_HOUR_WEIGHT[16] = -1.0;
        NIGHT_HOUR_WEIGHT[17] = -1.0;
        NIGHT_HOUR_WEIGHT[18] = -0.6;
        NIGHT_HOUR_WEIGHT[19] = -0.1;
        NIGHT_HOUR_WEIGHT[20] = 0.2;
        NIGHT_HOUR_WEIGHT[21] = 0.4;
        NIGHT_HOUR_WEIGHT[22] = 0.9;
        NIGHT_HOUR_WEIGHT[23] = 1.0;
    }

    public static final Double DEFAULT_JD_WD_VALUE = 0.0;
    public static final Double DEFAULT_RADIUS = 0.05;

    public static final class GPSGraph {
        protected double cjd;
        protected double cwd;
        protected double r;
        protected int num;
        protected long minTime;
        protected long maxTime;

        protected LinkedList<Double> jds = new LinkedList<>();
        protected LinkedList<Double> wds = new LinkedList<>();

        protected HashMap<Integer, HashMap<Integer, Integer>> graph = new HashMap<>();

        public double getCjd() {
            return cjd;
        }

        public void setCjd(double cjd) {
            this.cjd = cjd;
        }

        public double getCwd() {
            return cwd;
        }

        public void setCwd(double cwd) {
            this.cwd = cwd;
        }

        public double getR() {
            return r;
        }

        public void setR(double r) {
            this.r = r;
        }

        public int getNum() {
            return num;
        }

        public void setNum(int num) {
            this.num = num;
        }

        public long getMinTime() {
            return minTime;
        }

        public void setMinTime(long minTime) {
            this.minTime = minTime;
        }

        public long getMaxTime() {
            return maxTime;
        }

        public void setMaxTime(long maxTime) {
            this.maxTime = maxTime;
        }

        public LinkedList<Double> getJds() {
            return jds;
        }

        public void setJds(LinkedList<Double> jds) {
            this.jds = jds;
        }

        public LinkedList<Double> getWds() {
            return wds;
        }

        public void setWds(LinkedList<Double> wds) {
            this.wds = wds;
        }

        public HashMap<Integer, HashMap<Integer, Integer>> getGraph() {
            return graph;
        }

        public void setGraph(HashMap<Integer, HashMap<Integer, Integer>> graph) {
            this.graph = graph;
        }
    }

    private LinkedList<GPSGraph> graphList = new LinkedList<>();

    public GPSGraphHandler() {

    }

    public GPSGraphHandler(String str) {
        if (str == null || str.length() == 0)
            return;
        GPSGraph[] gpsGraphs = JSON.parseObject(str, GPSGraph[].class);
        for (GPSGraph g : gpsGraphs)
            graphList.add(g);
    }

    public String toJSONString() {
        return JSON.toJSONString(graphList);
    }

    public GPSGraph getGPSGraph(long time, double jd, double wd) {
        GPSGraph graph = null;
        Double minR = Double.MAX_VALUE;
        Double r = 0.;
        for (GPSGraph g : graphList) {
            r = Math.pow(jd - g.getCjd(), 2) + Math.pow(wd - g.getCwd(), 2);
            if (r < minR) {
                minR = r;
                graph = g;
            }
        }

        if (graph == null
                || JavaUtils.timeMillis2DayNum(time) - JavaUtils.timeMillis2DayNum(graph.maxTime) >= 60
                || minR >= Math.pow(graph.getR(), 2)) {
            graph = new GPSGraph();
            graphList.add(graph);
            graph.cjd = jd;
            graph.cwd = wd;
            graph.r = DEFAULT_RADIUS;
            graph.num = 0;

            graph.minTime = Long.MAX_VALUE;
            graph.maxTime = 0L;
        }
        return graph;
    }

    public GPSGraph addGPS(long time, double jd, double wd) {
        GPSGraph graph = getGPSGraph(time, jd, wd);

        graph.cjd = (graph.cjd * graph.num + jd) / (graph.num + 1);
        graph.cwd = (graph.cwd * graph.num + wd) / (graph.num + 1);
        graph.num++;

        if (graph.maxTime < time)
            graph.maxTime = time;
        if (graph.minTime > time)
            graph.minTime = time;

        graph.jds.add(jd);
        graph.wds.add(wd);

        if (graph.jds.size() > 50) {
            double maxR = 0.0;
            double r = 0.0;
            int maxIndex = 0;
            for (int i = 0; i < graph.jds.size(); i++) {
                r = Math.pow(graph.jds.get(i) - graph.cjd, 2) + Math.pow(graph.wds.get(i) - graph.cwd, 2);
                if (r > maxR) {
                    maxR = r;
                    maxIndex = i;
                }
            }
            graph.jds.remove(maxIndex);
            graph.wds.remove(maxIndex);
        }

        int day = JavaUtils.timeMillis2DayNum(time);
        int hour = (int) (((time + 28800000) % 86400000) / 3600000);

        HashMap<Integer, Integer> mapday = graph.graph.get(day);
        if (mapday == null) {
            mapday = new HashMap<Integer, Integer>();
            graph.graph.put(day, mapday);
        }
        mapday.put(hour, mapday.getOrDefault(hour, 0) + 1);

        return graph;
    }

    public LinkedList<GPSGraph> getGraphList() {
        return graphList;
    }

    public double[] getGPS(GPSGraph graph) {
        double[] gps = new double[2];
        double minR = Double.MAX_VALUE;
        double r;
        int minIndex = 0;
        for (int i = 0; i < graph.jds.size(); i++) {
            r = Math.pow(graph.jds.get(i) - graph.cjd, 2) + Math.pow(graph.wds.get(i) - graph.cwd, 2);
            if (r < minR) {
                minR = r;
                minIndex = i;
            }
        }
        gps[0] = graph.jds.get(minIndex);
        gps[1] = graph.wds.get(minIndex);
        return gps;
    }

    public double getNightValue(GPSGraph gPSGraph) {
        double night = 0.;
        int h, v;
        HashMap<Integer, HashMap<Integer, Integer>> graph = gPSGraph.graph;
        for (Map.Entry<Integer, HashMap<Integer, Integer>> item : graph.entrySet()) {
            for (Map.Entry<Integer, Integer> hour : item.getValue().entrySet()) {
                h = hour.getKey();
                v = hour.getValue();
                if (h >= 0 && h <= 23) {
                    night += v * NIGHT_HOUR_WEIGHT[h];
                }
            }
        }
        return night;
    }

    public int getDayOffValue(GPSGraph gPSGraph) {
        int dayOff = 0;
        HashMap<Integer, HashMap<Integer, Integer>> graph = gPSGraph.graph;
        for (Map.Entry<Integer, HashMap<Integer, Integer>> item : graph.entrySet()) {
            if (DateUtils.isDayOff(item.getKey()))
                dayOff += 1;
            else
                dayOff -= 1;
        }
        return dayOff;
    }
}
