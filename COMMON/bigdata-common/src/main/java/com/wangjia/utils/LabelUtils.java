package com.wangjia.utils;

/**
 * Created by Administrator on 2017/5/10.
 */
public final class LabelUtils {
    /**
     * 次数权重
     */
    private static final float WEB_W_COUNT = 0.7f;
    private static final float MOBILE_W_COUNT = 0.3f;

    /**
     * 时间权重
     */
    private static final float WEB_W_TIME = 0.3f;
    private static final float MOBILE_W_TIME = 0.7f;

    /**
     * 时间比上次数
     */
    private static final float WEB_TIME_COUNT_MUL = 20000.0f;
    private static final float MOBILE_TIME_COUNT_MUL = 10000.0f;

    /**
     * 计算WEB标签值
     *
     * @param wc    来源权重
     * @param wb    行为权得
     * @param count 次数
     * @param time  时间
     * @return 标签的值
     */
    @Deprecated
    public static float getWebLabelValue(float wc, float wb, int count, long time) {
        return wc * wb * count * WEB_W_COUNT + time / WEB_TIME_COUNT_MUL * WEB_W_TIME;
    }

    /**
     * 计算 WEB标签值
     *
     * @param count
     * @param time
     * @return
     */
    @Deprecated
    public static float getWebLabelValue(int count, long time) {
        return getWebLabelValue(1.0f, 1.0f, count, time);
    }

    /**
     * 计算WEB标签值
     *
     * @param count
     * @param time
     * @param weight
     * @param baseWeight
     * @return
     */
    public static float getWebLabelValue(int count, long time, float weight, float baseWeight) {
        return (count * WEB_W_COUNT + time / WEB_TIME_COUNT_MUL * WEB_W_TIME) * (weight + baseWeight);
    }

    /**
     * 计算MOBILE标签值
     *
     * @param count
     * @param time
     * @param weight
     * @param baseWeight
     * @return
     */
    public static float getMobileLabelValue(int count, long time, float weight, float baseWeight) {
        return (count * MOBILE_W_COUNT + time / MOBILE_TIME_COUNT_MUL * MOBILE_W_TIME) * (weight + baseWeight);
    }
}
