package com.wangjia.math;

/**
 * Created by Administrator on 2017/9/12.
 */
public class ExMath {

    /**
     * 衰减公式
     *
     * @param value
     * @param pace  衰减步长
     * @param max   最大步长
     * @return
     */
    public static double attenuation(double value, int pace, int max) {
        if (pace >= max)
            return 0F;
        return value * (max - pace) / max;
    }


    /**
     * 60天，衰减公式
     *
     * @param value
     * @param pace
     * @return
     */
    public static double attenuation(double value, int pace) {
        return attenuation(value, pace, 60);
    }
}
