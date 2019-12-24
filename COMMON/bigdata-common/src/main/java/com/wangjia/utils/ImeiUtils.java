package com.wangjia.utils;

/**
 * Created by Administrator on 2017/10/23.
 * <p>
 * imei由15位数字组成，
 * 前6位(TAC)是型号核准号码，代表手机类型。
 * 接着2位(FAC)是最后装配号，代表产地。
 * 后6位(SNR)是串号，代表生产顺序号。
 * 最后1位 (SP)是检验码。
 * <p>
 * 检验码计算：
 * (1).将偶数位数字分别乘以2，分别计算个位数和十位数之和
 * (2).将奇数位数字相加，再加上上一步算得的值
 * (3).如果得出的数个位是0则校验位为0，否则为10减去个位数
 */
public final class ImeiUtils {
    /**
     * 通过imei的前14位获取完整的imei(15位)
     *
     * @param imeiString
     * @return
     */
    public static String getImeiBy14(String imeiString) {
        String retVal = null;
        char[] imeiChar = imeiString.toCharArray();
        for (int i = 0; i < imeiChar.length; i++) {
            if (imeiChar[i] > '9' || imeiChar[i] < '0')
                return imeiString.toUpperCase() + '0';
        }

        int resultInt = 0;
        for (int i = 0; i < imeiChar.length; i++) {
            int a = Integer.parseInt(String.valueOf(imeiChar[i]));
            i++;
            final int temp = Integer.parseInt(String.valueOf(imeiChar[i])) * 2;
            final int b = temp < 10 ? temp : temp - 9;
            resultInt += a + b;
        }
        resultInt %= 10;
        resultInt = resultInt == 0 ? 0 : 10 - resultInt;
        retVal = imeiString + resultInt;
        return retVal;
    }

    public static void main(String[] args) {
        /**
         * 869310020601038ec456a4cbb5e1829
         *
         * 35538306376023
         * 最后一位是6
         */
        System.out.println(ImeiUtils.getImeiBy14("99001021019098"));

        String str = "86931002060103ec456a4cbb5e1829";
        String imei = ImeiUtils.getImeiBy14(str.substring(0, 14));
        System.out.println(imei + str.substring(14));

    }
}
