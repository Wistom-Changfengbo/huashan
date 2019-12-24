package com.wangjia.eqid;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wangjia.hbase.HBaseConst;
import com.wangjia.hbase.HBaseTableName;
import com.wangjia.utils.HBaseUtils;
import com.wangjia.utils.HttpClientUtil;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;

/**
 * 查询百度EQID
 */
public class EqidConnection {
    private static final DateTimeFormatter alternateIso8601DateFormat = ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.UTC);
    private static final String SIGNATURE_HEADERS = "host";
    private static final String VERSION = "1";
    private static final String EXPIRATION_SECONDS = "1800";
    private static final String DEFAULT_ENCODING = "UTF-8";
    private static final Charset UTF8 = Charset.forName(DEFAULT_ENCODING);
    private static final String TRANSFER_URL = "http://180.76.110.24:8089/v1/eqid/";
    private static final String BASE_URL = "http://referer.bj.baidubce.com/v1/eqid/";
    private static final int EQID_SIZE = "9df115b700015c51000000065a9cc43f".length();

    private Connection hBConn;
    private Table hbTable;

    public EqidConnection() {
        hBConn = HBaseUtils.getConn();
        hbTable = HBaseUtils.getTable(hBConn, HBaseTableName.EQID_KEYWORD);
    }

    public void close() {
        if (hbTable != null)
            HBaseUtils.closeTable(hbTable);
        if (hBConn != null)
            HBaseUtils.closeConn(hBConn);
    }

    public String getKeyWord(String eqid, String accessKey, String secretKey) {
        try {
            if (eqid == null || eqid.length() != EQID_SIZE) {
                return null;
            }
            StringBuilder stringBuilder = new StringBuilder(eqid);
            byte[] rowkey = Bytes.toBytes(stringBuilder.reverse().toString());
            Result result = hbTable.get(new Get(rowkey));
            if (result.containsColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG))
                return Bytes.toString(result.getValue(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG));

            //查询百度
            String utc_time_str = alternateIso8601DateFormat.print(new DateTime(new Date()));
            String auString = getAuthString(utc_time_str, accessKey);
            String signing_key = sha256Hex(secretKey, auString);
            String canonical_request = getRequest(BASE_URL.concat(eqid));
            String signature = sha256Hex(signing_key, canonical_request);
            String authorization = String.format("bce-auth-v%s/%s/%s/%s/%s/%s", VERSION, accessKey, utc_time_str, EXPIRATION_SECONDS, SIGNATURE_HEADERS, signature);
            HashMap header = getHeader(authorization, utc_time_str);
            String response = HttpClientUtil.get(TRANSFER_URL.concat(eqid), header);

            JSONObject jsonObject = JSON.parseObject(response);

            if (jsonObject.containsKey("wd")) {
                Put put = new Put(rowkey);
                String wd = URLDecoder.decode(jsonObject.getString("wd"), "utf-8");
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG, Bytes.toBytes(wd));
                hbTable.put(put);
                return wd;
            }
            return null;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //拼接Header参数
    private HashMap getHeader(String auth, String utc_time_str) {
        HashMap headerParams = new HashMap<String, String>();
        headerParams.put("accept-encoding", "gzip, deflate");

        headerParams.put("host", "referer.bj.baidubce.com");
        headerParams.put("x-bce-date", utc_time_str);
        headerParams.put("Authorization", auth);
        headerParams.put("accept", "*/*");
        return headerParams;
    }

    //获取Header的加密request参数
    private String getRequest(String ustr) throws MalformedURLException {
        URL url = new URL(ustr);
        String host = url.getHost();
        String canonical_uri = url.getPath();
        String canonical_headers = String.format("host:%s", host.trim());
        String canonical_request = String.format("%s\n%s\n\n%s", "GET", canonical_uri, canonical_headers);
        return canonical_request;
    }

    //获取Header的Authorization参数
    private String getAuthString(String utc_time_str, String accessKey) {
        String authString = String.format("bce-auth-v%s/%s/%s/%s", VERSION, accessKey, utc_time_str, EXPIRATION_SECONDS);
        return authString;
    }

    private String sha256Hex(String signingKey, String stringToSign) throws NoSuchAlgorithmException, InvalidKeyException {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(signingKey.getBytes(UTF8), "HmacSHA256"));
        return new String(Hex.encodeHex(mac.doFinal(stringToSign.getBytes(UTF8))));
    }
}
