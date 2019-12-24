package com.wangjia.handler.item;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2018/1/2.
 */
public final class MatchItemHandler implements Serializable {
    private static final long serialVersionUID = 622521560334844627L;

    /**
     * 不同的APPID的适配器
     */
    private Map<String, List<ItemPattern>> mapPatterns = new HashMap<>();

    public void addItemMatcher(ItemPattern itemPattern) {
        List<ItemPattern> list = null;
        if (mapPatterns.containsKey(itemPattern.appid)) {
            list = mapPatterns.get(itemPattern.appid);
        } else {
            list = new LinkedList<ItemPattern>();
            mapPatterns.put(itemPattern.appid, list);
        }
        int size = list.size();
        int index = 0;
        while (true) {
            if (index == size)
                break;
            if (itemPattern.weight > list.get(index).weight)
                break;
            index++;
        }
        list.add(index, itemPattern);
    }

    public void addItemMatcher(List<ItemPattern> patterns) {
        for (int i = 0; i < patterns.size(); i++) {
            addItemMatcher(patterns.get(i));
        }
    }

    /**
     * 抓取物品信息
     *
     * @param appid
     * @param page
     * @param data
     * @return
     */
    public Item getItem(String appid, String page, String data) {
        if (!mapPatterns.containsKey(appid)) {
            return null;
        }
        List<ItemPattern> list = mapPatterns.get(appid);
        for (ItemPattern pattern : list) {
            Item item = pattern.getItem(appid, page, data);
            if (item != null) {
                return item;
            }
        }
        return null;
    }

    /**
     * 校验正则表达式
     *
     * @param url   URL
     * @param regex 正则表达式
     * @return
     */
    public static String verifyRegex(String url, String regex) {
        String error = "";
        try {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(url);
            if (matcher.find() && matcher.groupCount() >= 1) {
                String itemid = matcher.group(1);
                return "SUCCESS:" + itemid;
            }
            throw new Exception("No match found");
        } catch (Exception e) {
            error = e.getMessage();
        }
        return "ERROR:" + error;
    }
}
