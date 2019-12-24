package com.wangjia.es;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017/9/7.
 */
public class EsPageBean<T> implements Serializable{
    //    总量
    protected long total;
    //一页的ｓｉｚｅ
    protected int size;
    //一页的数量
    protected int num;
    protected  long totalPage;
    //返回的数据
    protected List<T> datas;

    protected Map<String, T> map;

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }


    public long getTotalPage() {
        return totalPage;
    }

    public void setTotalPage(long totalPage) {
        this.totalPage = totalPage;
    }

    public List<T> getDatas() {
        return datas;
    }


    public void setDatas(List<T> datas) {
        this.datas = datas;
    }

    public Map<String, T> getMap() {
        return map;
    }

    public void setMap(Map<String, T> map) {
        this.map = map;
    }

    @Override
    public String toString() {
        return "EsPageBean{" +
                "total=" + total +
                ", size=" + size +
                ", num=" + num +
                ", totalPage=" + totalPage +
                ", datas=" + datas +
                ", map=" + map +
                '}';
    }
}
