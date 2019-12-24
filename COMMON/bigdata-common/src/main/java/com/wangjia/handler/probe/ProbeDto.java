package com.wangjia.handler.probe;

/**
 * Created by Administrator on 2017/9/20.
 */
public class ProbeDto {
    private int venueId;
    private String mac;
    private long time;
    private long firstTime;

    public ProbeDto(int venueId, String mac, long time, long firstTime) {
        this.venueId = venueId;
        this.mac = mac;
        this.time = time;
        this.firstTime = firstTime;
    }

    public ProbeDto(int venueId, String mac, long time) {
        this.venueId = venueId;
        this.mac = mac;
        this.time = time;
        this.firstTime = -1;
    }

    public int getVenueId() {
        return venueId;
    }

    public void setVenueId(int venueId) {
        this.venueId = venueId;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getFirstTime() {
        return firstTime;
    }

    public void setFirstTime(long firstTime) {
        this.firstTime = firstTime;
    }

    @Override
    public String toString() {
        return "ProbeDto{" +
                "venueId=" + venueId +
                ", mac='" + mac + '\'' +
                ", time=" + time +
                ", firstTime=" + firstTime +
                '}';
    }
}
