package com.sohu.rdc.infcdn.offline.mr.entity;

/**
 * Created by zengxiaosen on 2017/5/24.
 */
public class IPRegion {
    private Long minIP;

    public Long getMinIP() {
        return minIP;
    }

    public void setMinIP(Long minIP) {
        this.minIP = minIP;
    }

    public Long getMaxIP() {
        return maxIP;
    }

    public void setMaxIP(Long maxIP) {
        this.maxIP = maxIP;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    private Long maxIP;
    private String region;
}
