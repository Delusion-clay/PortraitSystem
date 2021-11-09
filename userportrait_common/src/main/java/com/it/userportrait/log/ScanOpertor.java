package com.it.userportrait.log;

/**
 * 浏览商品行为
 */
public class ScanOpertor {
    private long pingdaoId;
    private long productTypeId;
    private Long productId;
    private Long scanTime;
    private long stayTime;
    private long userid;
    private int diviceType;//(1、pc端，2微信小程序 3、app,4、快应用)
    private String deviceId;//

    public long getPingdaoId() {
        return pingdaoId;
    }

    public void setPingdaoId(long pingdaoId) {
        this.pingdaoId = pingdaoId;
    }

    public long getProductTypeId() {
        return productTypeId;
    }

    public void setProductTypeId(long productTypeId) {
        this.productTypeId = productTypeId;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Long getScanTime() {
        return scanTime;
    }

    public void setScanTime(Long scanTime) {
        this.scanTime = scanTime;
    }

    public long getStayTime() {
        return stayTime;
    }

    public void setStayTime(long stayTime) {
        this.stayTime = stayTime;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public int getDiviceType() {
        return diviceType;
    }

    public void setDiviceType(int diviceType) {
        this.diviceType = diviceType;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }
}
