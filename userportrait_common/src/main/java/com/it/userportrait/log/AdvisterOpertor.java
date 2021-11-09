package com.it.userportrait.log;

/**
 * 广告
 */
public class AdvisterOpertor {
    private long adviId;//广告id
    private long productId;//商品id
    private String clickTime;//点击时间
    private String pulishTime;//分发时间
    private String stayTime;//停留时间
    private long userid;
    private int diviceType;//(1、pc端，2微信小程序 3、app,4、快应用)
    private String deviceId;//
    private int advType;//0、动画； 1、纯文字 ； 2、视屏 ； 3、文字加动画
    private int isMingxing;//0没有明星 1有明星

    public long getAdviId() {
        return adviId;
    }

    public void setAdviId(long adviId) {
        this.adviId = adviId;
    }

    public long getProductId() {
        return productId;
    }

    public void setProductId(long productId) {
        this.productId = productId;
    }

    public String getClickTime() {
        return clickTime;
    }

    public void setClickTime(String clickTime) {
        this.clickTime = clickTime;
    }

    public String getPulishTime() {
        return pulishTime;
    }

    public void setPulishTime(String pulishTime) {
        this.pulishTime = pulishTime;
    }

    public String getStayTime() {
        return stayTime;
    }

    public void setStayTime(String stayTime) {
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

    public int getAdvType() {
        return advType;
    }

    public void setAdvType(int advType) {
        this.advType = advType;
    }

    public int getIsMingxing() {
        return isMingxing;
    }

    public void setIsMingxing(int isMingxing) {
        this.isMingxing = isMingxing;
    }
}
