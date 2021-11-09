package com.it.userportrait.log;

/**
 * 购物车行为
 */
public class CartOpertor {
    private long pingdaoId;
    private long productTypeId;
    private Long productId;
    private Long opetorTime;
    private int opertorType;//（0加入，1取消）
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

    public Long getOpetorTime() {
        return opetorTime;
    }

    public void setOpetorTime(Long opetorTime) {
        this.opetorTime = opetorTime;
    }

    public int getOpertorType() {
        return opertorType;
    }

    public void setOpertorType(int opertorType) {
        this.opertorType = opertorType;
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
