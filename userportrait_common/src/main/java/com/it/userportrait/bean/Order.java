package com.it.userportrait.bean;

import java.util.Date;

public class Order {

    private long id;
    private double amount;
    private long userid;
    private long productid;
    private long merchartid;
    private Date createTime;
    private Date payTime;
    private int paystatus;
    private String adress;
    private String telphone;
    private String username;
    private String tradenumber;
    private int paytype;
    private long number;
    private int orderstatus;
    private Date updateTime;
    private Long advisterId;//广告id
    private long couponId;//如果没有用优惠券的话 默认0
    private Double couponAmount;//优惠金额

    public long getCouponId() {
        return couponId;
    }

    public void setCouponId(long couponId) {
        this.couponId = couponId;
    }

    public Double getCouponAmount() {
        return couponAmount;
    }

    public void setCouponAmount(Double couponAmount) {
        this.couponAmount = couponAmount;
    }

    public Long getAdvisterId() {
        return advisterId;
    }

    public void setAdvisterId(Long advisterId) {
        this.advisterId = advisterId;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public long getProductid() {
        return productid;
    }

    public void setProductid(long productid) {
        this.productid = productid;
    }

    public long getMerchartid() {
        return merchartid;
    }

    public void setMerchartid(long merchartid) {
        this.merchartid = merchartid;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getPayTime() {
        return payTime;
    }

    public void setPayTime(Date payTime) {
        this.payTime = payTime;
    }

    public int getPaystatus() {
        return paystatus;
    }

    public void setPaystatus(int paystatus) {
        this.paystatus = paystatus;
    }

    public String getAdress() {
        return adress;
    }

    public void setAdress(String adress) {
        this.adress = adress;
    }

    public String getTelphone() {
        return telphone;
    }

    public void setTelphone(String telphone) {
        this.telphone = telphone;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getTradenumber() {
        return tradenumber;
    }

    public void setTradenumber(String tradenumber) {
        this.tradenumber = tradenumber;
    }

    public int getPaytype() {
        return paytype;
    }

    public void setPaytype(int paytype) {
        this.paytype = paytype;
    }

    public long getNumber() {
        return number;
    }

    public void setNumber(long number) {
        this.number = number;
    }

    public int getOrderstatus() {
        return orderstatus;
    }

    public void setOrderstatus(int orderstatus) {
        this.orderstatus = orderstatus;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}
