package com.it.userportrait.bean;

import java.util.Date;

public class Product {
    private long id;
    private long productTypeid;
    private String productname;
    private String productTitile;
    private double productprice;
    private long merchartId;
    private Date createTime;
    private Date updateTime;
    private String productplace;
    private String productbrand;
    private String productweight;
    private String productspecification;


    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getProductTypeid() {
        return productTypeid;
    }

    public void setProductTypeid(long productTypeid) {
        this.productTypeid = productTypeid;
    }

    public String getProductname() {
        return productname;
    }

    public void setProductname(String productname) {
        this.productname = productname;
    }

    public String getProductTitile() {
        return productTitile;
    }

    public void setProductTitile(String productTitile) {
        this.productTitile = productTitile;
    }

    public double getProductprice() {
        return productprice;
    }

    public void setProductprice(double productprice) {
        this.productprice = productprice;
    }

    public long getMerchartId() {
        return merchartId;
    }

    public void setMerchartId(long merchartId) {
        this.merchartId = merchartId;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getProductplace() {
        return productplace;
    }

    public void setProductplace(String productplace) {
        this.productplace = productplace;
    }

    public String getProductbrand() {
        return productbrand;
    }

    public void setProductbrand(String productbrand) {
        this.productbrand = productbrand;
    }

    public String getProductweight() {
        return productweight;
    }

    public void setProductweight(String productweight) {
        this.productweight = productweight;
    }

    public String getProductspecification() {
        return productspecification;
    }

    public void setProductspecification(String productspecification) {
        this.productspecification = productspecification;
    }
}
