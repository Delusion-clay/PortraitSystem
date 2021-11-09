package com.it.userportrait.analy;

public class BrandEntity {
    private long userid;
    private String brand;
    private long numbers = 0l;
    private String groupField;
    private long productId;
    private String timeinfo;
    private String timeinfoString;

    public String getTimeinfoString() {
        return timeinfoString;
    }

    public void setTimeinfoString(String timeinfoString) {
        this.timeinfoString = timeinfoString;
    }

    public String getTimeinfo() {
        return timeinfo;
    }

    public void setTimeinfo(String timeinfo) {
        this.timeinfo = timeinfo;
    }

    public long getProductId() {
        return productId;
    }

    public void setProductId(long productId) {
        this.productId = productId;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public long getNumbers() {
        return numbers;
    }

    public void setNumbers(long numbers) {
        this.numbers = numbers;
    }

    public String getGroupField() {
        return groupField;
    }

    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }
}
