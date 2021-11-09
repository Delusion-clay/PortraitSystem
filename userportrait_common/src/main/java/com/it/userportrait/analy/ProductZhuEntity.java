package com.it.userportrait.analy;

public class ProductZhuEntity {
    private long userid;
    private String productZhuFlag;
    private long numbers = 0l;
    private String groupField;
    private long timeinfo;

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public String getProductZhuFlag() {
        return productZhuFlag;
    }

    public void setProductZhuFlag(String productZhuFlag) {
        this.productZhuFlag = productZhuFlag;
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

    public long getTimeinfo() {
        return timeinfo;
    }

    public void setTimeinfo(long timeinfo) {
        this.timeinfo = timeinfo;
    }
}
