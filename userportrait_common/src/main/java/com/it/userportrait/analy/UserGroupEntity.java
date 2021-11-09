package com.it.userportrait.analy;


import java.util.Date;
import java.util.List;

public class UserGroupEntity {
    private long userid;
    private Date ordertime;
    private String productTypeId;
    private double amount;
    private String groupField;
    private List<UserGroupEntity> list;

    private double avgAmount;//
    private double maxAmount;
    private int avgdays;//
    private long dianZiNums;
    private long shenghuoNums;
    private long huwaiNums;
    private long time1;//7-12 1
    private long time2;//13-19 2
    private long time3;//20-24 3
    private long time4;//0-6 4

    private Point centerPoint;
    private long numbers;
    private long timeinfo;

    public long getTimeinfo() {
        return timeinfo;
    }

    public void setTimeinfo(long timeinfo) {
        this.timeinfo = timeinfo;
    }

    public Point getCenterPoint() {
        return centerPoint;
    }

    public void setCenterPoint(Point centerPoint) {
        this.centerPoint = centerPoint;
    }

    public long getNumbers() {
        return numbers;
    }

    public void setNumbers(long numbers) {
        this.numbers = numbers;
    }

    public long getTime1() {
        return time1;
    }

    public void setTime1(long time1) {
        this.time1 = time1;
    }

    public long getTime2() {
        return time2;
    }

    public void setTime2(long time2) {
        this.time2 = time2;
    }

    public long getTime3() {
        return time3;
    }

    public void setTime3(long time3) {
        this.time3 = time3;
    }

    public long getTime4() {
        return time4;
    }

    public void setTime4(long time4) {
        this.time4 = time4;
    }

    public long getDianZiNums() {
        return dianZiNums;
    }

    public void setDianZiNums(long dianZiNums) {
        this.dianZiNums = dianZiNums;
    }

    public long getShenghuoNums() {
        return shenghuoNums;
    }

    public void setShenghuoNums(long shenghuoNums) {
        this.shenghuoNums = shenghuoNums;
    }

    public long getHuwaiNums() {
        return huwaiNums;
    }

    public void setHuwaiNums(long huwaiNums) {
        this.huwaiNums = huwaiNums;
    }

    public double getAvgAmount() {
        return avgAmount;
    }

    public void setAvgAmount(double avgAmount) {
        this.avgAmount = avgAmount;
    }

    public double getMaxAmount() {
        return maxAmount;
    }

    public void setMaxAmount(double maxAmount) {
        this.maxAmount = maxAmount;
    }

    public int getAvgdays() {
        return avgdays;
    }

    public void setAvgdays(int avgdays) {
        this.avgdays = avgdays;
    }

    public List<UserGroupEntity> getList() {
        return list;
    }

    public void setList(List<UserGroupEntity> list) {
        this.list = list;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public Date getOrdertime() {
        return ordertime;
    }

    public void setOrdertime(Date ordertime) {
        this.ordertime = ordertime;
    }

    public String getProductTypeId() {
        return productTypeId;
    }

    public void setProductTypeId(String productTypeId) {
        this.productTypeId = productTypeId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getGroupField() {
        return groupField;
    }

    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }
}
