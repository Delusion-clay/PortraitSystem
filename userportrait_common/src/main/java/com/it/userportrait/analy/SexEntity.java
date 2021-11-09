package com.it.userportrait.analy;

public class SexEntity {
    private long userid;//用户id
    private long ordernums;//订单次数
    private long orderintenums;//订单频次
    private long manClothes;//浏览男装
    private long chidrenClothes;//浏览小孩
    private long oldClothes;//浏览老人
    private long womenClothes;//浏览女士
    private double ordermountavg;//订单平均金额
    private long productscannums;//浏览商品频次
    private int label;//0 女 1 男
    private String groupField;//
    private String sex;
    private long numbers;

    public long getNumbers() {
        return numbers;
    }

    public void setNumbers(long numbers) {
        this.numbers = numbers;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public long getOrdernums() {
        return ordernums;
    }

    public void setOrdernums(long ordernums) {
        this.ordernums = ordernums;
    }

    public long getOrderintenums() {
        return orderintenums;
    }

    public void setOrderintenums(long orderintenums) {
        this.orderintenums = orderintenums;
    }

    public long getManClothes() {
        return manClothes;
    }

    public void setManClothes(long manClothes) {
        this.manClothes = manClothes;
    }

    public long getChidrenClothes() {
        return chidrenClothes;
    }

    public void setChidrenClothes(long chidrenClothes) {
        this.chidrenClothes = chidrenClothes;
    }

    public long getOldClothes() {
        return oldClothes;
    }

    public void setOldClothes(long oldClothes) {
        this.oldClothes = oldClothes;
    }

    public long getWomenClothes() {
        return womenClothes;
    }

    public void setWomenClothes(long womenClothes) {
        this.womenClothes = womenClothes;
    }

    public double getOrdermountavg() {
        return ordermountavg;
    }

    public void setOrdermountavg(double ordermountavg) {
        this.ordermountavg = ordermountavg;
    }

    public long getProductscannums() {
        return productscannums;
    }

    public void setProductscannums(long productscannums) {
        this.productscannums = productscannums;
    }

    public int getLabel() {
        return label;
    }

    public void setLabel(int label) {
        this.label = label;
    }

    public String getGroupField() {
        return groupField;
    }

    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }
}
