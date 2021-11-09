package com.it.userportrait.analy;

public class MarketingSensitivityEntity {
    private long userid;
    private int ordernums;
    private int advernums;
    private String groupField;
    private long advisterId;
    private long timeinfo;
    private int advType;
    private String avTypeName;
    private String sensitivityFlag;
    private long numbers;
    private String timeinfoString;

    public String getTimeinfoString() {
        return timeinfoString;
    }

    public void setTimeinfoString(String timeinfoString) {
        this.timeinfoString = timeinfoString;
    }

    public long getNumbers() {
        return numbers;
    }

    public void setNumbers(long numbers) {
        this.numbers = numbers;
    }

    public String getAvTypeName() {
        return avTypeName;
    }

    public void setAvTypeName(String avTypeName) {
        this.avTypeName = avTypeName;
    }

    public String getSensitivityFlag() {
        return sensitivityFlag;
    }

    public void setSensitivityFlag(String sensitivityFlag) {
        this.sensitivityFlag = sensitivityFlag;
    }

    public int getAdvType() {
        return advType;
    }

    public void setAdvType(int advType) {
        this.advType = advType;
    }

    public long getTimeinfo() {
        return timeinfo;
    }

    public void setTimeinfo(long timeinfo) {
        this.timeinfo = timeinfo;
    }

    public long getAdvisterId() {
        return advisterId;
    }

    public void setAdvisterId(long advisterId) {
        this.advisterId = advisterId;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public int getOrdernums() {
        return ordernums;
    }

    public void setOrdernums(int ordernums) {
        this.ordernums = ordernums;
    }

    public int getAdvernums() {
        return advernums;
    }

    public void setAdvernums(int advernums) {
        this.advernums = advernums;
    }

    public String getGroupField() {
        return groupField;
    }

    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }
}
