package com.it.userportrait.analy;

public class ConsumeEntity {
    private long userid;
    private String consumeFlag;
    private long numbers = 0l;
    private String groupField;
    private long timeinfo;
    private String timeinfoString;

    public String getTimeinfoString() {
        return timeinfoString;
    }

    public void setTimeinfoString(String timeinfoString) {
        this.timeinfoString = timeinfoString;
    }

    public long getTimeinfo() {
        return timeinfo;
    }

    public void setTimeinfo(long timeinfo) {
        this.timeinfo = timeinfo;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public String getConsumeFlag() {
        return consumeFlag;
    }

    public void setConsumeFlag(String consumeFlag) {
        this.consumeFlag = consumeFlag;
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
