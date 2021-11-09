package com.it.userportrait.analy;

public class WoolEntity {
    private long userid;
    private long numbers = 0l;
    private String groupField;
    private String timeinfo;
    private long conpusId;

    public long getConpusId() {
        return conpusId;
    }

    public void setConpusId(long conpusId) {
        this.conpusId = conpusId;
    }

    public String getTimeinfo() {
        return timeinfo;
    }

    public void setTimeinfo(String timeinfo) {
        this.timeinfo = timeinfo;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
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
