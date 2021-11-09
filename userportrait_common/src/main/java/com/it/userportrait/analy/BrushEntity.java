package com.it.userportrait.analy;

import java.util.Set;

public class BrushEntity {
    private long userid;
    private String address;
    private long numbers = 0l;
    private String groupField;
    private String timeinfo;
    private Set<Long> userids;
    private String illusernums;//6-10 10 11-15 15 15-20 16 20- 20以上
    private String timeinfoString;

    public String getTimeinfoString() {
        return timeinfoString;
    }

    public void setTimeinfoString(String timeinfoString) {
        this.timeinfoString = timeinfoString;
    }

    public String getIllusernums() {
        return illusernums;
    }

    public void setIllusernums(String illusernums) {
        this.illusernums = illusernums;
    }

    @Override
    public String toString() {
        return "BrushEntity{}";
    }

    public Set<Long> getUserids() {
        return userids;
    }

    public void setUserids(Set<Long> userids) {
        this.userids = userids;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
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

    public String getTimeinfo() {
        return timeinfo;
    }

    public void setTimeinfo(String timeinfo) {
        this.timeinfo = timeinfo;
    }
}
