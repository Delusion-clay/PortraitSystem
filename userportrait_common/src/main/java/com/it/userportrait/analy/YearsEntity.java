package com.it.userportrait.analy;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-07-20 11:12
 */
public class YearsEntity {
    private int userid;
    private String yearsFlag;
    private long numbers = 0l;
    private String groupField;//yearslabel==time(yyyyMMddHHmm)==yearlabel

    public int getUserid() {
        return userid;
    }

    public void setUserid(int userid) {
        this.userid = userid;
    }

    public String getYearsFlag() {
        return yearsFlag;
    }

    public void setYearsFlag(String yearsFlag) {
        this.yearsFlag = yearsFlag;
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

