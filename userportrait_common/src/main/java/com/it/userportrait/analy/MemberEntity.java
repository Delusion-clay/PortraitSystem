package com.it.userportrait.analy;

public class MemberEntity {
    private int userid;
    private String memberFlag;
    private long numbers = 0l;
    private String groupField;//memberFlag==time(yyyyMMddHHmm)==memberFlag

    public int getUserid() {
        return userid;
    }

    public void setUserid(int userid) {
        this.userid = userid;
    }

    public String getMemberFlag() {
        return memberFlag;
    }

    public void setMemberFlag(String memberFlag) {
        this.memberFlag = memberFlag;
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
