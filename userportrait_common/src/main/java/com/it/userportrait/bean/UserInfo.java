package com.it.userportrait.bean;

import java.util.Date;

public class UserInfo {
    private int id;
    private int account;
    private String  password;
    private int sex;
    private int age;
    private String phone;
    private int status;
    private String weixinaccount;
    private String zhifubaoaccount;
    private String email;
    private Date createTime;
    private Date updateTime;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getAccount() {
        return account;
    }

    public void setAccount(int account) {
        this.account = account;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getSex() {
        return sex;
    }

    public void setSex(int sex) {
        this.sex = sex;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getWeixinaccount() {
        return weixinaccount;
    }

    public void setWeixinaccount(String weixinaccount) {
        this.weixinaccount = weixinaccount;
    }

    public String getZhifubaoaccount() {
        return zhifubaoaccount;
    }

    public void setZhifubaoaccount(String zhifubaoaccount) {
        this.zhifubaoaccount = zhifubaoaccount;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public String toString() {
        return "UserInfo{" +
                "id=" + id +
                ", account=" + account +
                ", password='" + password + '\'' +
                ", sex=" + sex +
                ", age=" + age +
                ", phone='" + phone + '\'' +
                ", status=" + status +
                ", weixinaccount='" + weixinaccount + '\'' +
                ", zhifubaoaccount='" + zhifubaoaccount + '\'' +
                ", email='" + email + '\'' +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}

