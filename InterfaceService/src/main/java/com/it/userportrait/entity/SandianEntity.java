package com.it.userportrait.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SandianEntity {
    private List<String> dataList = new ArrayList<String>();
    private List<List<Object>> listxiaomi = new ArrayList<List<Object>>();
    private List<List<Object>> huaweilist = new ArrayList<List<Object>>();
    private List<List<Object>> oppolist = new ArrayList<List<Object>>();

    private List<List<Object>> list1 = new ArrayList<List<Object>>();
    private List<List<Object>> list2  = new ArrayList<List<Object>>();
    private List<List<Object>>  list3 = new ArrayList<List<Object>>();

    public List<String> getDataList() {
        return dataList;
    }

    public void setDataList(List<String> dataList) {
        this.dataList = dataList;
    }

    public List<List<Object>> getListxiaomi() {
        return listxiaomi;
    }

    public void setListxiaomi(List<List<Object>> listxiaomi) {
        this.listxiaomi = listxiaomi;
    }

    public List<List<Object>> getHuaweilist() {
        return huaweilist;
    }

    public void setHuaweilist(List<List<Object>> huaweilist) {
        this.huaweilist = huaweilist;
    }

    public List<List<Object>> getOppolist() {
        return oppolist;
    }

    public void setOppolist(List<List<Object>> oppolist) {
        this.oppolist = oppolist;
    }

    public List<List<Object>> getList1() {
        return list1;
    }

    public void setList1(List<List<Object>> list1) {
        this.list1 = list1;
    }

    public List<List<Object>> getList2() {
        return list2;
    }

    public void setList2(List<List<Object>> list2) {
        this.list2 = list2;
    }

    public List<List<Object>> getList3() {
        return list3;
    }

    public void setList3(List<List<Object>> list3) {
        this.list3 = list3;
    }
}
