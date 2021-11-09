package com.it.userportrait.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CurveEntity {
    private List<String> xList;
    private List<Map<String,Object>> dataMapList;
    private List<String> legenddataList;

    public List<String> getLegenddataList() {
        return legenddataList;
    }

    public void setLegenddataList(List<String> legenddataList) {
        this.legenddataList = legenddataList;
    }

    public List<String> getxList() {
        return xList;
    }

    public void setxList(List<String> xList) {
        this.xList = xList;
    }

    public List<Map<String, Object>> getDataMapList() {
        return dataMapList;
    }

    public void setDataMapList(List<Map<String, Object>> dataMapList) {
        this.dataMapList = dataMapList;
    }
}
