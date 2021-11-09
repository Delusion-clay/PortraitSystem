package com.it.userportrait.entity;

import java.util.ArrayList;
import java.util.List;

public class BardataEntity {
    private List<String> xdata = new ArrayList<String>();
    private List<Long> ydata = new ArrayList<Long>();

    public List<String> getXdata() {
        return xdata;
    }

    public void setXdata(List<String> xdata) {
        this.xdata = xdata;
    }

    public List<Long> getYdata() {
        return ydata;
    }

    public void setYdata(List<Long> ydata) {
        this.ydata = ydata;
    }
}
