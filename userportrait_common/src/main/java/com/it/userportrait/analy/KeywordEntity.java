package com.it.userportrait.analy;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class KeywordEntity {
    private long userid;
    private List<String> productTitile;
    private String groupField;

    private String documentid;
    private Map<String,Long> datamap;
    private Map<String,Double> tfmap;
    private Long totaldocumet;
    private List<String> finalword;

    private String top1;
    private String top2;
    private String top3;
    private String timeinfoString;

    public String getTimeinfoString() {
        return timeinfoString;
    }

    public void setTimeinfoString(String timeinfoString) {
        this.timeinfoString = timeinfoString;
    }

    private long numbers;

    public String getTop1() {
        return top1;
    }

    public void setTop1(String top1) {
        this.top1 = top1;
    }

    public String getTop2() {
        return top2;
    }

    public void setTop2(String top2) {
        this.top2 = top2;
    }

    public String getTop3() {
        return top3;
    }

    public void setTop3(String top3) {
        this.top3 = top3;
    }

    public long getNumbers() {
        return numbers;
    }

    public void setNumbers(long numbers) {
        this.numbers = numbers;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public List<String> getProductTitile() {
        return productTitile;
    }

    public void setProductTitile(List<String> productTitile) {
        this.productTitile = productTitile;
    }

    public String getGroupField() {
        return groupField;
    }

    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }

    public String getDocumentid() {
        return documentid;
    }

    public void setDocumentid(String documentid) {
        this.documentid = documentid;
    }

    public Map<String, Long> getDatamap() {
        return datamap;
    }

    public void setDatamap(Map<String, Long> datamap) {
        this.datamap = datamap;
    }

    public Map<String, Double> getTfmap() {
        return tfmap;
    }

    public void setTfmap(Map<String, Double> tfmap) {
        this.tfmap = tfmap;
    }

    public Long getTotaldocumet() {
        return totaldocumet;
    }

    public void setTotaldocumet(Long totaldocumet) {
        this.totaldocumet = totaldocumet;
    }

    public List<String> getFinalword() {
        return finalword;
    }

    public void setFinalword(List<String> finalword) {
        this.finalword = finalword;
    }
}
