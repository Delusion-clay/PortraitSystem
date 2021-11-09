package com.it.userportrait.reduce;

import com.it.userportrait.analy.SexEntity;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.ReduceFunction;

public class SexSaveAnlayReduce implements ReduceFunction<SexEntity> {

    public SexEntity reduce(SexEntity youfanSexEntity, SexEntity t1) throws Exception {
        long numbers1 = 0l;
        String groupField = "";
        if(youfanSexEntity != null){
            numbers1 =youfanSexEntity.getNumbers();
            groupField = youfanSexEntity.getGroupField();
        }
        long numbers2 = 0l;
        if(t1 != null){
            numbers2 = t1.getNumbers();
            groupField = t1.getGroupField();
        }

        if(StringUtils.isNotBlank(groupField)){
            SexEntity sexEntity1 = new SexEntity();
            sexEntity1.setGroupField(groupField);
            sexEntity1.setNumbers(numbers1+numbers2);
            return  sexEntity1;
        }
        return null;
        //ssss
    }
}
