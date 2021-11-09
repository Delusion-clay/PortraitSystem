package com.it.userportrait.reduce;

import com.it.userportrait.analy.YearsEntity;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-07-20 11:20
 */
public class YearsAnlayReduce implements ReduceFunction<YearsEntity> {

    public YearsEntity reduce(YearsEntity yearsEntity, YearsEntity t1) throws Exception {
        long numbers1 = 0l;
        String groupField = "";
        if(yearsEntity != null){
            numbers1 = yearsEntity.getNumbers();
            groupField = yearsEntity.getGroupField();
        }
        long numbers2 = 0l;
        if(t1 != null){
            numbers2 = t1.getNumbers();
            groupField = t1.getGroupField();
        }

        if(StringUtils.isNotBlank(groupField)){
            YearsEntity yearsEntity1 = new YearsEntity();
            yearsEntity1.setGroupField(groupField);
            yearsEntity1.setNumbers(numbers1+numbers2);
            return  yearsEntity1;
        }
        return null;
    }
}

