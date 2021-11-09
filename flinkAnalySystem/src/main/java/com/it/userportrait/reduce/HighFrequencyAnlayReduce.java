package com.it.userportrait.reduce;

import com.it.userportrait.analy.BrushEntity;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.ReduceFunction;

public class HighFrequencyAnlayReduce implements ReduceFunction<BrushEntity> {


    @Override
    public BrushEntity reduce(BrushEntity brushEntity, BrushEntity t1) throws Exception {
        long numbers1 = brushEntity.getNumbers();
        long nubmers2 = t1.getNumbers();
        String timeinfo = brushEntity.getTimeinfo();
        long userid = brushEntity.getUserid();
        String groupFiled = brushEntity.getGroupField();

        BrushEntity brushEntityResult = new BrushEntity();
        brushEntityResult.setTimeinfo(timeinfo);
        brushEntityResult.setUserid(userid);
        brushEntityResult.setGroupField(groupFiled);
        brushEntityResult.setNumbers(numbers1+nubmers2);
        return brushEntityResult;
    }
}
