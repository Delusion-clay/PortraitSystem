package com.it.userportrait.reduce;

import com.it.userportrait.analy.WoolEntity;
import org.apache.flink.api.common.functions.ReduceFunction;

public class WoolGaiKuangAnlayReduce implements ReduceFunction<WoolEntity> {


    @Override
    public WoolEntity reduce(WoolEntity woolEntity, WoolEntity t1) throws Exception {
        String groupField = woolEntity.getGroupField();
        String timeInfo = woolEntity.getTimeinfo();
        long numbers = woolEntity.getNumbers();

        long nubmers2 = t1.getNumbers();

        WoolEntity woolEntityResult  = new WoolEntity();
        woolEntityResult.setTimeinfo(timeInfo);
        woolEntityResult.setGroupField(groupField);
        woolEntityResult.setNumbers(numbers+nubmers2);
        return woolEntityResult;
    }
}
