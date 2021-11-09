package com.it.userportrait.reduce;

import com.it.userportrait.analy.WoolEntity;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.ReduceFunction;

public class WoolWarningAnlayReduce implements ReduceFunction<WoolEntity> {


    @Override
    public WoolEntity reduce(WoolEntity woolEntity, WoolEntity t1) throws Exception {
        long nubmers = woolEntity.getNumbers();
        String timeinfo = woolEntity.getTimeinfo();
        long userid = woolEntity.getUserid();
        long conpusId = woolEntity.getConpusId();
        String groupField = woolEntity.getGroupField();

        long nubmers2 = t1.getNumbers();

        WoolEntity woolEntityResult = new WoolEntity();
        woolEntityResult.setUserid(userid);
        woolEntityResult.setGroupField(groupField);
        woolEntityResult.setTimeinfo(timeinfo);
        woolEntityResult.setNumbers(nubmers+nubmers2);
        woolEntityResult.setConpusId(conpusId);
        return  woolEntityResult;
    }
}
