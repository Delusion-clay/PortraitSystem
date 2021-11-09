package com.it.userportrait.reduce;

import com.it.userportrait.analy.MarketingSensitivityEntity;
import org.apache.flink.api.common.functions.ReduceFunction;

public class MarketingSensitivityAnlayReduce implements ReduceFunction<MarketingSensitivityEntity> {

    public MarketingSensitivityEntity reduce(MarketingSensitivityEntity marketingSensitivityEntity, MarketingSensitivityEntity t1) throws Exception {
        long userid = marketingSensitivityEntity.getUserid();
        String timeinfoString = marketingSensitivityEntity.getTimeinfoString();
        String groupField = marketingSensitivityEntity.getGroupField();
        long advserId = marketingSensitivityEntity.getAdvisterId();
        long timeInfo = marketingSensitivityEntity.getTimeinfo();
        int advType = marketingSensitivityEntity.getAdvType();
        int advernums1 = marketingSensitivityEntity.getAdvernums();
        int ordernums1 =  marketingSensitivityEntity.getOrdernums();

        int advernums2 =  t1.getAdvernums();
        int ordernums2 =  t1.getOrdernums();

        MarketingSensitivityEntity marketingSensitivityEntityresult = new MarketingSensitivityEntity();
        marketingSensitivityEntityresult.setUserid(userid);
        marketingSensitivityEntityresult.setGroupField(groupField);
        marketingSensitivityEntity.setOrdernums(ordernums1+ordernums2);
        marketingSensitivityEntity.setAdvernums(advernums2+advernums1);
        marketingSensitivityEntity.setAdvisterId(advserId);
        marketingSensitivityEntity.setTimeinfo(timeInfo);
        marketingSensitivityEntity.setAdvType(advType);
        marketingSensitivityEntity.setTimeinfoString(timeinfoString);
        return marketingSensitivityEntityresult;
    }
}
