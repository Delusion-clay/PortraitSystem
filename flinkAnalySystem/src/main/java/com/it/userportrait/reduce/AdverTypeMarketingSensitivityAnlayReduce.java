package com.it.userportrait.reduce;

import com.it.userportrait.analy.MarketingSensitivityEntity;
import org.apache.flink.api.common.functions.ReduceFunction;

public class AdverTypeMarketingSensitivityAnlayReduce implements ReduceFunction<MarketingSensitivityEntity> {

    public MarketingSensitivityEntity reduce(MarketingSensitivityEntity marketingSensitivityEntity, MarketingSensitivityEntity t1) throws Exception {
        String avTypeName = marketingSensitivityEntity.getAvTypeName();
        long timeinfo = marketingSensitivityEntity.getTimeinfo();
        String sensitivityFlag = marketingSensitivityEntity.getSensitivityFlag();
        String groupField = marketingSensitivityEntity.getGroupField();

        long numbers1 = marketingSensitivityEntity.getNumbers();

        long numbers2 = t1.getNumbers();


        MarketingSensitivityEntity marketingSensitivityEntityresult = new MarketingSensitivityEntity();
        marketingSensitivityEntityresult.setAvTypeName(avTypeName);
        marketingSensitivityEntityresult.setTimeinfo(timeinfo);
        marketingSensitivityEntityresult.setGroupField(groupField);
        marketingSensitivityEntityresult.setSensitivityFlag(sensitivityFlag);
        marketingSensitivityEntityresult.setNumbers(numbers1+numbers2);
        return marketingSensitivityEntityresult;
    }
}
