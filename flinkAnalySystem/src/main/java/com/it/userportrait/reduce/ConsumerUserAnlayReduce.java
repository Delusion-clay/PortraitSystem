package com.it.userportrait.reduce;

import com.it.userportrait.analy.ConsumeEntity;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.ReduceFunction;

public class ConsumerUserAnlayReduce implements ReduceFunction<ConsumeEntity> {


    @Override
    public ConsumeEntity reduce(ConsumeEntity consumeEntity, ConsumeEntity t1) throws Exception {
        String consumerFlag = consumeEntity.getConsumeFlag();
        long timeinfo = consumeEntity.getTimeinfo();
        String groupField = consumeEntity.getGroupField();
        long number1 = consumeEntity.getNumbers();
        long number2 = t1.getNumbers();

        ConsumeEntity consumeEntity1 = new ConsumeEntity();
        consumeEntity1.setGroupField(groupField);
        consumeEntity1.setTimeinfo(timeinfo);
        consumeEntity1.setConsumeFlag(consumerFlag);
        consumeEntity1.setNumbers(number1+number2);
        return consumeEntity1;
    }
}
