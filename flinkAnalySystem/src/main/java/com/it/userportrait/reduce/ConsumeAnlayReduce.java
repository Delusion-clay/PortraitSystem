package com.it.userportrait.reduce;
import com.it.userportrait.analy.ConsumeEntity;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.ReduceFunction;

public class ConsumeAnlayReduce implements ReduceFunction<ConsumeEntity> {


    @Override
    public ConsumeEntity reduce(ConsumeEntity consumeEntity, ConsumeEntity t1) throws Exception {
        String consumeFlag = consumeEntity.getConsumeFlag();
        String groupField = consumeEntity.getGroupField();
        long nubmer1 = consumeEntity.getNumbers();
        long timinfo = consumeEntity.getTimeinfo();
        long userid = consumeEntity.getUserid();
        long nubmer2 = t1.getNumbers();

        ConsumeEntity consumeEntityResult = new ConsumeEntity();
        consumeEntityResult.setConsumeFlag(consumeFlag);
        consumeEntityResult.setGroupField(groupField);
        consumeEntityResult.setNumbers(nubmer1+nubmer2);
        consumeEntityResult.setTimeinfo(timinfo);
        consumeEntityResult.setUserid(userid);
        return consumeEntityResult;
    }
}
